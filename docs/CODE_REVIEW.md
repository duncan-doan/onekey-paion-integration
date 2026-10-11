**Code review — 11 October 2026**

Reviewed the OneKey and Dataverse clients, mapping/transformation, full and
delta orchestration, retry tracing, queue workers, configuration, and tests.
Concrete fixes are applied in this workspace. Scheduled delta synchronization
still has a significant completeness issue described first below.

This review used offline tests, the local IQVIA reference PDFs, and official
Microsoft documentation. It did not call either live service, verify the
connector's purchased entitlements, inspect deployed Dataverse keys, or run a
production performance benchmark. A second review the same day corrected
several of the first round's fixes; see **Revisions after the second review**.

**Remaining findings, in priority order**

| Priority | Finding and evidence | Impact and recommended next change |
| --- | --- | --- |
| High | Delta contact composition requires related ACTIVITY, INDIVIDUAL, and WORKPLACE records in the same change window. See [`_sync_delta_codebase`](../integration/dynamic_sync.py), especially the `if activities_by_individual` composition block. An offline INDIVIDUAL-only delta returns success with zero contact writes and no failure. The vendor contract makes this the normal case: an ACTIVITY result "is always served with its workplace and its individual", and creating an individual with an activity delivers only the activity and its workplace, with no separate INDIVIDUAL result (Developer Guide 6.13.9). | Name/classification updates, activity transfers, new HCPs and other independent changes can disappear after the watermark advances. First step: when no separate INDIVIDUAL or WORKPLACE result is in the window, compose the contact from the `individual` and `workplace` embedded in the ACTIVITY result. Beyond that, hydrate changed entities from a persisted contracted snapshot rather than using presence in a delta as proof of membership in the contracted population. |
| Medium | The delta window starts 15 minutes before the previous window's end ([`WINDOW_OVERLAP`](../integration/scheduled_sync.py)), and the deltas run once a day at 00:00 UTC. OneKey dates an entity before the web service can serve it, after "a short but not constant time" (Developer Guide 6.13.7), and the guide recommends a refresh early in the morning and another at noon for population updates the nightly OneKey Connect job completes late (6.13.8). | A record dated more than 15 minutes before a nightly read but served only after it is read by no later delta, and waits for the monthly full sync. For example, dated 23:20 and served at 00:05: the 00:00 read is too early, and the next window starts at 23:45. Widen the overlap to several hours (re-reads are idempotent and skip-unchanged avoids the writes) and consider a second daily delta at noon UTC. |
| Medium | [`_POLYMORPHIC_NAVIGATION`](../dataverse/client.py) maps the customer lookups but not `ts_fromid` (bound as `ts_FromID_account` / `ts_FromID_contact`). | With skip-unchanged on (the default), every `ts_affiliation` bulk upsert asks for `_ts_fromid_account_value` / `_ts_fromid_contact_value`, columns that do not exist. Dataverse answers 400, the client logs a warning and repeats the lookup without comparison, so affiliations, the largest table, are never skipped and each batch costs an extra failed request. Add `ts_fromid_account` and `ts_fromid_contact` → `ts_fromid`. |
| High | [`active_job`](../integration/scheduled_sync.py) checks before creating a job; it does not acquire a lease. [`upsert_by_field`](../dataverse/client.py) and bulk upserts perform lookup followed by create. CLI runs can bypass the busy check. | Simultaneous workers can both start and create the same logical row. Use an atomic per-codebase lease and enforced alternate keys/native upserts. An immediate negative lookup after an ambiguous create also cannot prove that the original request will never commit. Existing schema/key claims in older documentation require deployment verification. |
| Medium | A population walk keeps its transaction only in memory. See [`iter_population`](../onekey/client.py). | Within a walk, a packet lost in transit is re-fetched by number (up to three) and a packet answered `success:false` is requested again, so these no longer fail the run. A run that dies mid-walk still starts again from the first packet, which for a full sync means re-reading the whole population. Persisting the transaction and last delivered packet would let a retried delivery resume. |
| Medium | `AFF-007` onward in [`entity_mapping.csv`](../config/entity_mapping.csv) targets `ts_affiliationrole`; [`transform_activity_affiliation`](../integration/transform.py) and orchestration only write `ts_affiliation`. | Activity role rows are never persisted. An offline activity with TIH roles produces only account, contact, and affiliation writes. Add role creation/update and reconciliation using the configured lookup tables. |
| Medium | [`_sync_addresses_impl`](../integration/dynamic_sync.py) receives one primary address per flattened HCO. Additional slots defined in [`entity_mapping.csv`](../config/entity_mapping.csv) have already been discarded by flattening. | Secondary addresses are lost. An offline two-address workplace writes only its first address. Iterate original address entries and construct each composite key before flattening. |
| Medium | [`CatalogResolver.ensure`](../integration/lookup_sync.py) uses raw codes as stub labels, then upserts on a cache miss. | A failed preload or a concurrently added row can have its proper label overwritten by `HOS`/`01`. Find existing GUIDs without updating labels; use insert-only stub creation backed by a unique key. |
| Medium | Empty source values and disappearing hierarchy/relationship entries commonly omit writes. | Existing Dataverse values and links can remain after the corresponding source data changes or disappears. Define authoritative fields and explicit clear/deactivate behavior. |
| Medium | Population walks, flattened entities, indexes, composed HCP rows, and prepared payloads coexist in memory during full sync. | Memory scales with contracted population size. Measure peak RSS on a representative export, then spool/index packets and write bounded chunks while preserving phase dependencies. A wall-clock timeout alone does not control memory. |
| Low | [`apply_updates`](../integration/hierarchy.py) falls back to one write per account on any UpdateMultiple error and continues past service errors. | During a Dataverse outage each hierarchy batch costs about three requests per account instead of stopping, unlike `bulk_upsert_by_field`, which stops at the first service error. Fall back only for record errors, or stop at the first single service failure. |
| Low | Offline tests that build an engine without a log directory write `sync_*.log` files into the repo's `logs/`. | The folder is gitignored, but test runs mix with real run logs, so local logs cannot be trusted as evidence of live behavior. Give those tests a temporary `SyncLogger` directory. |

Additional operational limits: job/failure tracing remains best effort if the
initial job row cannot be created. Heartbeat writes use the same Dataverse
identity as the sync: they are sent once without retries, and Dataverse retry
waits do not send them, because under throttling they would be throttled too.
The longest silent stretch is about 18 minutes (a 5-minute beat interval, two
capped 5-minute waits and three timeouts), under the 20-minute busy check. A
separate lease/heartbeat store would make worker exclusion independent of
Dataverse availability.
Importing [`dataverse/auth.py`](../dataverse/auth.py) globally forces urllib3
to IPv4, affecting other clients in the process. Bulk capability exceptions
are tailored to this org and need discovery/verification for another schema.

**Fixes applied**

| Area | Result |
| --- | --- |
| Population completeness | Missing/ambiguous completion flags, missing continuation IDs, empty incomplete packets and malformed results now fail instead of silently truncating a successful load. A packet skipped because a response was lost is re-fetched by number (up to three, Developer Guide 6.13.4); a larger gap or a packet number going backwards fails. A valid empty completed population remains allowed, as the live October 8 captures show (`isTransactionComplete=true`, no results). |
| API error handling | Only an `ERROR` status raises; a `WARNING` envelope keeps its records. On `refreshMyOnekey`, `success:false` marks a packet that was not delivered: it is requested again (three attempts, 5 s then 10 s apart), as the vendor sample does (Developer Guide 6.13). Malformed envelopes produce typed errors without dumping whole result payloads. |
| Credentials and writes | OneKey requires HTTPS. Dataverse rejects foreign-origin pagination/request URLs and disables redirects. Single and batched updates use `If-Match: *` so an update cannot recreate a deleted row. |
| Retry behavior | Both clients read numeric and HTTP-date `Retry-After`. OneKey waits 30–300 s after a 429 (the quota is per minute) and at most 300 s after a 503; its waits refresh the job heartbeat every minute. Dataverse honours `Retry-After` up to 300 s per wait and does not refresh the heartbeat while waiting. A bulk call that fails for authentication or permissions raises; any other bulk failure falls back to single writes, which stop at the first single write that fails for a service reason, so an outage costs one more request rather than one per record. A write that fails inside a `$batch` is retried alone and the batch's successful writes keep their ids. Failure-row status updates stop at the first service error. |
| Failure recovery | Failure rows that Dataverse could not take (outage, throttling, token or permission errors) are kept for the next checkpoint, and a job that finishes with such rows is marked unsuccessful so it does not cover its window. A gateway timeout is checked by `ts_failurekey` before anything is resent, and a rejected row that already exists counts as written. A row Dataverse rejects is written once more without its optional columns; if that also fails it is logged as an error with its source ID and counted in `ts_phasecounts` (`trace_failures_dropped`), without failing the job, because it would fail again on every run. |
| Failure resolution | Scheduled resolution matches phase, entity, source ID and external key. A successful account write cannot clear an address/hierarchy/affiliation failure; a current failure prevents clearing the corresponding older failure. Hierarchy success records include already-linked children. |
| API boundaries | `$batch` enforces/splits at 1,000 requests. OneKey preflights the documented 30-criteria maximum, and rejects a search by the entity's own ID (for example `activity.activityEid` in an ACTIVITY search) combined with other criteria (Developer Guide 6.3.2). A related ID such as `workplace.workplaceEid` in an ACTIVITY search stays an ordinary filter; the sample sync relies on it. Invalid attempt counts/timeouts/configuration fail early; short-lived Dataverse tokens are renewed before expiry. |
| Mapping correctness | CSV choice/state values become JSON integers consistently. Specialty fallback considers qualifications even when unrelated ADA entries exist; compound-key ranks are parsed consistently. Dry-run affiliations no longer falsely fail solely because write-time GUID lookups are skipped. `processCode` R is REFRESH, "updated or inserted" (Developer Guide 6.13.5), and is upserted like I and U; the [`value_mapping.csv`](../config/value_mapping.csv) description and the delta's extract count (`refreshed`, formerly `removed`) now say so. |
| Redundancy/performance | Removed duplicate choice conversions and an unreachable transform branch. Primary-address selection uses a linear minimum instead of sorting all addresses. Numeric unchanged checks preserve large-integer precision, avoiding skipped updates. Existing bulk writes, lookup caches, one-pass full population extraction and skip-unchanged behavior remain in place. |
| Offline tests | Live OneKey tests require `pytest --live`, even if credentials exist. The example page size stays at 1,000 (see **OneKey limits**). |

Historical Activity lookup failures used related account/contact IDs as their
external keys. New failures use the actual affiliation key. Old rows may stay
Pending under exact matching. They are non-retryable lookup failures, so the
retry workflow never selects them: mark them Ignored or Resolved once, for
example by their parent job, rather than waiting for a replay. Failure-key
deduplication still groups multiple failed W2W edges from one
workplace/phase/operation into one stored retry row; the retry handler replays
that workplace's relations together.

**OneKey limits and subscription behavior**

The newer local [START HERE Guide](reference/1.%20OneKey%20API%20-%20START%20HERE%20Guide.pdf)
pp. 26–27 lists these **common configurable settings**, not verified limits
for this connector:

| Endpoint/control | Common setting |
| --- | --- |
| Each individual endpoint | 50 calls/minute, counted separately by endpoint |
| Search | 100 returned results/request; 10 IDs/request |
| Population refresh | 250 returned records/request |
| Code search | 5,000 results/request |
| Population submit | 250 IDs/request |

Activation details contain the actual connector caps (START HERE pp. 11–12,
26–27). Obtain those values before changing concurrency. The older
[Developer Guide](reference/4.%20Onekey%20API%20Developer%20Guide.pdf) p. 49
describes a request range of 100–2,000. The backend applies the connector's
cap itself: a larger request "may reduce the number but not increasing it"
(START HERE 6.1.14), so requesting 1,000 against a 250 cap returns 250 per
packet at no cost, while requesting fewer than the cap adds packets, packet
delays and calls against the per-minute quota. The implementation and
`.env.example` request 1,000; the live delta captures requested 2,000.

Search does not paginate. A broad search may report many matches while only
returning its capped subset; use population traversal for complete extraction.
Search criteria are limited to 30, and a search by the entity's own EID must
carry no other criterion (START HERE p. 16; Developer Guide p. 18). By-ID
reads return contracted detail and consume subscription usage; avoid
speculative paid hydration outside the contracted population.

Packet pacing is local to each walk, not a shared endpoint limiter. Multiple
workers and by-ID retry/hydration calls can exceed a shared connector quota.
For example, a 1.5-second packet delay permits up to 40 packet calls/minute per
worker before network/processing time; two workers could exceed a common
50-call endpoint quota. Use the actual contract cap and a shared limiter.

The population completion flag and transaction sequence govern traversal;
previously delivered packets can be replayed by packet number (Developer
Guide pp. 49–51; START HERE p. 19). Source propagation lag varies, the feed is
not a historical snapshot, and identical time-range queries can differ
(Developer Guide pp. 53–54). The 15-minute overlap reduces some exposure but
is not a documented completeness guarantee. Use periodic reconciliation and
subsequent deltas for long initial loads. Newly subscribed entities generally
appear after nightly working-day recalculation (START HERE p. 19).

START HERE p. 20 describes Optimized V2 request/read/ack endpoints. This client
implements the older refresh flow; confirm which feed this connector supports
before planning a resume/ack implementation. Local vendor PDFs are gitignored
and must be supplied separately when reproducing this review.

**Dataverse limits**

Default service protection applies per authenticated user and web server:
6,000 requests per 300 seconds, 1,200 seconds of combined execution in the
same window, and 52 or more concurrent requests. Values can vary by environment;
the server's `Retry-After` controls recovery, and this client honours it up to
300 s per wait. See
[Microsoft service protection limits](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/api-limits).

`$batch` allows at most 1,000 requests, with ordered results. It reduces HTTP
round trips but still consumes execution time and does not remove licensing
entitlements. See [Microsoft batch documentation](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/webapi/execute-batch-operations-using-web-api).

Standard-table bulk messages have no fixed record-count maximum: payload size,
execution time, plug-ins and table support govern usable size. Microsoft
recommends starting with 100–1,000 small records. Any error rolls back a
standard-table bulk operation; elastic tables can partially succeed. This
integration's bulk fallback/recovery assumptions target standard tables. See
[Microsoft bulk guidance](https://learn.microsoft.com/en-us/power-apps/developer/data-platform/bulk-operations).

Daily Power Platform request entitlements are separate from service protection
and depend on licensing/application-user allocations. Batching does not create
unlimited entitlement; verify the tenant allocation and usage reports. See
[Microsoft request allocations](https://learn.microsoft.com/en-us/power-platform/admin/api-request-limits-allocations).

**Verification**

Run `.venv/bin/python -m pytest -q` for the full offline regression suite.
Final result: **429 passed, 5 live tests skipped**. Python compilation and
the whitespace check with `cr-at-eol` (preserving existing CRLF files) passed.
Regression coverage includes incomplete population streams, lost-packet
recovery, `success:false` packet retries, capped retry hints and heartbeat
callbacks, bounded bulk fallback, update-only semantics, credential URL
boundaries, numeric precision, mapping types, scoped retry resolution, and
retained, re-checked and dropped trace failures. Each corrected behavior was
also checked by reverting it and confirming a test fails. Run live tests only
with an explicit `--live` flag and appropriate connector credentials.

**Revisions after the second review**

| First-round change | Problem | Now |
| --- | --- | --- |
| Dataverse retry waits refreshed the job heartbeat every minute. | The heartbeat is a Dataverse write with its own retries, so under throttling every wait spawned throttled heartbeat waits: with `Retry-After: 300`, one request waited 7,200 s and made 36 calls instead of 600 s and 3. | Dataverse waits do not refresh the heartbeat; the heartbeat is sent once, without retries, and retried a minute after a failure. |
| Unwritten failure rows failed the job. | A gateway timeout was handled as an outage before the "check what landed" step, so rows that were written were resent, hit the unique `ts_failurekey`, and failed the job; a row Dataverse always rejects failed every run, so the window never advanced. | See **Failure recovery** above. |
| Any ID field had to be the only search criterion. | `sample_ok_to_dv` searches activities by `workplace.workplaceEid` plus the default status filter and raised `ValueError` on every run. | Only the entity's own ID is exclusive. |
| Service errors inside a `$batch`, and bulk 429/5xx, raised at once. | A throttled write inside a batch discarded the batch's successful writes and stopped the codebase, where a single retry would have succeeded; a bulk 5xx caused by the batch itself was not retried. | See **Retry behavior** above. |
| `Retry-After` was honoured without floor or cap. | `Retry-After: 1` on OneKey's per-minute quota spent every attempt in seconds; a huge value parked a worker for hours while the heartbeat kept the job looking alive. | 30–300 s for a OneKey 429, at most 300 s otherwise. |
| `success:false` raised on every OneKey endpoint. | The vendor's own sample treats it as a packet to request again; raising ended an hours-long full sync on one transient packet. | Handled on `refreshMyOnekey` only; see **API error handling**. |
| `.env.example` requested 250 records per packet. | OneKey cuts a larger request to the cap, so a smaller request only adds packets and calls against the per-minute quota. | 1,000. |
| `processCode` R was reported as needing replacement reconciliation. | The guide defines R as REFRESH (update or insert); the code's upsert was correct. | Description and log label corrected. |
| The README and `onekey/auth.py` docstring dropped the live-verified authentication evidence. | It recorded a verified result (no auth → 401, Basic → 200). | Restored. |
