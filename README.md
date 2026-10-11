# OneKey ↔ PAION Integration

Loads IQVIA **OneKey** healthcare professional (HCP) and organisation (HCO)
reference data into **Dynamics 365 / Dataverse**.

See the [code review and API limits](docs/CODE_REVIEW.md) for verified fixes,
remaining data consistency risks, and connector-specific OneKey limits.

```
OneKey workplace  (HCO) ──▶ Dataverse account
OneKey individual (HCP) ──▶ Dataverse contact
```

## Layout

| Path | |
| --- | --- |
| [`onekey/`](onekey/) | IQVIA OneKey API client. Knows nothing about Dynamics. |
| [`dataverse/`](dataverse/) | Dataverse Web API client. Knows nothing about OneKey. |
| [`integration/`](integration/) | The only place that knows about both — mapping and sync. |
| [`scripts/`](scripts/) | CLI entry points and maintenance scripts. |
| [`tests/`](tests/) | Offline regression tests and opt-in live tests. |
| [`docs/`](docs/) | [Architecture](docs/architecture.md) and vendor reference material. |

See [docs/architecture.md](docs/architecture.md) for why the two API clients
stay independent.

## Setup

```bash
pip install -r requirements.txt      # or: pip install -e ".[dev]"
cp .env.example .env                 # then fill in the two secrets
```

`.env` is gitignored and loaded automatically — no `source .env` needed.

## The three scripts

### 1. Retrieve from OneKey

```bash
python scripts/retrieve_onekey.py --test                     # check credentials
python scripts/retrieve_onekey.py --type hcp --last-name Schmidt
python scripts/retrieve_onekey.py --type both --limit 5 --out data/sample.json
```

### 2. Read and write Dataverse

```bash
python scripts/dataverse_records.py whoami                   # check the token
python scripts/dataverse_records.py list
python scripts/dataverse_records.py upsert --in data/sample.json
python scripts/dataverse_records.py delete --prefix WDE --dry-run
```

### 3. The whole flow

```bash
python scripts/run_full_sync.py --dry-run
python scripts/run_full_sync.py --hco 5 --hcp 5
```

Steps 1 and 2 chain through a JSON file so you can inspect or edit the data in
between. Step 3 is the same flow without the intermediate file — and it links
each contact to its own workplace account, which the two-step route cannot do
when the HCP and HCO searches are independent.

## Authentication: two different models

|  | OneKey | Dataverse |
| --- | --- | --- |
| Mechanism | HTTP Basic, every request | OAuth2 client credentials |
| Token | **none exists** | bearer, 1 hour, cached and renewed |
| Identifier | `connector_id` in the URL path | `tenant_id` in the token request |

**OneKey issues no token.** Verified against 272 pages of vendor documentation
(the only OAuth2 mention is a 2020 note that it might arrive "in next release")
and against the live service: no auth → `401`, Basic → `200`. The
`connector_id` is not part of any handshake — it is the last path segment of
each endpoint (`/ok/search/494`).

Dataverse does issue a real token. The `/.default` scope suffix is required;
Entra rejects the request without it.

## Identity and idempotency

| | Dataverse column | Holds |
| --- | --- | --- |
| account (HCO) | `cree7_externalid` | OneKey `workplaceEid` |
| contact (HCP) | `externaluseridentifier` | OneKey `individualEid` |

`cree7_externalid` is the org alternate key (`cree7_onekeyid`). Contact has no
dedicated OneKey column, so the out-of-box `externaluseridentifier` is borrowed.
Records are matched before writing, so a re-run updates rather than duplicates.

## What gets loaded

**Identity and classification only. Addresses are not written.**

| account (HCO) | contact (HCP) |
| --- | --- |
| `name` | `firstname`, `lastname` |
| `cree7_externalid` | `externaluseridentifier` |
| `description` — type, specialty, provenance | `jobtitle`, `gendercode` |
| | `parentcustomerid` → its account |

## Known blocker: address writes fail

**Any `address1_*` write to account or contact fails** with SQL error 2601,
duplicate value for a unique constraint. This affects the whole org, not just
this integration.

```
customeraddress.cree7_externalkey → KeyAttributes: ['addressnumber']  (Active)
```

`addressnumber` is **not** unique by design — Dynamics numbers every record's
addresses 1 and 2, so only one row in the entire org can have
`addressnumber = 1`. One already exists, so every later address insert
collides. Confirmed by probe: `{"name":"X"}` succeeds,
`{"name":"X","address1_city":"Berlin"}` fails; same on contact.

**Fix:** delete or deactivate that key (Tables → Address → Keys). It cannot do
what it appears intended to do; an external-id key on addresses needs a
dedicated custom column. Then pass `--with-address` — the mapping is written
and tested, just switched off.

## OneKey behaviour worth knowing

**`/ok/search` cannot paginate.** `resultSize` is the only control — no offset,
no cursor. A search reporting 127,879 matches still returns only `resultSize`
records. Use `client.iter_population()`, the one paged endpoint.

**Endpoints are subscribed per connector.** On connector 494, `/ok/count` and
`/ok/isInMyOnekey` return `403` while `/ok/search` works.

**Population continuations must repeat `codBase`.** Omitting it returns `403`,
not the `400` a missing parameter would suggest.

**Search by id is billed; search by criteria is not.** By criteria returns a
thin identifying subset; by id returns every contracted attribute and counts
against the subscription.

**Territory is required and exclusive.** Pass `cod_base` (`WDE`) or `iso_cod2`
(`DE`), never both.

**A person appears once per workplace.** Three affiliations means three
ACTIVITY rows sharing an `individualEid`. Syncing keys on `individualEid`, so
they collapse to one contact; key on `activityEid` for one per location.

## Tests

```bash
pytest                  # offline; live tests skip even with credentials
pytest tests/test_onekey.py
pytest tests/test_live.py --live  # explicit live API calls; loads .env
```

Live tests count against OneKey limits and require configured credentials.

## Azure Function

Scheduled syncs run as a Python Function App. Timers enqueue one codebase at a
time; the queue worker does the sync and Azure retries failures onto
`onekey-queue-poison`. Setup, plan choice, and app settings are in
[docs/AZURE_FUNCTION_SETUP.md](docs/AZURE_FUNCTION_SETUP.md).

## Environments

| | OneKey | Dataverse |
| --- | --- | --- |
| Production | `https://okws.ok.imshealth.com` | — |
| UAT | `https://okws-uat.onekey.iqvia.com` | `https://paion-crm-test.api.crm4.dynamics.com` |
| QA | `https://okws-qa.ok.imshealth.com` | — |
