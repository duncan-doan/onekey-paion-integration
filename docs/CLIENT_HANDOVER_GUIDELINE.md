# OneKey to Dataverse Integration — Client Handover Guideline

This document describes how the PAION integration loads IQVIA OneKey healthcare data into Microsoft
Dataverse (Dynamics 365). It is written for the client team that will operate, extend, and hand the solution
on.

It covers four subjects:

- **Architecture** — how records are retrieved from OneKey, transformed, and written to Dataverse, phase
  by phase.
- **Dynamic mapping** — how objects and fields are configured, including custom tables and columns.
- **Azure queue sync** — how the scheduled jobs run and stay consistent across countries.
- **Failure and retry** — how a failed record is stored and replayed, and how a failed country message is
  retried.

The configuration that drives the live mapping lives in `config/`. The behaviour described here matches
that configuration and the sync engine in `integration/`.

## 1. Architecture

### 1.1 What the integration does

The integration copies the connector's contracted OneKey population into Dataverse and keeps it current.

| OneKey concept | Dataverse table | Business meaning |
| --- | --- | --- |
| Workplace | `account` | Healthcare organisation (HCO): hospital, practice, department |
| Individual | `contact` | Healthcare professional (HCP): one contact per person |
| Activity | `contact.ts_department`, `ts_hospital` and `ts_hospitalgroup` plus `ts_affiliation` | The person's workplace. The main activity sets the contact's department (its workplace account), hospital (that workplace's parent) and hospital group (the hospital's parent). Every activity, including the main one, is also an affiliation row. |
| Workplace address | `customeraddress` | Address rows owned by the account |
| Workplace-to-workplace relation | `ts_affiliation` | HCO–HCO links (hierarchy, group, and similar) |
| Activity role (TIH) | `ts_affiliationrole` | One row per role code on an HCP–HCO affiliation |
| Code lists (type, specialty, title, role, and others) | Lookup tables such as `ts_specialty_ref` | Country-specific reference data |
| Short codes (gender, status, and similar) | Dataverse choice columns | Small lists that mean the same thing in every country |

A re-run updates existing rows. It does not create a second account or contact for the same OneKey id.

### 1.2 System layout

Three layers stay separate. OneKey code never calls Dataverse. Dataverse code never calls OneKey.
Everything that joins the two systems lives in the integration layer and is driven by CSV configuration.

```
    IQVIA OneKey                         Microsoft Dataverse
    HTTP Basic, no token                 OAuth2 bearer token
            │                                    ▲
            ▼                                    │
    ┌───────────────┐                   ┌────────────────┐
    │ onekey/       │                   │ dataverse/     │
    │ search        │                   │ Web API        │
    │ population    │                   │ create/update  │
    │ code lists    │                   │ lookups        │
    └───────┬───────┘                   └────────▲───────┘
            │                                    │
            │         ┌──────────────────┐       │
            └────────▶│ integration/     │───────┘
                      │ validate         │
                      │ map (CSV)        │
                      │ transform        │
                      │ sync by phase    │
                      └────────▲─────────┘
                               │
                  Azure Function App
                  timers enqueue work
                  queue worker runs one country
```

| Layer | Responsibility |
| --- | --- |
| `onekey/` | Talks to the OneKey API: authentication, search, population packets, code lists. |
| `dataverse/` | Talks to the Dataverse Web API: token, retrieve, create, update, bulk lookup. |
| `integration/` | Loads the CSV mapping, validates it, transforms each record, and runs the phases in order. |
| `config/` | The mapping. Changing a field target is a CSV change, then a redeploy of the Function App so the new files are in the package. |
| `function_app.py` | Azure timers and the queue worker. They call the same engine as a manual run. |

### 1.3 Authentication

The two systems use different credentials. Neither credential is stored in source control. Local runs read
`.env`. Azure runs read Function App settings of the same names.

| | OneKey | Dataverse |
| --- | --- | --- |
| Mechanism | HTTP Basic on every request | OAuth2 client credentials against Microsoft Entra |
| Token | None. There is no token endpoint. | Bearer token, about one hour, cached and renewed five minutes before expiry |
| Identity | `ONEKEY_CONNECTOR_ID` is the last segment of each URL (`/ok/search/{connector}`) | `DATAVERSE_TENANT_ID` is sent on the token request |
| Settings | `ONEKEY_ENV` or `ONEKEY_BASE_URL`, `ONEKEY_USERNAME`, `ONEKEY_PASSWORD`, `ONEKEY_CONNECTOR_ID` | `DATAVERSE_URL`, `DATAVERSE_TENANT_ID`, `DATAVERSE_CLIENT_ID`, `DATAVERSE_CLIENT_SECRET` |

The Dataverse scope must end in `/.default`. Entra rejects the token request without it. The application
user needs create, read, and update rights on every table the mapping writes, including the operational
tables in section 1.8.

Typical bases:

| Environment | OneKey |
| --- | --- |
| Production | `https://okws.ok.imshealth.com` |
| UAT | `https://okws-uat.onekey.iqvia.com` |
| QA | `https://okws-qa.ok.imshealth.com` |

`ONEKEY_ENV` accepts `uat`, `prd`, or `qa`. A full `ONEKEY_BASE_URL` overrides it.

### 1.4 How records are retrieved from OneKey

Two OneKey APIs are used. They are not interchangeable.

**Population refresh — the sync path.**
`POST /pop/refreshMyOnekey` is the only endpoint that pages. The first call opens a transaction and
returns `transactionUniqueIdentifier`. Each following call sends that id again, and must repeat
`codBase`. Omitting the territory on a continuation returns HTTP 403. Packets continue until
`isTransactionComplete` is true. Page size is 100–2000 records (`ONEKEY_PAGE_SIZE`, default 1000).

- A full sync calls it with the territory only. OneKey returns the contracted population.
- A delta sync calls it with `isoDateFrom` and `isoDateTo`. OneKey returns entities that changed in
  that window. Each packet row carries `processCode`: I (insert), U (update), R (refresh, upserted
  like an update). A delta has no removal signal: only a full sync sees what left the contract.

A packet whose answer is lost (a timeout or a reset) is not requested again blindly, which OneKey
can refuse with HTTP 400. Following Developer Guide 6.13.4, the walk asks for that packet by
number; if OneKey says it was never delivered (OKWS-3208), it asks for the next packet again, up
to three times. The connection keeps OneKey's load-balancer cookie, which ties the walk to the
server holding its transaction, and a refused request reports OneKey's reason (the OKWS code).

Each row has `entityType` of WORKPLACE, INDIVIDUAL, or ACTIVITY. The engine keeps only contracted
ids from this walk. A later search is not used to add organisations or people that are outside the contract.

**Search — lookup and samples, not the full load.**
`POST /ok/search` cannot page. `resultSize` is the only limit, and there is no offset. A search that
matches tens of thousands of rows still returns only `resultSize` records. Search by criteria returns a thin
identifying set and is not billed. Search by id returns the contracted attributes and is billed. The sample
function and ad-hoc retrieval use search. The scheduled full and delta jobs do not.

**Code lists.**
`POST /ok/searchCodeList` returns one list for one territory and language (for example German specialty
codes for WDE). The language follows the territory: WUK English, WDE German, WNL Dutch, WDK Danish, WNO
Norwegian Bokmål, WSE Swedish, WFI Finnish.

Territory is exclusive. Each request sends `codBase` (for example `WDE`) or an ISO country, never both.

### 1.5 How records are written to Dataverse

For each source row the engine:

1. Flattens the OneKey JSON into a stable set of field names (`flatten_hco`, `flatten_hcp`, and the
   affiliation builders).
2. Applies every `entity_mapping.csv` row whose target table matches the phase.
3. Resolves lookups to existing Dataverse rows and writes them as `@odata.bind` navigation properties.
4. Upserts by the external id: find the row whose key column equals the OneKey id, then PATCH it, or
   POST it when it is absent.

Writes go through the Dataverse Web API (`/api/data/v9.2`). Lookups of many keys are batched so the
filter URLs stay within limits. Accounts and contacts can use bulk create and update. In this environment,
`customeraddress` and `ts_affiliation` are written one row at a time.

A lookup that cannot be resolved is omitted or the row fails, depending on whether the mapping marks the
relationship required. The failure is stored (section 1.8) and can be retried.

Unmapped choice codes are left blank. They are listed in `logs/picklist_gaps_*.csv` on a machine that
has a writable log directory. The Azure app does not write those files unless `SYNC_LOG_DIR` points at a
writable path; the host log and the trace tables are the operational record there.

### 1.6 Identity

Every business table is matched on a single external id before it is written. The column is `ts_externalid`.
Dataverse should have an alternate key on that column so the platform itself rejects duplicates.

| Dataverse table | `ts_externalid` contains | OneKey source |
| --- | --- | --- |
| `account` | Workplace id | `workplaceEid` |
| `contact` | Individual id | `individualEid` |
| `customeraddress` | `{workplaceEid} - {addressEid}` | Workplace plus address |
| `ts_affiliation` (HCO–HCO) | `{fromWorkplaceEid} - {toWorkplaceEid} - {linkCode}` | `workplace.relations[]` |
| `ts_affiliation` (HCP–HCO) | Activity id | `activityEid` |
| `ts_affiliationrole` | `{activityEid} - {role code}` | One TIH code on that activity |
| Code lookup tables | `{codBase}.{list}.{code}` | Example: `WDE.SP.01` |

One person is one contact, even when they practise at several workplaces. The main activity supplies
`ts_department`, `ts_hospital` and `ts_hospitalgroup`. The other workplaces are affiliation rows, not extra contacts.

OneKey treats an address as shared across workplaces. Dataverse keeps Account to Address as one-to-many,
so the same street under two organisations becomes two `customeraddress` rows. They share the
street text and differ by the composite key and the parent account.

Addresses are written only to `customeraddress`, never to `account.address1_*` or
`contact.address1_*` directly. Every address of a workplace is loaded. OneKey keys each one in
`workplaceAddresses` as `{typeCode},{rank}`: P is the main address, M the mailing address, S a
secondary one. They become the account's address rows in this order, numbered by `addressnumber`:

| OneKey | `addressnumber` |
| --- | --- |
| P,1 (main) | 1 |
| M,1 (mailing) | 2 |
| S,1, S,2, S,3 (secondary) | 3, 4, 5 |
| any other type | after those, by type and rank |

A missing type moves the next one up, so the numbers always run from 1 without a gap: a workplace with a
main and two secondary addresses has them on rows 1, 2 and 3. Rows 1 and 2 also feed the account's
Address 1 and Address 2 fields. An address linked twice (under two types) is written once, at its first
place.

Each sync puts the rows in that order before it writes them (`integration/address_rows.py`):

- the row that already has a number is reused, including the empty rows 1 and 2 Dataverse creates, so
  no number is held twice;
- a row the sync wrote earlier that no address needs any more (its address left OneKey, or it moved to
  another number) is deleted; rows 1 and 2 are emptied instead, and so is a row Dataverse will not
  delete;
- rows without a OneKey key (added by hand) are never deleted, but a row that has a number a OneKey
  address needs is reused for it;
- a workplace that arrives with no address at all keeps its rows unchanged;
- if a step still fails, that account is written as before, without renumbering, and the run goes on.

How many address rows an account or contact gets depends on two Dataverse environment settings; see
**Dataverse address settings** in section 3.8.

### 1.7 Sync phases

Full sync and delta sync use the same order. Parents exist before children that point at them. Each phase
checkpoints the job row, so a long run stays visible and a restart can be diagnosed from the last completed
phase.

```
    0  Picklists and code lookups
    1  HCO accounts
    2  Addresses
    3  HCO parent hierarchy
    4  HCO–HCO affiliations
    5  HCP contacts, then (full sync) records that left the contract are retired
    6  HCP–HCO affiliations and role rows, then account primary contacts
```

#### Phase 0 — Picklists and code lookups

Two different stores:

- Short choices (gender, status, language, working status, link type, and similar) are upserted as
  Dataverse choice options from `value_mapping.csv` and `picklist_targets.csv`. They do not
  depend on the country. The Azure timer does this once, before it enqueues countries. If that step
  fails, each country worker does it itself.
- Large lists (HCO type TET, specialty SP, category LEX, legal category CAJ, title TIT, role TIH) are
  loaded from `/ok/searchCodeList` into the lookup tables named in
  `lookup_table_mapping.csv`. This part is per country, because the same local code can mean
  different things in different countries.

A picklist failure is recorded and the HCO and HCP phases still run. Missing options are omitted on the
record rather than blocking the load.

#### Phase 1 — HCO accounts

Workplaces from the population walk are flattened and upserted to `account`. The parent account is not
bound in this phase. Binding it here would fail whenever the parent appears later in the same extract.

#### Phase 2 — Addresses

Each workplace address becomes a `customeraddress` whose `parentid` is the account from phase 1, on
the row its OneKey order gives it (section 1.6). The external id is `{workplaceEid} - {addressEid}`.

#### Phase 3 — HCO hierarchy

`managerWorkplaceEid` is resolved to an account and written to `parentaccountid`. Both ends must be
in the contracted set for this run. A parent that is not in the contract is left unlinked and counted as missing.

#### Phase 4 — HCO–HCO affiliations

Each entry in `workplace.relations[]` becomes a `ts_affiliation` between two accounts
(`ts_fromid` and `ts_toid`). The link type (hierarchy, group, and the rest) is a choice decoded from
`value_mapping.csv`. Both workplaces must be contracted.

#### Phase 5 — HCP contacts

Individuals are composed with their main activity and that activity's workplace, then upserted to `contact`.
An activity OneKey marks inactive (its `stateCode` decodes to Inactive) is not used: the next active one
places the contact, and a person with only inactive activities is still written (name, state) with
their department, hospital and group left as they are. Inactive activities give no level either.
`ts_department` points at the main workplace account, `ts_hospital` at that workplace's parent and
`ts_hospitalgroup` at the hospital's parent. Each parent is OneKey's `managerWorkplaceEid` first, then the
HIE ("Is managed by") parent when that is a group of organisations (structure GOR or POR); a contact
whose workplace or hospital has neither gets the field cleared. The sync no longer writes
`parentcustomerid` or the `department` text. A person whose individual, activity, or
workplace is outside the contracted set for this extract is skipped.

The same resolved lookup chain sets `account.ts_hierarchylevel`: an Account referenced by
`ts_department` is level 3, by `ts_hospital` is level 2, and by `ts_hospitalgroup` is level 1.
If the same Account has several roles, the most direct HCP role wins (3 before 2 before 1).
Only active HCPs count: one whose OneKey stateCode makes the contact inactive (such as INA or
RET) gives no level, and an Account no active HCP points at has its level cleared.
During a complete full sync these values are derived in memory from the population and included
in the existing Account upsert, so this adds no Dataverse read or write pass. Only such a full
sync sets levels: a delta, a retry, a sampled run and an HCP-only run leave them as they are,
since they do not see the whole population.

After the contacts, a full sync over the complete population (not a sample, HCO-only or
HCP-only run, nor a delta) retires what left the contract. An account or contact of the
codebase that the walk did not return is deactivated. So is each active affiliation the sync
will not write again: an HCP–HCO row whose activity is gone or has an end outside the contract,
and an HCO–HCO row with an end outside it. A contact's `ts_department`, `ts_hospital` or
`ts_hospitalgroup`, and an account's `parentaccountid`, that point at an out-of-contract account
are cleared, and a parent outside the contract is no longer taken as a contact's hospital. A role row
whose activity no longer has that role is deactivated too, and comes back when OneKey sends it again.
Nothing
is deleted: a record that returns to the contract is written with its OneKey state, which
reactivates it. This runs before Phase 6, so the primary contacts see the result.

A walk that lost packets would make valid records look gone. When more than
`SYNC_CONTRACT_CLEANUP_MAX_PERCENT` (default 5) of a codebase's active accounts, contacts or
affiliations would be deactivated, nothing is written and the job ends Partially Succeeded
with the counts in its error summary. Check them, then raise the setting for one run if the
cleanup is expected, such as the first one. `--dry-run` reports the counts without writing.
A delta never deactivates an affiliation, not even an HCO–HCO relation a changed workplace no
longer lists; it writes only what OneKey sends, including an inactive state OneKey sends for an
activity. A delta can still link a new activity to a retired account; the next full sync retires
it again.

#### Phase 6 — HCP–HCO affiliations and roles

Every activity, not only the main one, becomes a `ts_affiliation` from the contact to the account. Each
TIH role code on that activity becomes a `ts_affiliationrole` row looking up `ts_role_ref`.

Then each account's `primarycontactid` is set to its primary HCP: the contact of the account's
active affiliation with `ts_isprimary` (OneKey `isMainActivity`). Of several, the earliest start
wins, then the lowest activity ID, so an HCP who joins later does not replace the current one. An
account without one is cleared, and a value set by hand is overwritten: the sync owns the column
(mapping row HCO-028; remove it to stop). A full sync reads the codebase's accounts and primary
affiliations once; a delta re-checks the accounts it wrote affiliations to and the accounts whose
primary contact is one of the contacts it wrote, or reads the whole codebase when that is more
than 1,000 accounts and contacts. The writes are PATCHes in a `$batch`.

After a successful job, pending failure rows whose OneKey id was written successfully in this run are
marked resolved. Section 4 describes that path.

### 1.8 Operational trace

Two custom tables record the run. They are not part of the HCO/HCP model. Column definitions are in
`docs/DATAVERSE_TRACE_TABLES.md`.

| Table | One row represents |
| --- | --- |
| `ts_integrationjob` | One execution: type (Full or Delta), codebase, status, start, watermark, heartbeat, and per-phase counts |
| `ts_integrationfailure` | One source record that failed in that job, with the OneKey id, phase, and whether it may be retried. Section 4 is the lifecycle of these rows |

A job stays Active in Dataverse even when the business status is Failed, so the row remains visible.
Outcome is `ts_status`, not the system status.

While a job runs, every OneKey or Dataverse call can refresh `ts_lastheartbeat`. A live job is one whose
status is Running and whose heartbeat is under 20 minutes old. A process that was killed keeps the
Running status, but the heartbeat stops, so it stops blocking the next run after 20 minutes.

### 1.9 Manual runs

The same engine can be run from a machine that has the credentials, without Azure:

```bash
python scripts/run_dynamic_sync.py --validate-only
python scripts/run_dynamic_sync.py --dry-run --codbase WDE --max-records 20
python scripts/run_dynamic_sync.py --codbase WDE --max-records 100
python scripts/run_dynamic_sync.py --delta --codbase WDE --date-from 2026-10-01 --date-to 2026-10-02
```

`--validate-only` checks the CSV and the Dataverse connection and writes nothing. `--dry-run` walks
the transform without upserting. Scheduled production loads are the Azure jobs in section 3, not these
commands.

## 2. Dynamic mapping

### 2.1 Strategy

Field mapping is data, not code. A spreadsheet row says which OneKey value lands in which Dataverse
column, how it is converted, and whether it is the upsert key or a lookup. The sync engine loads the CSVs at
the start of the job and applies them to every record.

That split exists so a new custom column, a new code, or a new custom table can be added by someone
who knows the Dataverse schema and the OneKey field, without changing the Python that walks the
population.

The engine will only emit a column that has a row. Adding a column in Dataverse does nothing until
`entity_mapping.csv` names it. Removing a row stops that column from being written; it does not clear
the value already stored.

CSV rules:

- The first row is the header. Do not rename columns.
- A line whose first field starts with `#` is a comment.
- Booleans are `TRUE` or `FALSE`.
- Leave a cell empty rather than writing `NULL` or `N/A`.
- Do not put an unquoted comma inside a cell. The parser splits on commas.

After a mapping change, run validation, then a small dry run, before a full country load. Redeploy the
Function App so Azure uses the new files. The package includes `config/*.csv`.

### 2.2 Configuration files

| File | Question it answers |
| --- | --- |
| `entity_definition.csv` | Which OneKey and Dataverse objects exist, their API names, upsert keys, and load order |
| `entity_mapping.csv` | Which source field maps to which target column, and how the value is converted |
| `relationship_mapping.csv` | Which lookup depends on which parent, and the order those links are safe to write |
| `lookup_table_mapping.csv` | Which large OneKey lists are lookup tables, and how their external id is built |
| `value_mapping.csv` | How a short OneKey code becomes a Dataverse choice integer or label |
| `picklist_targets.csv` | Which choice (global or local) receives those short codes |
| `field_constraints.csv` | Max length and required checks, and whether a too-long value is truncated or rejected |

### 2.3 How one mapping row is applied

`entity_mapping.csv` is the file that changes most often. One row is one source value contributing to one
target column.

| Column | Meaning |
| --- | --- |
| `mapping_id` | Stable id. Prefix shows the object: HCO, HCP, ADR, W2W, AFF |
| `source_entity` | Workplace, Individual, or Activity |
| `source_field` | Name on the flattened record the engine actually reads |
| `source_field_path` | Path in the raw OneKey JSON, kept for documentation |
| `target_entity` | Dataverse logical name: `account`, `contact`, `customeraddress`, `ts_affiliation`, `ts_affiliationrole`, or a custom table |
| `target_field` | Dataverse logical column name |
| `target_field_type` | string, integer, picklist, lookup, boolean, datetime, state |
| `max_length` | Truncation length for strings |
| `is_required` | The source value must be present |
| `is_key` | This value is part of `ts_externalid`. Several rows can build one key with `append` |
| `is_lookup` | The value is a foreign key, not stored as text |
| `lookup_entity` | Table to search |
| `lookup_key_field` | Column to match, almost always `ts_externalid` |
| `transform_rule` | Conversion. See the table below |
| `default_value` | Used when the source and any fallback are empty |
| `notes` | Why the row exists |

`source_field` must be a name the flatten step produces. A path that exists only in the raw JSON is
ignored until `onekey/parse.py` (or the affiliation flattener) exposes it under `source_field`. The engine
logs a warning the first time a mapped name is missing on a row.

### 2.4 Transform rules

| Rule | Effect |
| --- | --- |
| `direct` | Copy the value |
| `coalesce:otherField` | Use this value, otherwise `otherField`, otherwise `default_value` |
| `map:A=1\|B=2` | Replace a code with another code. An unknown code is omitted and recorded as a picklist gap |
| `decode:type` | Look up `type` plus the source code in `value_mapping.csv`. Unknown codes are omitted and recorded |
| `template:Label: {value}` | Build a string. Several template rows that target `description` are combined |
| `append: - {value}` | Append to the same target column. Used to build composite external ids |
| `constant:3` | Always write this literal |
| `lookup_by_key` | Do not copy the text. Find the Dataverse row whose key equals this value and bind the lookup |
| `or:otherField` | Boolean OR of this field and another |

`lookup_by_key` never writes the OneKey id into the lookup column. It writes a bind such as
`/ts_specialty_refs(ts_externalid='WDE.SP.01')` or `/accounts({guid})` after the parent has
been found. Catalog lookups (type, specialty, title, role) use the external-id form so they do not need a prior
GUID cache. Account, contact, and affiliation lookups are resolved to GUIDs because those parents were
upserted earlier in the same job.

### 2.5 Code lists: choice or lookup

Use a choice when the list is small and the code means the same thing in every country. Gender and status
are choices. Add the codes to `value_mapping.csv` and point `picklist_targets.csv` at the global or
local option set. The mapping row uses `decode:` or `map:`.

Use a lookup table when the list is large or the local code changes meaning by country. German specialty
01 is not the same value as another country's 01. The external id is `{codBase}.{list}.{code}` from
`lookup_table_mapping.csv`, so `WDE.SP.01` and `WUK.SP.01` are different rows. The mapping row uses
`lookup_by_key` and names that lookup table.

Current lookup tables:

| OneKey list | Dataverse table | Used on |
| --- | --- | --- |
| TET | `ts_hcotype_ref` | Account type |
| SP | `ts_specialty_ref` | Account and contact specialty slots 1–5. A slot OneKey no longer fills is cleared, so removing a specialty does not leave the old one in the last slot (account telephones 1–2 work the same way) |
| LEX | `ts_hcocategory_ref` | Account category |
| CAJ | `ts_hcolegalcategory_ref` | Account legal category |
| TIT | `ts_title_ref` | Contact title |
| TIH | `ts_role_ref` | Affiliation role |

The primary name on these tables is `ts_longlabel`. The country language is stored in
`ts_longlocalizedlabel`.

### 2.6 Relationships and load order

`relationship_mapping.csv` records which lookup may be written only after its parent exists. The phase
order in section 1.7 is that rule applied:

| Order | Link | Lookup |
| --- | --- | --- |
| 1 | Account to parent account | `parentaccountid` from `managerWorkplaceEid` |
| 2 | Contact to account | `ts_department` from the main activity's workplace, `ts_hospital` from that workplace's parent, `ts_hospitalgroup` from the hospital's parent |
| 3 | Affiliation between two accounts | `ts_fromid` and `ts_toid` |
| 4 | Affiliation from contact to account | `ts_fromid` and `ts_toid` |
| 5 | Address to account | `customeraddress.parentid` |

A required parent that is missing fails that child row. An optional parent that is missing leaves the lookup
empty.

### 2.7 Map an additional field on an existing table

Example: write OneKey fax number to a custom column `ts_fax` on `account`.

1. Create `ts_fax` on `account` in Dataverse (text, length agreed with the source). Publish the change.
2. Confirm the flattened workplace record exposes the value. Workplace telephones are already
   flattened as `telephone1` and `telephone2`. A brand-new JSON path must be added to the flatten
   step first; the CSV cannot read raw JSON by itself.
3. Add a row to `entity_mapping.csv`:

   ```
   HCO-040,OneKey,Workplace,fax,workplace.telephones.1_3.callNumberForSearch,Dataverse,account,ts_fax,string,50,FALSE,FALSE,FALSE,,,direct,,Fax number
   ```

4. Add a max-length constraint to `field_constraints.csv` if the column is shorter than the source.
5. Validate and dry-run:

   ```bash
   python scripts/run_dynamic_sync.py --validate-only
   python scripts/run_dynamic_sync.py --dry-run --codbase WDE --max-records 10
   ```

6. Redeploy the Function App.

Use the logical name, not the display name. `ts_fax` is the logical name. A display name such as "Fax" is
not accepted by the Web API.

For a choice column, add `value_mapping.csv` rows and a `picklist_targets.csv` row, and set the
transform to `decode:` plus that mapping type. For a lookup to an existing reference table, set `is_lookup`
to `TRUE`, name `lookup_entity` and `lookup_key_field`, and set the transform to `lookup_by_key`. The
value written into the key must already follow that table's external-id pattern.

### 2.8 Map a new custom table

Example: a custom table that should receive a new OneKey child collection. The affiliation table is the
pattern already in production. The steps are the same for another table.

1. Create the table in Dataverse, in the same solution and publisher as `ts_affiliation`.
   - Logical name, for example `ts_example`.
   - Entity set (the Web API collection), for example `ts_examples`. Dataverse pluralises this;
     confirm it in the table properties.
   - Primary name column.
   - Text column `ts_externalid`, long enough for the OneKey key, with an alternate key on that
     column. Wait until the key status is Active.
   - Lookup columns to `account`, `contact`, or other parents, with the Web API navigation names
     noted. Binds use those navigation names (`ts_FromID_contact@odata.bind`), not the
     logical column name alone.
   - Organisation ownership if the integration user should create rows without an owner assign
     right.
2. Register the object in `entity_definition.csv`. One row for the Dataverse table (entity set,
   primary key, alternate key field, `is_required`, `load_order`) and, if it is a new OneKey object, one
   row for the source. `load_order` must be after every table it looks up.
3. Register the relationships in `relationship_mapping.csv`. Name the child, the lookup column,
   the parent table, and the parent's key field (`ts_externalid`). Set `is_required` to `TRUE` only when
   a missing parent should fail the row.
4. Map the fields in `entity_mapping.csv`.
   - One or more rows with `is_key` `TRUE` that build `ts_externalid`. Use `direct` for the first part
     and `append: - {value}` for the rest.
   - One `lookup_by_key` row per lookup column.
   - One row per ordinary column.

   A minimal affiliation-style key and two lookups look like this:

   ```
   EX-001,OneKey,Activity,activityEid,activity.activityEid,Dataverse,ts_example,ts_externalid,string,50,TRUE,TRUE,FALSE,,,direct,,Upsert key
   EX-002,OneKey,Activity,individualEid,activity.individualEid,Dataverse,ts_example,ts_person,lookup,,TRUE,FALSE,TRUE,contact,ts_externalid,lookup_by_key,,Person
   EX-003,OneKey,Activity,workplaceEid,activity.workplaceEid,Dataverse,ts_example,ts_org,lookup,,TRUE,FALSE,TRUE,account,ts_externalid,lookup_by_key,,Organisation
   ```

5. Point the phase at the new table. A new target entity is transformed whenever the phase asks the
   engine to transform that entity name. The current phases already do this for `account`, `contact`,
   `customeraddress`, `ts_affiliation`, and `ts_affiliationrole`. A brand-new table that is not
   one of those targets needs a small addition in the sync phase that should write it (call the same
   transform and upsert the new entity set, after the parents). Mapping rows alone do not invent a new
   phase.
6. Add constraints in `field_constraints.csv` for required columns and string lengths.
7. Validate, dry-run a few records, then redeploy.

If the new table holds a large OneKey code list rather than a business record, do not follow the steps above.
Add a row to `lookup_table_mapping.csv` instead. The picklist phase loads every row in that file. The
external id template `{codBase}.{list}.{code}` should stay unless there is a reason to change it.
Account and contact mappings then point at the new table with `lookup_by_key`.

### 2.9 What not to change casually

- The key column and the key formula. Changing `ts_externalid`'s shape makes the next run insert
  duplicates beside the old rows.
- Phase order. Writing contacts before accounts, the hierarchy and the HCO–HCO relations leaves
  `ts_department`, `ts_hospital` and `ts_hospitalgroup` unresolved.
- `codBase` inside an external id for lookup tables. Dropping the country makes two countries overwrite
  each other's codes.
- Choice integers in `value_mapping.csv`. They must match the option values published in
  Dataverse, not the labels.

## 3. Azure queue sync flow

### 3.1 Why a queue

A country sync can run for hours. The timers do not do that work inside the timer invocation. They prepare
shared reference data, then place one message per country on an Azure Storage queue. A worker function
takes one message and runs that country to completion. A failure retries that country only.

```
    00:00 UTC
        │
        ▼
    Timer (delta most days, full on the 1st)
        │
        ├─ sync choice options once (gender, status, …)
        │
        └─ enqueue one JSON message per codebase
               │
               ▼
         onekey-queue
               │
               ├─ message WUK ──▶ worker instance ──▶ phases 0–6 for WUK
               ├─ message WDE ──▶ worker instance ──▶ phases 0–6 for WDE
               └─ message WFR ──▶ waits if the instance cap is reached
               │
               ▼
         success: message deleted, job row Succeeded or Partially Succeeded
         failure: message hidden 30 minutes, then retried
         give up: body copied to onekey-queue-poison
```

The Function App is Python v4. `host.json` sets `functionTimeout` to 12 hours. The plan must be Flex
Consumption or Premium. The classic Consumption plan stops a run at 10 minutes. Flex Consumption
schedules are UTC only.

### 3.2 Functions

| Function | Trigger | When (UTC) | What it does |
| --- | --- | --- | --- |
| `scheduled_onekey_full` | Timer `0 0 0 1 * *` | 00:00 on the 1st | Syncs choice options, then enqueues one full-sync message per codebase |
| `scheduled_onekey_delta` | Timer `0 0 0 2-31 * *` | 00:00 on every other day | Syncs choice options, then enqueues one delta message per codebase |
| `scheduled_onekey_worker` | Queue `onekey-queue` | When a message is available | Runs that one country |
| `sample_ok_to_dv` | Timer `0 0 12 1 * *` | 12:00 on the 1st | Upserts a few HCOs through the same phases. A connectivity check, not the production load. It skips itself if that country is already syncing |

The delta timer skips the 1st so it does not start in the same minute as the full sync. On the 2nd, the delta
window starts from the full sync.

Both scheduled timers stay idle until the app setting `SYNC_SCHEDULE` is `true`, `1`, or `yes`.
`sample_ok_to_dv` is not gated by that setting.

### 3.3 Which countries run

There is no country list in code. The app setting `ONEKEY_CODBASE` is a comma-separated list of OneKey
territories. The timer reads it when it fires. Duplicates are enqueued once. An empty setting uses WUK.

| `ONEKEY_CODBASE` | 1st of the month | Other days |
| --- | --- | --- |
| `WUK` | 1 full message | 1 delta message |
| `WUK,WDE,WFR` | 3 full messages | 3 delta messages |

Changing the setting takes effect on the next timer tick. No redeploy is required for a country-list change. A
mapping-file change still needs a deploy.

### 3.4 The queue message

```json
{"choices":false,"codebase":"WDE","date_to":null,"mode":"delta","retry_limit":100,"version":1}
```

| Field | Meaning |
| --- | --- |
| `version` | Must be 1. Any other version is rejected and sent to the poison queue |
| `mode` | `full` or `delta` |
| `codebase` | One territory, for example `WDE` |
| `date_to` | Leave null. The worker reads changes up to the moment it starts the delta. Set it only to replay an old window by hand. Not valid on a full message |
| `retry_limit` | How many pending failure rows this country replays before the delta. Default 100 |
| `choices` | `false` when the timer already synced the short choice options. `true` tells the worker to sync them |

The binding expects a Base64 body. A plain-text message cannot be decoded and goes to the poison
queue.

Each instance processes one message at a time (`batchSize` 1). Flex Consumption can start one instance
per queued message, up to the app's maximum instance count. All workers share one Dataverse application
user. That user is limited by Dataverse to 6,000 requests and 20 minutes of execution in each 5-minute
window, and 52 concurrent requests. Beyond two or three countries at once, the extra workers wait on
HTTP 429 rather than finishing sooner. Cap the Function App at about three instances so the remaining
countries stay in the queue. Waiting does not drop changes: the delta window is based on what was last
read, not on the clock time the message was created.

### 3.5 What the worker runs

`scheduled_onekey_worker` calls the same engine as the command-line sync.

**Full message**

1. If another live job is already syncing this codebase, defer. Azure shows the message again after 30
   minutes.
2. Run phases 0–6 for the whole contracted population. Phase 0 skips choice options when `choices` is
   false, and still loads this country's lookup tables.
3. Mark pending failures resolved when the same OneKey id was written successfully.

**Delta message**

1. If another live job is already syncing this codebase, skip and complete the message. The next
   midnight delta starts from the last window that was actually covered, so the skipped run loses
   nothing.
2. Replay up to `retry_limit` pending `ts_integrationfailure` rows for this country. Matching is
   by OneKey id prefix, so a WDE worker does not claim WUK rows.
3. Compute the window (section 3.6) after the retry, so a long retry does not open a gap.
4. Run phases 0–6 for entities OneKey reports as changed in that window.
5. Mark pending failures resolved when this delta wrote those ids.

A full sync does not replay the failure table first. It rewrites the population, and successful ids are resolved
at the end.

### 3.6 The delta window

The watermark is `ts_integrationjob`, not a file.

The next delta starts at the latest "stopped reading" time among Succeeded or Partially Succeeded Full and
Delta jobs for that country, minus 15 minutes of overlap, and ends at the moment the worker begins the
delta.

A job stopped reading at the earlier of `ts_watermarkto` and `ts_startedon`. A full sync has no
watermark end; its start time is used. Jobs that stopped early, lost a country, or are retries of a previous job
are not used as the watermark. A manual replay of an old window does not move the next window.

If no covering job exists, the delta starts at 00:00 UTC yesterday.

The overlap is intentional. A row changed near the boundary is read twice and upserted again, which is safe
because the write matches on `ts_externalid`.

OneKey will not return a failed id again unless that id changes. The retry step before the delta is what picks
those ids up.

### 3.7 Retries and the poison queue

| Setting | Value | Effect |
| --- | --- | --- |
| `visibilityTimeout` | 30 minutes | How long a failed message stays hidden before Azure retries it. A running message stays hidden for the whole run |
| `maxDequeueCount` | 5 | Deliveries before Azure moves a delta message to `onekey-queue-poison` |
| `maxPollingInterval` | 30 seconds | Longest wait before a new message is noticed |

| Situation | Result |
| --- | --- |
| Message is not valid JSON, has an unknown field, or has a bad version | Copied to `onekey-queue-poison` immediately. Retrying cannot repair it |
| Delta fails | Exception. Azure retries after 30 minutes, up to 5 deliveries, then poison |
| Full sync fails | Retried once. On the second failed delivery it goes to the poison queue, because every retry restarts the population from the first phase |
| Full sync finds the country busy | Deferred and retried after 30 minutes. A busy delivery counts as a delivery |
| Delta finds the country busy | Message completed as skipped |

A poison message keeps its original JSON. Fix the cause, then put the same JSON back on `onekey-queue`
(Base64-encoded) to run that country again. Deleting a delta poison message drops that attempt only. The
next scheduled delta covers the window, because the watermark does not advance on a failed job.

### 3.8 Enable and operate

**Dataverse address settings.** Before the first load into an environment, turn on two features in the
Power Platform admin center: **Manage › Environments ›** *the environment* **› Settings › Product ›
Features**. Changing them needs an environment administrator.

| Feature | Why it is needed |
| --- | --- |
| **Disable empty address record creation** | Stops new blank address rows (accounts and contacts only). Without it, Dataverse creates two empty address rows with every account and three with every contact. The sync fills the account's rows 1 and 2 with its OneKey addresses either way (section 1.6); an account with a single address keeps an empty row 2, and contacts keep their empty rows. |
| **Enable deletion of address records** | The clean-up needs it: address rows 1 and 2 of an account or contact cannot be deleted otherwise. When the sync no longer needs a row it deletes it; if Dataverse refuses, the row is emptied instead and stays. |

Turning on **Disable empty address record creation** does not remove blank rows that already exist. In an
environment that was loaded before it was on, delete those rows once, for example with a Dataverse bulk
delete job, after **Enable deletion of address records** is on.

Leave `SYNC_SCHEDULE` false until the four functions are visible on the app: `sample_ok_to_dv`,
`scheduled_onekey_delta`, `scheduled_onekey_full`, `scheduled_onekey_worker`. Then set
`SYNC_SCHEDULE` to true.

Confirm a night run in Application Insights or the log stream:

| Log event | Meaning |
| --- | --- |
| `scheduled_delta_enqueued` or `scheduled_full_enqueued` | The timer placed the messages |
| `scheduled_queue_complete` | The worker finished that country |
| `scheduled_queue_skipped` | A delta found the country already running and left the window for the next delta |
| `scheduled_queue_poisoned` | The message was moved to `onekey-queue-poison` |

The same run writes `ts_integrationjob`. Row-level errors are in `ts_integrationfailure`.

To run one country by hand, add a Base64 message to `onekey-queue`. In the portal: Storage account,
Queues, `onekey-queue`, Add message, and encode the body in Base64. Example body:

```json
{"version":1,"mode":"delta","codebase":"WDE","date_to":null,"retry_limit":100,"choices":true}
```

Setup commands, plan choice, and the full app-setting list are in `docs/AZURE_FUNCTION_SETUP.md`.

### 3.9 End-to-end sequence

For a normal day that is not the 1st, with `ONEKEY_CODBASE=WUK,WDE`:

1. At 00:00 UTC the delta timer checks `SYNC_SCHEDULE`.
2. It upserts the shared choice options once.
3. It enqueues two messages, `choices` false, one for WUK and one for WDE.
4. A worker takes WUK. No other WUK job has a fresh heartbeat, so it continues.
5. It replays pending WUK failures, then asks Dataverse for the last covering WUK job and builds the
   window.
6. It calls `/pop/refreshMyOnekey` for WUK with that window and walks every packet.
7. It loads WUK lookup tables, then accounts, addresses, hierarchy, HCO affiliations, contacts, HCP
   affiliations, and roles.
8. It updates `ts_integrationjob` and resolves failure rows whose ids succeeded.
9. Azure deletes the WUK message. The WDE message runs the same way, on its own window and its
   own failure rows.
10. On the 1st, the full timer enqueues mode `full` instead. Each worker loads the whole contracted
    population for its country. The delta on the 2nd starts from that full run.

## 4. Failure and retry

Failures are handled at two levels. A bad record is stored and replayed on its own. A country job that cannot
finish is retried as a queue message. One does not replace the other: a single rejected account must not
cause the rest of the country to run again, and a worker that dies mid-run must not leave that country
unprocessed.

### 4.1 Record failures

Each failed source record becomes one row in `ts_integrationfailure`, linked to the
`ts_integrationjob` that produced it. The row is written during the phase, not only at the end of the job.

The same failure inside one job is updated rather than duplicated. The key is
`{job id}|{phase}|{OneKey id}|{operation}`.

| Column | What it holds |
| --- | --- |
| `ts_sourceid` | OneKey id: workplace, individual, or activity |
| `ts_externalid` | Dataverse key when it differs from the source id, such as `{workplaceEid} - {addressEid}` |
| `ts_phase` | Picklists, HCO, Addresses, Hierarchy, HCP, or HCP Activity. HCO–HCO relation failures use HCO |
| `ts_entitytype` | Account, Contact, Customer Address, or Affiliation |
| `ts_operation` | Validate, Create, Update, Upsert, or Relate |
| `ts_errorcategory` | Validation, Mapping, Lookup, Timeout, or Unknown. Dataverse HTTP errors use Unknown and store the status on `ts_httpstatus` |
| `ts_errormessage` | Short message, at most 2,000 characters. No payload and no stack trace |
| `ts_isretryable` | Whether a later retry may replay this id |
| `ts_retrystatus` | Pending, Retried, Resolved, or Ignored |
| `ts_attemptcount` | How many times this row has been tried. The first failure counts as 1 |

A validation error, a mapping error, or a lookup that cannot be resolved is stored with `ts_isretryable` =
No. Replaying it would fail the same way until the data or the mapping changes. A timeout, HTTP 429, or
HTTP 500, 502, 503, or 504 is stored as retryable. Anything else is not retried unless the phase that caught
it says so.

The rest of the phase continues. A job with some failed rows and some successful rows finishes as Partially
Succeeded. The watermark still advances when the job covered its window, so the next delta does not
re-read the whole country just because a handful of rows failed.

OneKey will not return an unchanged id on a later delta. A record that failed and then stays unchanged in
OneKey would be lost if the only recovery were the delta window. The failure row is what brings it back.

### 4.2 When pending rows are replayed

The daily delta worker replays this country's pending rows before it opens the delta window. The message
field `retry_limit` caps that replay. The default is 100, oldest first. A full sync does not replay the table
first. It rewrites the population, and ids it writes successfully are resolved at the end.

A row is selected only when all of the following are true:

- `ts_retrystatus` is Pending, or it was left at Retried by a run that stopped and the claim is more
  than two hours old.
- `ts_isretryable` is Yes.
- `ts_attemptcount` is below 5.
- The parent job is not still Running, or its heartbeat is more than an hour old. A killed job stays
  Running, but its failures must not wait forever.

Selection is by the OneKey id prefix (WDE…, WUK…), so two country workers never claim the same row. Rows
are grouped by the job that first recorded them, and replayed in phase order: accounts, addresses,
hierarchy, HCO affiliations, contacts, then HCP affiliations.

### 4.3 What one retry does

```
    Pending row
        │
        ├─ no step replays its phase and table ──▶ Ignored
        ▼
    Mark Retried (claim)
        │
        ▼
    Read that id from OneKey by id
        │
        ├─ not found ──────────────▶ Ignored
        │
        ▼
    Run the same transform and upsert as the original phase
        │
        ├─ written, no new error ──▶ Resolved
        ├─ failed, attempts left ──▶ Pending (attempt + 1)
        └─ failed, 5 attempts ─────▶ Ignored
```

The retry reads the current OneKey record by id and sends it through the normal mapping. It does not reuse
the payload from the night it failed. A retry job is written to `ts_integrationjob` with a link back to the
parent job (`ts_retryofjob`) and "Retry" in its name (`WUK Retry Full …`), so it is recognised even when
the parent could not be looked up. That job is not used as a delta watermark, because it only replays
listed ids. A row that fails again is an outcome, not a failed run: only an error that stops a step
fails the retry and its queue message. A role failure is recorded against its activity, so the replay
rewrites the activity with its roles.

While the row is in progress its status is Retried, so a second worker does not take it. If the worker is killed
before it records the outcome, the claim expires after two hours and the next run takes the row back. Each
expired claim counts as an attempt. Five attempts that stop before an outcome is recorded set the row to
Ignored.

Resolved means the upsert succeeded. The original failure row is kept. A retry does not insert a second
queue row for the same failure.

Ignored means the integration will not replay it again. Typical causes are a record OneKey no longer returns,
a non-retryable error on replay, or five unsuccessful attempts. Someone reviews Ignored rows, fixes the
cause, and either waits for a later full sync or puts the id back to Pending.

### 4.4 Resolved by the sync itself

A later full or delta run also closes pending rows. After the job succeeds, every OneKey id that was upserted
in that run is matched to pending failure rows, excluding rows that belong to the job that just finished. Those
rows are set to Resolved. A record that failed yesterday and changed again today is therefore fixed by the
delta even if it was not in the 100-row retry batch.

### 4.5 Failed country messages

Section 3.7 is the queue behaviour. In short:

| Situation | What happens |
| --- | --- |
| One record fails inside a job | The job continues. The id is a failure row and is eligible for section 4.2 |
| The worker raises (OneKey down, Dataverse down, an uncaught error) | Azure hides the message for 30 minutes and delivers it again. A delta is tried up to 5 times. A full sync is tried twice, because each attempt restarts from the first phase |
| The message itself is invalid | It goes to `onekey-queue-poison` immediately |
| A delta finds the country already running | The message is completed and skipped. The next delta starts from the last covered window |
| A full sync finds the country already running | The message is deferred and retried after 30 minutes |

A failed job does not move the watermark. The next successful delta reads from the last job that covered its
window, so the gap is read again. Deleting a poison delta message drops that attempt only. The following
scheduled delta covers the same window.

Record retry and message retry solve different problems. Message retry repeats the country. Record retry
repeats the ids that the country already reported as failed.
