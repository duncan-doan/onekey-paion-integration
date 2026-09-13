# Architecture

```
                  ┌─────────────┐        ┌──────────────┐
   IQVIA OneKey ─▶│   onekey/   │        │  dataverse/  │─▶ Dynamics 365
   (HTTP Basic)   │  API client │        │  API client  │   (OAuth2 token)
                  └──────┬──────┘        └──────▲───────┘
                         │                      │
                         │   ┌──────────────┐   │
                         └──▶│ integration/ │───┘
                             │ mapping+sync │
                             └──────────────┘
```

## Packages

| Package | Responsibility |
| --- | --- |
| `onekey/` | Talks to IQVIA OneKey. Knows nothing about Dynamics. |
| `dataverse/` | Talks to the Dataverse Web API. Knows nothing about OneKey. |
| `integration/` | The only place that knows about both. Field mapping and sync orchestration. |

The two API clients stay independent so either can be tested, reused or
replaced without touching the other. Everything that couples them lives in
`integration/`.

## Scripts

| Script | Step |
| --- | --- |
| `scripts/retrieve_onekey.py` | 1. Retrieve HCP/HCO from OneKey. |
| `scripts/dataverse_records.py` | 2. Read/write those records in Dataverse. |
| `scripts/run_full_sync.py` | 3. Both steps in one pass. |

Steps 1 and 2 chain through a JSON file, so data can be inspected or edited in
between. Step 3 is the same flow without the intermediate file.

## Two different auth models

| | OneKey | Dataverse |
| --- | --- | --- |
| Mechanism | HTTP Basic, per request | OAuth2 client credentials |
| Token | none exists | bearer, 1 hour, cached and renewed |
| Connector/tenant | `connector_id` in the URL path | `tenant_id` in the token request |

Don't assume one from the other — OneKey has no token endpoint at all.

## Identity

| Entity | Dataverse column | Holds |
| --- | --- | --- |
| HCO → `account` | `accountnumber` | OneKey `workplaceEid` |
| HCP → `contact` | `externaluseridentifier` | OneKey `individualEid` |

Both are matched before writing, so a re-run updates in place rather than
duplicating.
