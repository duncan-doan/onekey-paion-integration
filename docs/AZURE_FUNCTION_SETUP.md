# Azure Function setup

This app is a Python v4 Function App. The timers do not run the country sync
themselves. They enqueue one message per codebase on `onekey-queue`, and
`scheduled_onekey_worker` runs that message. A failed message is retried, then
moved to `onekey-queue-poison`.

`host.json` sets `functionTimeout` to 30 minutes. Use **Flex Consumption** or
**Premium**. The classic Consumption plan stops a run at 10 minutes, so it
cannot honor this timeout. Python 3.13 and 3.14 are also unavailable on classic
Linux Consumption.

## What gets deployed

| Function | Trigger | Schedule (UTC) | What it does |
| --- | --- | --- | --- |
| `sample_ok_to_dv` | Timer | 00:00 on the 1st | Upserts a few HCOs. Runs even when `SYNC_SCHEDULE` is off. |
| `scheduled_onekey_delta` | Timer | 00:00 on days 2–31 | Enqueues one delta message per codebase. |
| `scheduled_onekey_full` | Timer | 00:00 on the 1st | Enqueues one full-sync message per codebase. |
| `scheduled_onekey_worker` | Queue `onekey-queue` | — | Runs that one codebase. |

Schedules are NCRONTAB in UTC. Set `WEBSITE_TIME_ZONE` only if the business
day must follow another timezone.

A delta message looks like:

```json
{"codebase":"WUK","date_to":"2026-10-07T00:00:00Z","mode":"delta","retry_limit":100,"version":1}
```

`date_to` is captured when the timer enqueues the message, so a retry uses the
same window end. The window start still comes from the last completed
`ts_integrationjob`.

## What one job runs

`scheduled_onekey_worker` calls the same engine as the CLI. Nothing in the
queue path turns a phase off. One message is one country, and that run does
the whole flow for that country:

1. Picklists
2. HCO accounts
3. HCO addresses
4. HCO parent hierarchy
5. HCO–HCO relationships (`ts_affiliation`)
6. HCP contacts
7. HCP–HCO activity relationships (`ts_affiliation`)

A `full` message loads the population. A `delta` message first replays pending
`ts_integrationfailure` rows, then runs the same phases for records changed
since the last completed job. `sample_ok_to_dv` is the small connectivity
check. It is not this flow.

## Which countries run

There is no country list in the code. Set App Setting `ONEKEY_CODBASE` to one
OneKey territory or to several separated by commas. The timer reads that value
when it fires and enqueues one message per country. `WUK` is used only when
the setting is empty.

| `ONEKEY_CODBASE` | Messages on the 1st | Messages on other days |
| --- | --- | --- |
| `WUK` | 1 full | 1 delta |
| `WUK,WDE,WFR` | 3 full, one each | 3 delta, one each |

Change the setting and the next timer tick uses the new list. No redeploy.
The queue worker still runs one country at a time, so a failure retries only
that country.

## Prerequisites

- Azure CLI
- [Azure Functions Core Tools](https://learn.microsoft.com/azure/azure-functions/functions-run-local) v4
- Permission to create a resource group, a storage account, and a Function App
- OneKey and Dataverse credentials. The names match [`.env.example`](../.env.example). Do not deploy `.env`.

```bash
az login
```

Run every command below from `onekey-paion-integration`, not the parent folder.
Replace `rg-onekey`, `stonekeypaion`, and `fa-onekey-paion` with the names for
this environment. A storage account name is 3–24 lowercase letters and numbers.

## 1. Create the Function App

Flex Consumption, Python 3.14, Linux:

```bash
az group create --name rg-onekey --location westeurope

az storage account create \
  --name stonekeypaion \
  --resource-group rg-onekey \
  --location westeurope \
  --sku Standard_LRS

az functionapp create \
  --name fa-onekey-paion \
  --resource-group rg-onekey \
  --storage-account stonekeypaion \
  --flexconsumption-location westeurope \
  --runtime python \
  --runtime-version 3.14
```

`az functionapp create` sets `AzureWebJobsStorage`. Both queues use that same
storage account.

For a worker that stays warm, create a Linux Premium plan instead and pass it
to `az functionapp create --plan`:

```bash
az functionapp plan create \
  --name plan-onekey \
  --resource-group rg-onekey \
  --location westeurope \
  --sku EP1 \
  --is-linux
```

## 2. Create the queues

```bash
az storage queue create --name onekey-queue --account-name stonekeypaion
az storage queue create --name onekey-queue-poison --account-name stonekeypaion
```

`host.json` processes one message at a time (`batchSize` 1). After 5 failed
deliveries (`maxDequeueCount`), Azure moves the message to
`onekey-queue-poison`. The visibility timeout is 30 minutes, matching the
function timeout, so a running message is not handed to a second worker.

## 3. App settings

Leave `SYNC_SCHEDULE` false until the publish check below succeeds. The delta
and full timers stay idle until it is `true`, `1`, or `yes`.

```bash
az functionapp config appsettings set \
  --name fa-onekey-paion \
  --resource-group rg-onekey \
  --settings \
    ONEKEY_ENV=uat \
    ONEKEY_USERNAME="<username>" \
    ONEKEY_PASSWORD="<password>" \
    ONEKEY_CONNECTOR_ID="<connector-id>" \
    DATAVERSE_URL="https://<org>.api.crm4.dynamics.com" \
    DATAVERSE_TENANT_ID="<tenant-id>" \
    DATAVERSE_CLIENT_ID="<client-id>" \
    DATAVERSE_CLIENT_SECRET="<client-secret>" \
    ONEKEY_CODBASE=WUK \
    SYNC_SCHEDULE=false
```

| Setting | Required | Purpose |
| --- | --- | --- |
| `AzureWebJobsStorage` | yes | Set by `az functionapp create`. Queue connection. |
| `ONEKEY_ENV` or `ONEKEY_BASE_URL` | yes | `uat`, `prd`, `qa`, or a full base URL. The URL wins. |
| `ONEKEY_USERNAME`, `ONEKEY_PASSWORD`, `ONEKEY_CONNECTOR_ID` | yes | OneKey HTTP Basic credentials. |
| `DATAVERSE_URL`, `DATAVERSE_TENANT_ID`, `DATAVERSE_CLIENT_ID`, `DATAVERSE_CLIENT_SECRET` | yes | Dataverse app registration. |
| `SYNC_SCHEDULE` | for scheduled sync | `true` enables the delta and full timers. |
| `ONEKEY_CODBASE` | no | Comma-separated territories. Default `WUK`. The sample uses the first. |
| `ONEKEY_SAMPLE` | no | HCOs in `sample_ok_to_dv`. Default `5`. |
| `SYNC_LOG_DIR` | no | Leave unset. The app forces `off` because the package folder is read-only. |

Optional OneKey and Dataverse tunables (`ONEKEY_TIMEOUT`, `ONEKEY_PACKET_DELAY`,
`DATAVERSE_BULK_BATCH_SIZE`, and the rest) use the same names as `.env.example`.

## 4. Publish

Remote build installs `requirements.txt` on Linux. `.funcignore` keeps `.env`,
`scripts/`, `tests/`, and `docs/` out of the package. `config/*.csv` is included.

```bash
func azure functionapp publish fa-onekey-paion --python --build remote
```

The Azure Functions extension in Cursor or VS Code deploys the same folder.
`.vscode/settings.json` already sets the deploy root to `.` and turns on the
remote build.

## 5. Check, then enable the schedule

```bash
az functionapp function list \
  --name fa-onekey-paion \
  --resource-group rg-onekey \
  --query "[].name" -o tsv
```

Expect `sample_ok_to_dv`, `scheduled_onekey_delta`, `scheduled_onekey_full`,
and `scheduled_onekey_worker`. Then:

```bash
az functionapp config appsettings set \
  --name fa-onekey-paion \
  --resource-group rg-onekey \
  --settings SYNC_SCHEDULE=true
```

Confirm a run in Application Insights, or in the Function App's log stream.
A healthy delta logs `scheduled_delta_enqueued` from the timer and
`scheduled_queue_complete` from the worker. The same run writes
`ts_integrationjob` and any row-level errors to `ts_integrationfailure`.

## Poison messages

A message that still fails on the fifth delivery lands in `onekey-queue-poison`
with its original body. Read it, fix the cause, and submit the same JSON back
to `onekey-queue` to retry that codebase. Deleting it drops that work.

```bash
az storage message peek \
  --queue-name onekey-queue-poison \
  --account-name stonekeypaion \
  --num-messages 5
```

## Local run

```bash
cp .env.example .env
# fill in the secrets, then:
func start
```

`local.settings.json` is gitignored. For a local queue, set
`AzureWebJobsStorage` there to a storage connection string or to
`UseDevelopmentStorage=true` when Azurite is running. `load_env()` fills
OneKey and Dataverse settings from `.env` and does not overwrite variables
that are already set.
