# Azure Function setup

This app is a Python v4 Function App. The timers do not run the country sync
themselves. They sync the choice options once, then enqueue one message per
codebase: full syncs on `onekey-full`, daily work on `onekey-retry`. Each queue
has its own worker; the retry worker replays the country's pending failures,
then queues its delta on `onekey-delta`. A failed message is retried, then
moved to that queue's poison queue (`onekey-full-poison`, `onekey-retry-poison`,
`onekey-delta-poison`).
How messages, workers and instances fit together is in
[QUEUE_ARCHITECTURE.md](QUEUE_ARCHITECTURE.md).

`host.json` sets `functionTimeout` to 12 hours, because one country's full
sync can run for hours. Use **Flex Consumption** or **Premium**, where the
maximum is unbounded. The classic Consumption plan stops a run at 10 minutes.
Python 3.13 and 3.14 are also unavailable on classic Linux Consumption.

The timeout is not a guarantee. When Azure removes an instance (scale-in), a
running sync gets 60 minutes to finish, and 10 minutes during a platform
update; a deployment restarts the app. The message then comes back and the
country runs again (see [Retries and overlap](#retries-and-overlap)).

## What gets deployed

| Function | Trigger | Schedule (UTC) | What it does |
| --- | --- | --- | --- |
| `sample_ok_to_dv` | Timer | 12:00 on the 1st | Upserts a few HCOs. Runs even when `SYNC_SCHEDULE` is off, but skips while its codebase is syncing. |
| `scheduled_onekey_delta` | Timer | 00:00 on days 2–31 | Syncs the choice options, then enqueues one retry message per codebase on `onekey-retry`. |
| `scheduled_onekey_full` | Timer | 00:00 on the 1st | Syncs the choice options, then enqueues one full-sync message per codebase on `onekey-full`. |
| `scheduled_onekey_full_worker` | Queue `onekey-full` | — | Runs that codebase's full sync. |
| `scheduled_onekey_retry_worker` | Queue `onekey-retry` | — | Replays that codebase's pending failures, then puts its delta on `onekey-delta`. |
| `scheduled_onekey_delta_worker` | Queue `onekey-delta` | — | Runs that codebase's delta. |

Schedules are NCRONTAB in UTC. Flex Consumption does not support the
`WEBSITE_TIME_ZONE` or `TZ` settings, so 00:00 is always midnight UTC.

A scheduled retry message looks like:

```json
{"choices":false,"codebase":"WUK","date_to":null,"mode":"retry","retry_limit":100,"version":1}
```

| Field | Meaning |
| --- | --- |
| `mode` | `full`, `retry` or `delta`, matching the queue. May be left out; the queue supplies it. A message on another mode's queue goes to the poison queue. |
| `codebase` | One OneKey territory. |
| `date_to` | Leave `null`. The delta reads changes up to the moment it runs. Set it only to replay an old window by hand (delta only). |
| `retry_limit` | Pending failures one retry replays. |
| `choices` | `false` when the timer already synced the choice options. Defaults to `true`. The retry passes it on to the delta it queues. |
| `run_id`, `enqueued_at` | Set by the timer: the run the message belongs to (e.g. `delta-20261012T000001Z-7`, ending in the number of countries queued) and when it was queued (UTC). The retry passes them on to its delta. Leave them out of manual messages. `scripts/sync_run_report.py` uses them to report how long a run took (see [QUEUE_ARCHITECTURE.md](QUEUE_ARCHITECTURE.md#how-long-a-run-took)). |

Any other field is logged as unknown and ignored.

### The delta window

Each delta starts where the last Full or Delta job of the same codebase that
covered its whole window stopped reading, less 6 hours of overlap. A job
stopped reading at the earlier of `ts_watermarkto` (its window end) and
`ts_startedon`; the latest such end among recent jobs wins. A message that waited
in the queue, or failed and came back, therefore leaves no gap: the next delta
starts from the end it actually read, not from when it ran. A manual replay of
an old window does not move the next window either.

A country with no such job yet (newly added to `ONEKEY_CODBASE`, or whose
earlier runs all failed) gets a **full sync instead of its delta**, so its
first run loads the whole population rather than only recent changes. The log
says `running a full sync instead of a delta`, the job row is typed Full, and
the worker logs `mode=delta ran=full`. Like any full sync it gets two
deliveries before the poison queue. Setting `SYNC_DELTA_FROM` (below) skips
this and runs the delta from that date.

To start from a fixed moment instead, set App Setting `SYNC_DELTA_FROM` (UTC,
`2026-08-01` or `2026-08-01T00:00:00Z`). Every delta, for every country, then
reads from that moment to now, and logs a warning that it did. Remove the
setting once that window has been read, or each night re-reads everything since
then. A value that is not a date, or that lies in the future, is logged as an
error and ignored. Changing an App Setting restarts the app, which stops any
sync that is running.

## What one job runs

The workers call the same engine as the CLI. One message is one country, and
a full or delta run does the whole flow for that country:

1. Picklists: that country's code-list lookup tables. The choice options do
   not depend on the country, so the timer syncs them once before it
   enqueues. If that fails, the messages say `"choices": true` and each
   worker syncs them itself.
2. HCO accounts
3. HCO addresses
4. HCO parent hierarchy
5. HCO–HCO relationships (`ts_affiliation`)
6. HCP contacts
7. HCP–HCO activity relationships (`ts_affiliation`)

A `full` message loads the population. A `retry` message replays that
country's pending `ts_integrationfailure` rows (matched by the OneKey ID
prefix, so parallel workers never claim the same row), then queues the
country's `delta`, which runs the same phases for records changed since the
last completed job. The retry and the delta never run at the same time, so the
delta is not skipped as busy. `sample_ok_to_dv` is the small connectivity
check. It is not this flow.

## Which countries run

There is no country list in the code. Set App Setting `ONEKEY_CODBASE` to one
OneKey territory or to several separated by commas. The timer reads that value
when it fires and enqueues one message per country. `WUK` is used only when
the setting is empty.

| `ONEKEY_CODBASE` | Messages on the 1st | Messages on other days |
| --- | --- | --- |
| `WUK` | 1 full | 1 retry, then 1 delta |
| `WUK,WDE,WFR` | 3 full, one each | 3 retry, then 3 delta, one each |

Change the setting and the next timer tick uses the new list. No redeploy.
A code listed twice is enqueued once. Each message is one country, so a
failure retries only that country.

### How many countries run at once

Flex Consumption starts an instance per queued message, up to the app's
maximum instance count (100 by default) for each worker, so every country
starts at midnight.
All workers write to Dataverse as one application user, and Dataverse limits
each user to 6,000 requests and 20 minutes of execution per 5 minutes, and 52
requests at a time. Beyond two or three workers you get throttling (429) waits,
not speed. Cap it:

```bash
az functionapp scale config set \
  --name fa-onekey-paion \
  --resource-group rg-onekey \
  --maximum-instance-count 3
```

The cap applies to each function group, and each worker is its own group: up
to 3 full syncs, 3 retries and 3 deltas can run at once. The timers and the
sample are capped at 3 as well, which they never reach. The remaining
countries wait in their queue; the delta window still starts where the
previous delta stopped reading, so waiting loses nothing.

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

`az functionapp create` sets `AzureWebJobsStorage`. All the queues use that
same storage account.

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
for q in onekey-full onekey-retry onekey-delta; do
  az storage queue create --name $q --account-name stonekeypaion
  az storage queue create --name $q-poison --account-name stonekeypaion
done
```

The output bindings create them on first use, so this step only makes them
visible before the first run. Azure always poisons a message into
`<queue>-poison`, so the poison queues keep that name.

### Retries and overlap

| `host.json` `extensions.queues` | Value | Effect |
| --- | --- | --- |
| `batchSize`, `newBatchThreshold` | 1, 0 | Each instance runs one country at a time. |
| `visibilityTimeout` | 30 min | How long a **failed** message waits before it is retried. It is not a run limit: the host keeps a running message hidden for as long as the run takes, whatever this value. Keep it short; raising it with `functionTimeout` would delay every retry by hours. |
| `maxDequeueCount` | 5 | Deliveries before Azure moves a message to its queue's poison queue. Applies to all three queues. |
| `maxPollingInterval` | 30 s | Longest wait before a new message is noticed. |

| Situation | What the worker does |
| --- | --- |
| Malformed message, or a `mode` that belongs to another queue | Writes it to the queue's poison queue at once. Retrying cannot fix it. |
| Delta fails | Raises; Azure retries after 30 minutes, up to 5 deliveries. |
| Retry fails | Retries once, and the delta waits. On the second failed delivery it goes to `onekey-retry-poison` and the delta is queued anyway; the failure rows stay Pending for the next retry. |
| Full sync fails | Retries once. On the second failed delivery it goes to `onekey-full-poison`, because every retry restarts hours of work. A busy delivery (below) counts as a delivery. |
| Another live job is syncing the same codebase | Retry or delta: skipped and completed, and a skipped retry queues no delta, since the next delta starts where the last covering job stopped. Full: deferred; Azure retries after 30 minutes. |

A job counts as live while its `ts_integrationjob` row is Running and its
`ts_lastheartbeat` is under 20 minutes old. A running job refreshes the
heartbeat at most every 5 minutes, after any OneKey or Dataverse request and
during OneKey retry waits, so a long phase stays live. Dataverse retry waits
are capped at 5 minutes, so even a throttled request goes no more than about
18 minutes between heartbeats. A run killed by a restart stays Running, but its
heartbeat stops, so it blocks nothing after 20 minutes. Its failure rows become
retryable after an hour.

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
| `SYNC_SKIP_UNCHANGED` | no | Default `true`: a record that already holds every value to write is not updated (counted as skipped, `ts_skipped` on the job). `false` writes every record, makes a full sync of a large country take hours, and stops a paused run from continuing. |
| `SYNC_TIME_BUDGET_MINUTES` | no | Default `45`. A queued full sync or delta still running after this long stops between batches and queues its next part, before Flex Consumption ends the execution 60 minutes into a scale-in. `0` turns it off. See [Queue architecture](QUEUE_ARCHITECTURE.md#time-budget-and-continuations). |
| `SYNC_DELTA_FROM` | no | UTC start for every delta, e.g. `2026-08-01`. Unset: start where the last job stopped reading. See [The delta window](#the-delta-window). |
| `SYNC_LOG_DIR` | no | Leave unset. The app forces `off` because the package folder is read-only. |

Optional OneKey and Dataverse tunables (`ONEKEY_TIMEOUT`, `ONEKEY_PACKET_DELAY`,
`ONEKEY_PAGE_SIZE`, `DATAVERSE_BULK_BATCH_SIZE`, and the rest) use the same
names as `.env.example`.

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
`scheduled_onekey_full_worker`, `scheduled_onekey_retry_worker` and
`scheduled_onekey_delta_worker`. Then:

```bash
az functionapp config appsettings set \
  --name fa-onekey-paion \
  --resource-group rg-onekey \
  --settings SYNC_SCHEDULE=true
```

Confirm a run in Application Insights, or in the Function App's log stream.
A healthy daily run logs `scheduled_delta_enqueued queue=onekey-retry` from
the timer, then from the workers `scheduled_queue_complete queue=onekey-retry`,
`scheduled_queue_chained` (the retry queued the delta) and
`scheduled_queue_complete queue=onekey-delta` (`scheduled_queue_skipped` when
the codebase was busy, `scheduled_queue_poisoned` when the message went to the
poison queue). The same run writes `ts_integrationjob` and any row-level errors
to `ts_integrationfailure`.

## Running one country by hand

Put a message on the queue for the work you want: `onekey-full` for a full
sync, `onekey-retry` for retry then delta, or `onekey-delta` for the delta
alone. The queue sets the mode, so `{"codebase":"WUK"}` is enough. The
bindings use Base64, so encode the body; a plain-text message cannot be
decoded and lands in the poison queue.

```bash
CS=$(az functionapp config appsettings list --name fa-onekey-paion \
  --resource-group rg-onekey --query "[?name=='AzureWebJobsStorage'].value" -o tsv)
az storage message put --queue-name onekey-retry --connection-string "$CS" \
  --content "$(printf '%s' '{"codebase":"WUK"}' | base64)"
```

In the portal: Storage account → Queues → `onekey-retry` → **Add message**,
and tick **Encode the message body in Base64**.

## Poison messages

A message lands in `<queue>-poison` with its original body when it is
malformed or on the wrong queue, when a full sync or retry fails twice, or when
a delta fails on its fifth delivery. Read it, fix the cause, and submit the
same JSON back to the queue it came from (the poison queue's name without
`-poison`) to retry that codebase. Deleting it drops that work; for a retry or
delta that is safe, since the next day's retry and delta cover it.

```bash
for q in onekey-full-poison onekey-retry-poison onekey-delta-poison; do
  az storage message peek --queue-name $q --account-name stonekeypaion --num-messages 5
done
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
