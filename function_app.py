"""Azure Function App - OneKey to Dataverse sample and the scheduled syncs.

sample_ok_to_dv upserts a few OneKey HCOs and their related data to Dataverse,
through the same engine phases as the full sync. It is a small end-to-end
check: a failure there is a connectivity, configuration or mapping problem.

scheduled_onekey_delta (daily) and scheduled_onekey_full (monthly) enqueue
one work item per codebase. scheduled_onekey_worker runs the real sync and
lets Azure retry failures before moving them to onekey-queue-poison. Sync
runs are traced to ts_integrationjob / ts_integrationfailure.

App Settings required (the same names the CLI scripts read from .env):
    ONEKEY_ENV or ONEKEY_BASE_URL, ONEKEY_USERNAME, ONEKEY_PASSWORD,
    ONEKEY_CONNECTOR_ID, DATAVERSE_URL, DATAVERSE_TENANT_ID,
    DATAVERSE_CLIENT_ID, DATAVERSE_CLIENT_SECRET
Scheduled syncs also need:
    SYNC_SCHEDULE=true
    AzureWebJobsStorage
Optional:
    ONEKEY_CODBASE   comma-separated territories (default WUK); the
                     sample uses the first
    ONEKEY_SAMPLE    HCOs in the sample (default 5)
    SYNC_LOG_DIR     directory for sync log files and reports (default off)
"""

import logging
import os
from datetime import datetime, timezone

import azure.functions as func

from integration import load_env
from onekey import OneKeyError

app = func.FunctionApp()

# In Azure the credentials arrive as App Settings, already in os.environ.
# load_env() returns {} when there is no .env file, and never overwrites a
# real environment variable, so this is a no-op in the cloud and a
# convenience when running the app locally with `func start`.
load_env()

# The app folder (/home/site/wwwroot) is read-only when running from a package,
# and the host already ships logs to Application Insights, so no local log files
# or reports. Set the App Setting SYNC_LOG_DIR to a writable path (e.g. /tmp/logs)
# to get them back.
os.environ.setdefault("SYNC_LOG_DIR", "off")


def _stats(result: dict, key: str) -> str:
    """processed/created/updated/failed for one phase of a sync result."""
    stats = result.get(key) or {}
    return "/".join(
        str(stats.get(name, 0)) for name in ("processed", "created", "updated", "failed")
    )


@app.timer_trigger(schedule="0 0 0 1 * *", arg_name="timer", run_on_startup=False)
def sample_ok_to_dv(timer: func.TimerRequest) -> None:
    """Every six hours: upsert a few OneKey HCOs and their related data.

    Per HCO: account, addresses, parent link, HCO-HCO affiliations, and the
    HCPs whose main activity is there with their HCP-HCO affiliations. See
    integration.sample_sync.sync_sample.
    """
    if timer.past_due:
        logging.warning("sample_ok_to_dv_timer_past_due")

    from integration.sample_sync import sync_sample
    from integration.scheduled_sync import engine_from_env

    cod_base = os.environ.get("ONEKEY_CODBASE", "WUK").split(",")[0].strip().upper()
    limit = int(os.environ.get("ONEKEY_SAMPLE", "5"))
    logging.info("sample_ok_to_dv_start codbase=%s hcos=%d", cod_base, limit)

    try:
        with engine_from_env() as engine:
            # Verify the OneKey credentials up front so bad secrets surface as
            # an auth error rather than as an empty search.
            engine.onekey.session.test_connection()
            result = sync_sample(engine, codebase=cod_base, hco_limit=limit)

        logging.info(
            "sample_ok_to_dv_complete success=%s job=%s status=%s "
            "processed/created/updated/failed accounts=%s addresses=%s "
            "affiliations=%s contacts=%s activity_affiliations=%s "
            "hierarchy_linked=%s errors=%s",
            result.get("success"),
            result.get("job_id"),
            result.get("trace_status"),
            _stats(result, "accounts"),
            _stats(result, "addresses"),
            _stats(result, "affiliations"),
            _stats(result, "contacts"),
            _stats(result, "activity_affiliations"),
            (result.get("hierarchy") or {}).get("linked", 0),
            result.get("errors"),
        )
    except OneKeyError:
        # OneKey UAT returns 503/500 during maintenance windows; log and let
        # the next tick retry rather than failing the whole invocation.
        logging.exception("sample_ok_to_dv_onekey_failed")
    except Exception:
        logging.exception("sample_ok_to_dv_unexpected_error")


def _scheduled_codebases(job: str, timer: func.TimerRequest) -> list[str] | None:
    """Return the codebases to sync, or None while SYNC_SCHEDULE is off."""
    enabled = os.environ.get("SYNC_SCHEDULE", "").strip().lower()
    if enabled not in {"1", "true", "yes"}:
        logging.info("scheduled_%s_skipped SYNC_SCHEDULE is not enabled", job)
        return None
    if timer.past_due:
        logging.warning("scheduled_%s_timer_past_due", job)
    return [
        part.strip().upper()
        for part in os.environ.get("ONEKEY_CODBASE", "WUK").split(",")
        if part.strip()
    ]


# Schedules are NCRONTAB in UTC unless the app sets WEBSITE_TIME_ZONE (Windows)
# or TZ (Linux). The delta skips the 1st so it never overlaps the full sync,
# and the delta on the 2nd starts from the full sync's ts_startedon.


@app.queue_output(
    arg_name="work",
    queue_name="onekey-queue",
    connection="AzureWebJobsStorage",
)
@app.timer_trigger(schedule="0 0 0 2-31 * *", arg_name="timer", run_on_startup=False)
def scheduled_onekey_delta(
    timer: func.TimerRequest, work: func.Out[list[str]]
) -> None:
    """Daily at midnight, except the 1st: enqueue retry-and-delta work.

    The delta window runs from ts_startedon of the last Full or Delta
    ts_integrationjob that covered its window, up to now. Stays idle unless
    App Setting SYNC_SCHEDULE is true.
    """
    codebases = _scheduled_codebases("delta", timer)
    if codebases is None:
        return

    from integration.queue_sync import scheduled_messages

    date_to = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    messages = scheduled_messages("delta", codebases, date_to=date_to)
    work.set(messages)
    logging.info(
        "scheduled_delta_enqueued codebases=%s messages=%d date_to=%s",
        ",".join(codebases),
        len(messages),
        date_to,
    )


@app.queue_output(
    arg_name="work",
    queue_name="onekey-queue",
    connection="AzureWebJobsStorage",
)
@app.timer_trigger(schedule="0 0 0 1 * *", arg_name="timer", run_on_startup=False)
def scheduled_onekey_full(
    timer: func.TimerRequest, work: func.Out[list[str]]
) -> None:
    """Midnight on the 1st: enqueue one full-sync item per codebase.

    Stays idle unless App Setting SYNC_SCHEDULE is true.
    """
    codebases = _scheduled_codebases("full", timer)
    if codebases is None:
        return

    from integration.queue_sync import scheduled_messages

    messages = scheduled_messages("full", codebases)
    work.set(messages)
    logging.info(
        "scheduled_full_enqueued codebases=%s messages=%d",
        ",".join(codebases),
        len(messages),
    )


@app.queue_trigger(
    arg_name="message",
    queue_name="onekey-queue",
    connection="AzureWebJobsStorage",
)
def scheduled_onekey_worker(message: func.QueueMessage) -> None:
    """Process one codebase; an exception lets Azure retry then poison it."""
    from integration.queue_sync import SyncQueueMessage, dispatch

    item = SyncQueueMessage.decode(message.get_body())
    logging.info(
        "scheduled_queue_start id=%s dequeue_count=%s mode=%s codebase=%s",
        message.id,
        message.dequeue_count,
        item.mode,
        item.codebase,
    )
    result = dispatch(item)
    detail = result.get(item.mode) or {}
    logging.info(
        "scheduled_queue_complete id=%s mode=%s codebase=%s job=%s status=%s",
        message.id,
        item.mode,
        item.codebase,
        detail.get("job_id"),
        detail.get("trace_status"),
    )
