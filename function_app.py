"""Azure Function App - OneKey to Dataverse sample and the scheduled syncs.

sample_ok_to_dv upserts a few OneKey HCOs and their related data to Dataverse,
through the same engine phases as the full sync. It is a small end-to-end
check: a failure there is a connectivity, configuration or mapping problem.

scheduled_onekey_full (monthly) and scheduled_onekey_delta (daily) sync the
choice options once, then enqueue one work item per codebase: full syncs on
onekey-full, and retries on onekey-retry. Each queue has its own worker:
scheduled_onekey_full_worker runs the full sync, scheduled_onekey_retry_worker
replays pending failures and then queues the codebase on onekey-delta, and
scheduled_onekey_delta_worker pulls the delta. Azure retries failures before
moving them to <queue>-poison. Sync runs are traced to ts_integrationjob /
ts_integrationfailure. See integration.queue_sync for what each delivery does.

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
    SYNC_DELTA_FROM  UTC start for every delta (2026-08-01), instead of where
                     the last job stopped reading; remove once covered
    SYNC_SKIP_UNCHANGED  false writes records that already match (default true)
    SYNC_LOG_DIR     directory for sync log files and reports (default off)
"""

import logging
import os
import typing

import azure.functions as func

from integration import load_env
from onekey import OneKeyError

app = func.FunctionApp()

# Binding annotations must use typing generics: the Python worker rejects a
# built-in generic such as func.Out[list[str]] ("invalid non-type annotation"),
# and one function it cannot load stops every function from loading.

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
    """processed/created/updated/skipped/failed for one phase of a sync result."""
    stats = result.get(key) or {}
    return "/".join(
        str(stats.get(name, 0)) for name in ("processed", "created", "updated", "skipped", "failed")
    )


# Noon on the 1st, not midnight: the monthly full syncs are enqueued at 00:00
# and the sample would otherwise start in the same second, on the same records.
@app.timer_trigger(schedule="0 0 12 1 * *", arg_name="timer", run_on_startup=False)
def sample_ok_to_dv(timer: func.TimerRequest) -> None:
    """Monthly (or on demand from Test/Run): upsert a few OneKey HCOs and their related data.

    Per HCO: account, addresses, parent link, HCO-HCO affiliations, and the
    HCPs whose main activity is there with their HCP-HCO affiliations. See
    integration.sample_sync.sync_sample. Skipped while a sync of the same
    codebase is running.
    """
    if timer.past_due:
        logging.warning("sample_ok_to_dv_timer_past_due")

    from integration.sample_sync import sync_sample
    from integration.scheduled_sync import active_job, engine_from_env

    cod_base = os.environ.get("ONEKEY_CODBASE", "WUK").split(",")[0].strip().upper()
    limit = int(os.environ.get("ONEKEY_SAMPLE", "5"))
    logging.info("sample_ok_to_dv_start codbase=%s hcos=%d", cod_base, limit)

    try:
        with engine_from_env() as engine:
            running = active_job(engine.dataverse, [cod_base])
            if running:
                logging.warning(
                    "sample_ok_to_dv_skipped codbase=%s busy_job=%s",
                    cod_base,
                    running.get("ts_jobid"),
                )
                return
            # Verify the OneKey credentials up front so bad secrets surface as
            # an auth error rather than as an empty search.
            engine.onekey.session.test_connection()
            result = sync_sample(engine, codebase=cod_base, hco_limit=limit)

        logging.info(
            "sample_ok_to_dv_complete success=%s job=%s status=%s "
            "processed/created/updated/skipped/failed accounts=%s addresses=%s "
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
    # A codebase listed twice would run twice in parallel on the same records.
    return list(
        dict.fromkeys(
            part.strip().upper()
            for part in os.environ.get("ONEKEY_CODBASE", "WUK").split(",")
            if part.strip()
        )
    )


def _enqueue(
    job: str, mode: str, codebases: list[str], work: func.Out[typing.List[str]]
) -> None:
    """Sync the choice options once, then put one message per codebase on the mode's queue."""
    from integration.queue_sync import QUEUES, scheduled_messages
    from integration.scheduled_sync import sync_choice_picklists

    choices_synced = sync_choice_picklists()
    messages = scheduled_messages(mode, codebases, choices=not choices_synced)
    work.set(messages)
    logging.info(
        "scheduled_%s_enqueued queue=%s codebases=%s messages=%d choices_synced=%s",
        job,
        QUEUES[mode],
        ",".join(codebases),
        len(messages),
        choices_synced,
    )


# Schedules are NCRONTAB in UTC. Flex Consumption does not support the
# WEBSITE_TIME_ZONE or TZ settings, so 00:00 is always midnight UTC. The delta
# skips the 1st so it never overlaps the full sync, and the delta on the 2nd
# starts from the full sync's ts_startedon.


@app.queue_output(
    arg_name="work",
    queue_name="onekey-retry",
    connection="AzureWebJobsStorage",
)
@app.timer_trigger(schedule="0 0 0 2-31 * *", arg_name="timer", run_on_startup=False)
def scheduled_onekey_delta(
    timer: func.TimerRequest, work: func.Out[typing.List[str]]
) -> None:
    """Daily at midnight, except the 1st: enqueue one retry per codebase.

    The retry worker queues the codebase's delta when its retry is over. Each
    delta's window runs from where the last covering Full or Delta
    ts_integrationjob of its codebase stopped reading, up to the moment the
    delta worker runs. Stays idle unless App Setting SYNC_SCHEDULE is true.
    """
    codebases = _scheduled_codebases("delta", timer)
    if codebases is None:
        return
    _enqueue("delta", "retry", codebases, work)


@app.queue_output(
    arg_name="work",
    queue_name="onekey-full",
    connection="AzureWebJobsStorage",
)
@app.timer_trigger(schedule="0 0 0 1 * *", arg_name="timer", run_on_startup=False)
def scheduled_onekey_full(
    timer: func.TimerRequest, work: func.Out[typing.List[str]]
) -> None:
    """Midnight on the 1st: enqueue one full-sync item per codebase.

    Stays idle unless App Setting SYNC_SCHEDULE is true.
    """
    codebases = _scheduled_codebases("full", timer)
    if codebases is None:
        return
    _enqueue("full", "full", codebases, work)


# Each worker's poison output is its trigger queue's name plus "-poison": that
# is where Azure itself moves a message after maxDequeueCount deliveries, so
# each queue keeps one place for work that will not be retried.


def _work(
    mode: str,
    message: func.QueueMessage,
    poison: func.Out[str],
    delta: typing.Optional[func.Out[str]] = None,
) -> None:
    """Run one delivery from the mode's queue; an exception lets Azure retry then poison it.

    Work that retrying cannot fix (a malformed message, a message for another
    queue, work that failed on its last allowed delivery) goes to the poison
    queue straight away. A retry that is over, done or poisoned, also puts its
    codebase on ``delta``.
    """
    from integration.queue_sync import (
        OUTCOME_POISON,
        OUTCOME_SKIPPED,
        QUEUES,
        handle_message,
        job_ids,
        ran_mode,
    )

    queue = QUEUES[mode]
    body = message.get_body()
    logging.info(
        "scheduled_queue_start queue=%s id=%s dequeue_count=%s",
        queue,
        message.id,
        message.dequeue_count,
    )
    outcome = handle_message(body, int(message.dequeue_count or 1), mode=mode)
    item = outcome.item
    if outcome.next_delta:
        delta.set(outcome.next_delta)
        logging.info(
            "scheduled_queue_chained queue=%s id=%s codebase=%s next=%s",
            queue,
            message.id,
            item.codebase,
            QUEUES["delta"],
        )
    if outcome.status == OUTCOME_POISON:
        poison.set(body.decode("utf-8", errors="replace"))
        logging.error(
            "scheduled_queue_poisoned queue=%s id=%s mode=%s codebase=%s reason=%s",
            queue,
            message.id,
            item.mode if item else None,
            item.codebase if item else None,
            outcome.reason,
        )
        return
    ran = ran_mode(item, outcome.result)
    detail = (outcome.result or {}).get(ran) or {}
    logging.info(
        "scheduled_queue_%s queue=%s id=%s mode=%s ran=%s codebase=%s job=%s status=%s "
        "busy_job=%s",
        "skipped" if outcome.status == OUTCOME_SKIPPED else "complete",
        queue,
        message.id,
        item.mode,
        ran,
        item.codebase,
        job_ids(detail),
        detail.get("trace_status"),
        (outcome.result or {}).get("busy_job"),
    )


@app.queue_output(
    arg_name="poison",
    queue_name="onekey-full-poison",
    connection="AzureWebJobsStorage",
)
@app.queue_trigger(
    arg_name="message",
    queue_name="onekey-full",
    connection="AzureWebJobsStorage",
)
def scheduled_onekey_full_worker(message: func.QueueMessage, poison: func.Out[str]) -> None:
    """Run one codebase's full sync, with FULL_MAX_DELIVERIES deliveries."""
    _work("full", message, poison)


@app.queue_output(
    arg_name="delta",
    queue_name="onekey-delta",
    connection="AzureWebJobsStorage",
)
@app.queue_output(
    arg_name="poison",
    queue_name="onekey-retry-poison",
    connection="AzureWebJobsStorage",
)
@app.queue_trigger(
    arg_name="message",
    queue_name="onekey-retry",
    connection="AzureWebJobsStorage",
)
def scheduled_onekey_retry_worker(
    message: func.QueueMessage, poison: func.Out[str], delta: func.Out[str]
) -> None:
    """Replay one codebase's pending failures, then queue its delta.

    The delta is queued only once the retry is over (done, or poisoned after
    RETRY_MAX_DELIVERIES), so the two never run at the same time. A codebase
    found busy gets neither; the next day's retry and delta cover it.
    """
    _work("retry", message, poison, delta)


@app.queue_output(
    arg_name="poison",
    queue_name="onekey-delta-poison",
    connection="AzureWebJobsStorage",
)
@app.queue_trigger(
    arg_name="message",
    queue_name="onekey-delta",
    connection="AzureWebJobsStorage",
)
def scheduled_onekey_delta_worker(message: func.QueueMessage, poison: func.Out[str]) -> None:
    """Pull one codebase's delta; Azure gives it maxDequeueCount deliveries."""
    _work("delta", message, poison)
