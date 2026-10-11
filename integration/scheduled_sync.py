"""Scheduled OneKey jobs: the daily delta and the monthly full sync.

Daily delta:
1. Replay this codebase's pending retryable failure rows.
2. Pull the delta from where the last Full or Delta job that covered its
   whole window stopped reading, up to now.
3. Mark pending rows Resolved when that delta upserts the same source ID.

The CLI runs the three steps in one go (run_retry_then_delta). In Azure the
retry queue runs step 1 (retry_from_env) and then queues the delta, which runs
steps 2 and 3 (run_from_env with retry=False).

Monthly full: pull the whole population, then resolve pending rows the same
way. A completed full sync becomes the start of the next delta window.
A codebase with no completed Full or Delta job yet gets a full sync instead
of its first delta, unless the run is given a start (--date-from or
SYNC_DELTA_FROM).

The ts_integrationjob table is the watermark. A job stopped reading at the
earlier of its ts_watermarkto (the window end it was given) and ts_startedon;
the next window starts there, less WINDOW_OVERLAP. A job that stopped early
or lost a codebase is not used as the window start, so the next delta covers
its window again. A failed ID that OneKey does not change again will not
appear in a later delta, so step 1 is what retries it.

Queue workers run one codebase at a time and check active_job first. This is
a best-effort busy check; simultaneous starts still need an atomic lease.
"""

from __future__ import annotations

import logging
import os
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

from dataverse.client import escape_odata
from integration.env import load_env
from integration.job_trace import (
    HEARTBEAT_EVERY_SECONDS,
    JOB_DELTA,
    JOB_FULL,
    JOBS,
    STATUS_PARTIAL,
    STATUS_RUNNING,
    STATUS_SUCCEEDED,
    TRIGGER_TIMER,
    job_codebase,
)
from integration.retry_sync import resolve_pending_for_ids, retry_pending_failures

logger = logging.getLogger(__name__)

# Start each window this far before the previous window's end. OneKey stamps
# changes with its own clock and upserts are idempotent, so the overlap only
# re-reads a few records.
WINDOW_OVERLAP = timedelta(minutes=15)

# Recent Succeeded/Partially Succeeded jobs inspected for the window start.
_START_SCAN = 50

# A Running job whose heartbeat is newer than this is still alive. The tracer
# beats every HEARTBEAT_EVERY_SECONDS while requests flow, so four missed
# beats mean the run was killed and its Running status is stale.
ACTIVE_JOB_HEARTBEAT = timedelta(seconds=HEARTBEAT_EVERY_SECONDS * 4)

IF_BUSY_SKIP = "skip"
IF_BUSY_RAISE = "raise"

# App Setting / .env value that fixes where every delta starts, in UTC
# (2026-08-01 or 2026-08-01T00:00:00Z). While it is set, each delta reads from
# that moment to now, so remove it once the window it was set for is covered.
DELTA_FROM_SETTING = "SYNC_DELTA_FROM"


class SyncBusy(RuntimeError):
    """Another live job is already syncing this codebase."""

    def __init__(self, codebase: str, job: dict):
        self.codebase = codebase
        self.job = job
        super().__init__(
            f"Codebase {codebase} is busy: job {job.get('ts_jobid')} is Running "
            f"(heartbeat {job.get('ts_lastheartbeat')})"
        )


def _iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def covered_window(job: dict) -> bool:
    """True when a Succeeded or Partially Succeeded job processed its whole window.

    Partially Succeeded also marks a job that stopped early or lost a codebase,
    and those always leave ts_errorsummary set. Record failures alone do not.
    A picklist error does not stop the HCO/HCP phases, so it does not count.
    """
    summary = (job.get("ts_errorsummary") or "").strip()
    if not summary:
        return True
    return all(part.startswith("picklists:") for part in summary.split(" | "))


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        moment = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


def window_end(job: dict) -> datetime | None:
    """Where a covering job stopped reading OneKey changes.

    A delta reads up to ts_watermarkto, which is earlier than ts_startedon when
    the window end was fixed before the job started (a queued message, a
    manual replay). A full sync has no watermark and reads the population as
    of its start. The earlier of the two is safe either way.
    """
    started = _parse_iso(job.get("ts_startedon"))
    watermark = _parse_iso(job.get("ts_watermarkto"))
    if started and watermark:
        return min(started, watermark)
    return started or watermark


def last_window_start(dataverse, codebases: list[str]) -> str | None:
    """Return the latest window end among Full or Delta jobs that covered their window.

    The latest end, not the latest job: a manual replay of an old window
    started today must not move the next window back to that old end, nor
    forward to today. Retry jobs are excluded: a retry that spans several
    phases is typed Full, but it only replays failed IDs. They are the jobs
    with ts_retryofjob set.
    """
    jobs = dataverse.retrieve_multiple(
        JOBS,
        select=["ts_jobid", "ts_startedon", "ts_watermarkto", "ts_errorsummary"],
        filter=(
            f"ts_codebase eq '{escape_odata(job_codebase(codebases))}'"
            f" and (ts_jobtype eq {JOB_FULL} or ts_jobtype eq {JOB_DELTA})"
            f" and (ts_status eq {STATUS_SUCCEEDED} or ts_status eq {STATUS_PARTIAL})"
            " and _ts_retryofjob_value eq null"
        ),
        order_by="ts_startedon desc",
        top=_START_SCAN,
    )
    best_job, best_end = None, None
    for job in jobs:
        if not covered_window(job):
            continue
        end = window_end(job)
        if end and (best_end is None or end > best_end):
            best_job, best_end = job, end
    if best_end is None:
        return None
    logger.info(
        "Delta window starts from job %s (window end %s, started %s)",
        best_job.get("ts_jobid"),
        _iso(best_end),
        best_job.get("ts_startedon"),
    )
    return _iso(best_end)


def delta_window(
    dataverse,
    codebases: list[str],
    date_from: str | None = None,
    date_to: str | None = None,
) -> tuple[str, str]:
    """Return ISO date_from and date_to for the next delta.

    Explicit dates win, and a bare YYYY-MM-DD covers that whole day. Otherwise
    the window runs from where the last covering job stopped reading, less
    WINDOW_OVERLAP, to now. With no such job it starts at 00:00 UTC yesterday;
    run_retry_then_delta never gets here without a job, it runs a full sync.
    """
    now = datetime.now(timezone.utc)
    if date_to:
        iso_to = date_to if "T" in date_to else f"{date_to}T23:59:59Z"
    else:
        iso_to = _iso(now)
    if date_from:
        return (date_from if "T" in date_from else f"{date_from}T00:00:00Z"), iso_to

    started = last_window_start(dataverse, codebases)
    if started:
        start = datetime.fromisoformat(started.replace("Z", "+00:00"))
        return _iso(start - WINDOW_OVERLAP), iso_to

    fallback = (now - timedelta(days=1)).strftime("%Y-%m-%dT00:00:00Z")
    logger.warning(
        "No completed Full or Delta job for %s; delta starts at %s",
        job_codebase(codebases),
        fallback,
    )
    return fallback, iso_to


def active_job(
    dataverse,
    codebases: list[str],
    *,
    now: datetime | None = None,
) -> dict | None:
    """Return a live Running job for any of these codebases, or None.

    Live means its heartbeat is newer than ACTIVE_JOB_HEARTBEAT. A job killed
    by a timeout or a platform restart stays Running, but its heartbeat
    stops, so it no longer blocks the next run.
    """
    fresh = (now or datetime.now(timezone.utc)) - ACTIVE_JOB_HEARTBEAT
    for codebase in codebases:
        rows = dataverse.retrieve_multiple(
            JOBS,
            select=["ts_jobid", "ts_jobtype", "ts_startedon", "ts_lastheartbeat"],
            filter=(
                f"ts_codebase eq '{escape_odata(job_codebase([codebase]))}'"
                f" and ts_status eq {STATUS_RUNNING}"
                f" and ts_lastheartbeat ge {_iso(fresh)}"
            ),
            order_by="ts_lastheartbeat desc",
            top=1,
            max_records=1,
        )
        if rows:
            return {**rows[0], "codebase": codebase}
    return None


def _retry_each_codebase(
    engine,
    codebases: list[str],
    *,
    limit: int,
    dry_run: bool,
    trigger: int,
) -> dict:
    """Retry each codebase's own pending failures and merge the summaries.

    Scoping by codebase keeps parallel country workers from claiming and
    replaying the same rows.
    """
    merged = {
        "success": True,
        "selected": 0,
        "resolved": 0,
        "pending": 0,
        "ignored": 0,
        "jobs": [],
        "errors": [],
    }
    for codebase in dict.fromkeys(c.strip().upper() for c in codebases if c.strip()):
        summary = retry_pending_failures(
            engine,
            codebase=codebase,
            limit=limit,
            dry_run=dry_run,
            trigger=trigger,
        )
        for key in ("selected", "resolved", "pending", "ignored"):
            merged[key] += int(summary.get(key) or 0)
        merged["jobs"].extend(summary.get("jobs") or [])
        merged["errors"].extend(summary.get("errors") or [])
        merged["success"] = merged["success"] and bool(summary.get("success", True))
    return merged


def _delta_start(dataverse, codebases: list[str]) -> str | None:
    """Where the next delta starts, or None when no job has covered a window yet."""
    anchor = last_window_start(dataverse, codebases)
    if anchor is None:
        return None
    return _iso(_parse_iso(anchor) - WINDOW_OVERLAP)


def _promote_to_full(
    engine,
    *,
    codebases: list[str],
    dry_run: bool,
    trigger: int,
    sync_choices: bool,
) -> dict:
    """Run the full sync for a codebase that no Full or Delta job has read yet."""
    logger.warning(
        "No completed Full or Delta job for %s; running a full sync instead of a delta",
        job_codebase(codebases),
    )
    result = run_full(
        engine,
        codebases=codebases,
        dry_run=dry_run,
        trigger=trigger,
        sync_choices=sync_choices,
    )
    result["promoted_to_full"] = True
    return result


def run_retry(
    engine,
    *,
    codebases: list[str],
    dry_run: bool = False,
    trigger: int = TRIGGER_TIMER,
    limit: int = 100,
) -> dict:
    """Replay each codebase's pending failures: step 1 of the daily job on its own."""
    retry = _retry_each_codebase(
        engine,
        codebases,
        limit=limit,
        dry_run=dry_run,
        trigger=trigger,
    )
    return {"success": bool(retry["success"]), "retry": retry}


def run_retry_then_delta(
    engine,
    *,
    codebases: list[str],
    date_from: str | None = None,
    date_to: str | None = None,
    dry_run: bool = False,
    trigger: int = TRIGGER_TIMER,
    retry_limit: int = 100,
    sync_addresses: bool = True,
    sync_hierarchy: bool = True,
    sync_hco_relations: bool = True,
    sync_activity_affiliations: bool = True,
    sync_picklists: bool = True,
    sync_choices: bool = True,
) -> dict:
    """Run the daily job: retry, delta, resolve IDs the delta fixed.

    The window end is worked out after the retry step, so a long retry does
    not leave a gap between this window's end and the next one's start. Leave
    ``date_to`` unset for scheduled runs; a fixed end is safe (the next window
    starts from ts_watermarkto) but re-reads the time the run waited.

    Without ``date_from`` and without a Full or Delta job that covered its
    window, nothing has read these codebases yet: a delta would only see recent
    changes, so this runs the full sync instead, without the retry, and returns
    its result with ``promoted_to_full`` set.
    """
    if not date_from:
        date_from = _delta_start(engine.dataverse, codebases)
        if date_from is None:
            return _promote_to_full(
                engine,
                codebases=codebases,
                dry_run=dry_run,
                trigger=trigger,
                sync_choices=sync_choices,
            )

    retry = _retry_each_codebase(
        engine,
        codebases,
        limit=retry_limit,
        dry_run=dry_run,
        trigger=trigger,
    )
    result = run_delta(
        engine,
        codebases=codebases,
        date_from=date_from,
        date_to=date_to,
        dry_run=dry_run,
        trigger=trigger,
        sync_addresses=sync_addresses,
        sync_hierarchy=sync_hierarchy,
        sync_hco_relations=sync_hco_relations,
        sync_activity_affiliations=sync_activity_affiliations,
        sync_picklists=sync_picklists,
        sync_choices=sync_choices,
    )
    return {**result, "retry": retry}


def run_delta(
    engine,
    *,
    codebases: list[str],
    date_from: str | None = None,
    date_to: str | None = None,
    dry_run: bool = False,
    trigger: int = TRIGGER_TIMER,
    sync_addresses: bool = True,
    sync_hierarchy: bool = True,
    sync_hco_relations: bool = True,
    sync_activity_affiliations: bool = True,
    sync_picklists: bool = True,
    sync_choices: bool = True,
) -> dict:
    """Pull the delta and resolve IDs it fixed: steps 2 and 3 of the daily job.

    The window is worked out as in run_retry_then_delta, including the full
    sync for a codebase that no job has read yet (``promoted_to_full``).
    """
    if not date_from:
        date_from = _delta_start(engine.dataverse, codebases)
        if date_from is None:
            return _promote_to_full(
                engine,
                codebases=codebases,
                dry_run=dry_run,
                trigger=trigger,
                sync_choices=sync_choices,
            )

    # A retry leaves its parent job on the tracer; the delta is not a retry.
    engine.tracer.trigger = trigger
    engine.tracer.retry_of_job_id = None
    engine.succeeded_ids.clear()
    if hasattr(engine, "succeeded_records"):
        engine.succeeded_records.clear()
    iso_from, iso_to = delta_window(
        engine.dataverse, codebases, date_from=date_from, date_to=date_to
    )
    delta = engine.sync_delta(
        codebases=codebases,
        date_from=iso_from,
        date_to=iso_to,
        dry_run=dry_run,
        sync_addresses=sync_addresses,
        sync_hierarchy=sync_hierarchy,
        sync_hco_relations=sync_hco_relations,
        sync_activity_affiliations=sync_activity_affiliations,
        sync_picklists=sync_picklists,
        sync_choices=sync_choices,
    )
    resolved = 0
    if delta.get("success") and not dry_run:
        resolved = resolve_pending_for_ids(
            engine.dataverse, engine.succeeded_ids, exclude_job_guid=engine.tracer.job_guid,
            succeeded_records=getattr(engine, "succeeded_records", None),
            failed_records=getattr(engine.tracer, "recent_failures", []),
        )
    return {
        "success": bool(delta.get("success")),
        "delta": delta,
        "resolved_by_delta": resolved,
    }


def run_full(
    engine,
    *,
    codebases: list[str],
    dry_run: bool = False,
    trigger: int = TRIGGER_TIMER,
    sync_choices: bool = True,
) -> dict:
    """Run the monthly job: full sync, then resolve IDs the full sync fixed."""
    engine.tracer.trigger = trigger
    engine.tracer.retry_of_job_id = None
    engine.succeeded_ids.clear()
    if hasattr(engine, "succeeded_records"):
        engine.succeeded_records.clear()
    full = engine.sync_full(
        codebases=codebases, dry_run=dry_run, sync_choices=sync_choices
    )
    resolved = 0
    if full.get("success") and not dry_run:
        resolved = resolve_pending_for_ids(
            engine.dataverse, engine.succeeded_ids, exclude_job_guid=engine.tracer.job_guid,
            succeeded_records=getattr(engine, "succeeded_records", None),
            failed_records=getattr(engine.tracer, "recent_failures", []),
        )
    return {
        "success": bool(full.get("success")),
        "full": full,
        "resolved_by_full": resolved,
    }


def delta_from_setting(date_to: str | None = None) -> str | None:
    """Return SYNC_DELTA_FROM as an ISO start, or None when unset or unusable.

    A bare date means 00:00 UTC that day. A value that cannot be parsed, or
    that is not before the window end, is logged and ignored, so the delta
    falls back to the window worked out from ts_integrationjob.
    """
    raw = os.environ.get(DELTA_FROM_SETTING, "").strip()
    if not raw:
        return None
    value = raw if "T" in raw else f"{raw}T00:00:00Z"
    start = _parse_iso(value)
    if start is None:
        logger.error(
            "%s=%r is not a date (use 2026-08-01 or 2026-08-01T00:00:00Z); ignored",
            DELTA_FROM_SETTING,
            raw,
        )
        return None
    end = _parse_iso(date_to if not date_to or "T" in date_to else f"{date_to}T23:59:59Z")
    end = end or datetime.now(timezone.utc)
    if start >= end:
        logger.error(
            "%s=%s is not before the window end %s; ignored",
            DELTA_FROM_SETTING,
            _iso(start),
            _iso(end),
        )
        return None
    logger.warning(
        "Delta starts at %s from %s, not from the last covering job. "
        "Every delta does so until the setting is removed.",
        _iso(start),
        DELTA_FROM_SETTING,
    )
    return _iso(start)


def _busy(engine, codebases: list[str], if_busy: str | None, job: str) -> dict | None:
    """Apply ``if_busy`` when a live job is syncing one of the codebases.

    Returns the skipped result for ``"skip"``, raises SyncBusy for ``"raise"``,
    and returns None when nothing is running or ``if_busy`` is None.
    """
    if not if_busy:
        return None
    running = active_job(engine.dataverse, codebases)
    if not running:
        return None
    if if_busy == IF_BUSY_SKIP:
        logger.warning(
            "Skipped %s sync of %s: job %s is still running (heartbeat %s)",
            job,
            running["codebase"],
            running.get("ts_jobid"),
            running.get("ts_lastheartbeat"),
        )
        return {"success": True, "skipped": True, "busy_job": running.get("ts_jobid")}
    raise SyncBusy(running["codebase"], running)


def run_from_env(
    *,
    codebases: list[str] | None = None,
    full: bool = False,
    retry: bool = True,
    date_from: str | None = None,
    date_to: str | None = None,
    dry_run: bool = False,
    trigger: int = TRIGGER_TIMER,
    retry_limit: int = 100,
    config_dir: str | None = None,
    bulk_batch_size: int | None = None,
    if_busy: str | None = None,
    sync_choices: bool = True,
) -> dict:
    """Build the OneKey and Dataverse clients from the environment and run the job.

    ``full`` runs the monthly full sync; otherwise retry then delta, or the
    delta alone with ``retry=False`` (the delta queue, whose retry already ran
    from the retry queue).

    The delta starts at ``date_from`` when given (CLI ``--date-from``), else at
    the SYNC_DELTA_FROM setting when set, else where the last covering job
    stopped reading.

    ``if_busy`` decides what happens when a live job is already syncing one of
    the codebases: ``"skip"`` returns ``{"success": True, "skipped": True}``
    without running, ``"raise"`` raises :class:`SyncBusy`, and None (the
    default, for the CLI) does not check.
    """
    selected = codebases or ["WUK"]
    with engine_from_env(config_dir, bulk_batch_size) as engine:
        if not dry_run:
            skipped = _busy(engine, selected, if_busy, "full" if full else "delta")
            if skipped:
                return skipped
        if full:
            return run_full(
                engine,
                codebases=selected,
                dry_run=dry_run,
                trigger=trigger,
                sync_choices=sync_choices,
            )
        if not retry:
            return run_delta(
                engine,
                codebases=selected,
                date_from=date_from or delta_from_setting(date_to),
                date_to=date_to,
                dry_run=dry_run,
                trigger=trigger,
                sync_choices=sync_choices,
            )
        return run_retry_then_delta(
            engine,
            codebases=selected,
            date_from=date_from or delta_from_setting(date_to),
            date_to=date_to,
            dry_run=dry_run,
            trigger=trigger,
            retry_limit=retry_limit,
            sync_choices=sync_choices,
        )


def retry_from_env(
    *,
    codebases: list[str] | None = None,
    dry_run: bool = False,
    trigger: int = TRIGGER_TIMER,
    limit: int = 100,
    config_dir: str | None = None,
    bulk_batch_size: int | None = None,
    if_busy: str | None = None,
) -> dict:
    """Build the clients from the environment and replay pending failures only.

    ``if_busy`` works as in run_from_env. The retry queue skips a busy
    codebase: its failure rows stay Pending for the next run.
    """
    selected = codebases or ["WUK"]
    with engine_from_env(config_dir, bulk_batch_size) as engine:
        if not dry_run:
            skipped = _busy(engine, selected, if_busy, "retry")
            if skipped:
                return skipped
        return run_retry(
            engine,
            codebases=selected,
            dry_run=dry_run,
            trigger=trigger,
            limit=limit,
        )


def sync_choice_picklists(config_dir: str | None = None) -> bool:
    """Sync the choice options once, for every country in this run.

    Choice options come from value_mapping.csv and do not depend on the
    codebase. The timers run this before they enqueue, so the country workers
    can skip it instead of all writing the same option sets at once. Returns
    True when every option set synced cleanly; on False the workers sync
    choices themselves, as before.
    """
    try:
        with engine_from_env(config_dir) as engine:
            summaries = engine.sync_picklists(codebases=None)
    except Exception:
        logger.exception("Choice picklist sync failed; country workers will retry it")
        return False
    errors = sum(int(summary.get("errors") or 0) for summary in summaries or [])
    if errors:
        logger.warning(
            "Choice picklist sync finished with %d error(s); country workers will retry it",
            errors,
        )
        return False
    return True


@contextmanager
def engine_from_env(config_dir: str | None = None, bulk_batch_size: int | None = None):
    """Yield a sync engine on OneKey and Dataverse clients built from the environment."""
    from dataverse import DataverseClient, DataverseConfig
    from integration import create_sync_engine
    from onekey import OneKeyClient, OneKeyConfig, OneKeySession

    load_env()
    onekey = OneKeyClient(OneKeySession(OneKeyConfig.from_env()))
    dataverse = DataverseClient(DataverseConfig.from_env())
    try:
        yield create_sync_engine(
            onekey,
            dataverse,
            config_dir,
            bulk_batch_size=bulk_batch_size,
        )
    finally:
        onekey.close()
        if hasattr(dataverse, "close"):
            dataverse.close()
