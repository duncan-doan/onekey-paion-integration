"""Scheduled OneKey jobs: the daily delta and the monthly full sync.

Daily delta:
1. Replay pending retryable failure rows.
2. Pull the delta from the ts_startedon of the last Full or Delta job that
   covered its whole window, up to now.
3. Mark pending rows Resolved when that delta upserts the same source ID.

Monthly full: pull the whole population, then resolve pending rows the same
way. A completed full sync becomes the start of the next delta window.

The ts_integrationjob table is the watermark. A job that stopped early or lost
a codebase is not used as the window start, so the next delta covers its
window again. A failed ID that OneKey does not change again will not appear
in a later delta, so step 1 is what retries it.
"""

from __future__ import annotations

import logging
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone

from dataverse.client import escape_odata
from integration.env import load_env
from integration.job_trace import (
    JOB_DELTA,
    JOB_FULL,
    JOBS,
    STATUS_PARTIAL,
    STATUS_SUCCEEDED,
    TRIGGER_TIMER,
    job_codebase,
)
from integration.retry_sync import resolve_pending_for_ids, retry_pending_failures

logger = logging.getLogger(__name__)

# Start each window this far before the previous job's ts_startedon. OneKey
# stamps changes with its own clock and upserts are idempotent, so the overlap
# only re-reads a few records.
WINDOW_OVERLAP = timedelta(minutes=15)

# Recent Succeeded/Partially Succeeded jobs inspected for the window start.
_START_SCAN = 50


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


def last_window_start(dataverse, codebases: list[str]) -> str | None:
    """Return ts_startedon of the latest Full or Delta job that covered its window.

    Retry jobs are excluded: a retry that spans several phases is typed Full,
    but it only replays failed IDs. They are the jobs with ts_retryofjob set.
    """
    jobs = dataverse.retrieve_multiple(
        JOBS,
        select=["ts_jobid", "ts_startedon", "ts_errorsummary"],
        filter=(
            f"ts_codebase eq '{escape_odata(job_codebase(codebases))}'"
            f" and (ts_jobtype eq {JOB_FULL} or ts_jobtype eq {JOB_DELTA})"
            f" and (ts_status eq {STATUS_SUCCEEDED} or ts_status eq {STATUS_PARTIAL})"
            " and _ts_retryofjob_value eq null"
        ),
        order_by="ts_startedon desc",
        top=_START_SCAN,
    )
    for job in jobs:
        if job.get("ts_startedon") and covered_window(job):
            logger.info(
                "Delta window starts from job %s (started %s)",
                job.get("ts_jobid"),
                job["ts_startedon"],
            )
            return job["ts_startedon"]
    return None


def delta_window(
    dataverse,
    codebases: list[str],
    date_from: str | None = None,
    date_to: str | None = None,
) -> tuple[str, str]:
    """Return ISO date_from and date_to for the next delta.

    Explicit dates win, and a bare YYYY-MM-DD covers that whole day. Otherwise
    the window runs from the last covering job's start, less WINDOW_OVERLAP,
    to now. With no such job it starts at 00:00 UTC yesterday.
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
) -> dict:
    """Run the daily job: retry, delta, resolve IDs the delta fixed.

    The window is worked out after the retry step, so a long retry does not
    leave a gap between this window's end and the next one's start.
    """
    retry = retry_pending_failures(
        engine,
        limit=retry_limit,
        dry_run=dry_run,
        trigger=trigger,
    )
    # The retry leaves its parent job on the tracer; the delta is not a retry.
    engine.tracer.trigger = trigger
    engine.tracer.retry_of_job_id = None
    engine.succeeded_ids.clear()
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
    )
    resolved = 0
    if delta.get("success") and not dry_run:
        resolved = resolve_pending_for_ids(
            engine.dataverse, engine.succeeded_ids, exclude_job_guid=engine.tracer.job_guid
        )
    return {
        "success": bool(delta.get("success")),
        "retry": retry,
        "delta": delta,
        "resolved_by_delta": resolved,
    }


def run_full(
    engine,
    *,
    codebases: list[str],
    dry_run: bool = False,
    trigger: int = TRIGGER_TIMER,
) -> dict:
    """Run the monthly job: full sync, then resolve IDs the full sync fixed."""
    engine.tracer.trigger = trigger
    engine.tracer.retry_of_job_id = None
    engine.succeeded_ids.clear()
    full = engine.sync_full(codebases=codebases, dry_run=dry_run)
    resolved = 0
    if full.get("success") and not dry_run:
        resolved = resolve_pending_for_ids(
            engine.dataverse, engine.succeeded_ids, exclude_job_guid=engine.tracer.job_guid
        )
    return {
        "success": bool(full.get("success")),
        "full": full,
        "resolved_by_full": resolved,
    }


def run_from_env(
    *,
    codebases: list[str] | None = None,
    full: bool = False,
    date_from: str | None = None,
    date_to: str | None = None,
    dry_run: bool = False,
    trigger: int = TRIGGER_TIMER,
    retry_limit: int = 100,
    config_dir: str | None = None,
    bulk_batch_size: int | None = None,
) -> dict:
    """Build the OneKey and Dataverse clients from the environment and run the job.

    ``full`` runs the monthly full sync; otherwise retry then delta.
    """
    selected = codebases or ["WUK"]
    with engine_from_env(config_dir, bulk_batch_size) as engine:
        if full:
            return run_full(
                engine, codebases=selected, dry_run=dry_run, trigger=trigger
            )
        return run_retry_then_delta(
            engine,
            codebases=selected,
            date_from=date_from,
            date_to=date_to,
            dry_run=dry_run,
            trigger=trigger,
            retry_limit=retry_limit,
        )


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
