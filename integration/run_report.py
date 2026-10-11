"""Total time of a scheduled run, from its ts_integrationjob rows.

A timer run's messages carry a run_id (integration.queue_sync.new_run), and
every job they start has ``run=<run_id>`` in its ts_name. summarize_run reads
those jobs back and reports:

- total_s: from when the timer queued the run until its last job ended, i.e.
  how long the whole run took. Until the run is complete it is the time so
  far.
- work_s: the jobs' durations added up, i.e. roughly how long the run would
  take on one instance.
- one row per job: codebase, type (Retry for a retry job), state, start, end
  and duration.

The run is complete when nothing is running and every country it queued
(the count at the end of the run_id) has a main job, full or delta, that
covered its whole window. A failed job does not count: Azure retries its
message. A job left Running whose heartbeat stopped was killed; it counts as
stopped, ending at its last heartbeat, and its message is delivered again. A
country skipped as busy, or whose message went to the poison queue, never
completes the run; the workers' scheduled_run_progress log lines show those.
"""

from __future__ import annotations

import re
from datetime import datetime, timezone

from dataverse.client import escape_odata
from integration.job_trace import (
    JOB_LABELS,
    JOBS,
    STATUS_LABELS,
    STATUS_PARTIAL,
    STATUS_RUNNING,
    STATUS_SUCCEEDED,
    run_marker,
)
from integration.scheduled_sync import ACTIVE_JOB_HEARTBEAT, covered_window

STATE_RUNNING = "Running"
STATE_STOPPED = "Stopped"
TYPE_RETRY = "Retry"

_RUN_IN_NAME = re.compile(r"\brun=(\S+)")
# kind-YYYYMMDDTHHMMSSZ-countries; runs from before the count lack its suffix.
_RUN_PARTS = re.compile(r"-(\d{8}T\d{6}Z)(?:-(\d+))?$")

_SELECT = [
    "ts_jobid",
    "ts_name",
    "ts_jobtype",
    "ts_status",
    "ts_codebase",
    "ts_startedon",
    "ts_completedon",
    "ts_durationseconds",
    "ts_lastheartbeat",
    "ts_errorsummary",
    "_ts_retryofjob_value",
]


def _parse(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        moment = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


def _seconds(start: datetime | None, end: datetime | None) -> int:
    if not start or not end:
        return 0
    return max(0, int((end - start).total_seconds()))


def run_queued_at(run_id: str) -> datetime | None:
    """When the timer queued the run, from the timestamp new_run put in its id."""
    match = _RUN_PARTS.search(run_id or "")
    if not match:
        return None
    return datetime.strptime(match.group(1), "%Y%m%dT%H%M%SZ").replace(tzinfo=timezone.utc)


def run_size(run_id: str) -> int | None:
    """How many countries the run queued, or None for a run_id without the count."""
    match = _RUN_PARTS.search(run_id or "")
    return int(match.group(2)) if match and match.group(2) else None


def _done(job: dict) -> bool:
    """True when a main job covered its window, so its message will not run again."""
    status = job.get("ts_status")
    return status == STATUS_SUCCEEDED or (status == STATUS_PARTIAL and covered_window(job))


def run_id_of(job: dict) -> str | None:
    """The run_id a job's ts_name carries, or None."""
    match = _RUN_IN_NAME.search(job.get("ts_name") or "")
    return match.group(1) if match else None


def fetch_run_jobs(dataverse, run_id: str) -> list[dict]:
    """Every ts_integrationjob row of one run, oldest first."""
    rows = dataverse.retrieve_multiple(
        JOBS,
        select=_SELECT,
        filter=f"contains(ts_name, '{escape_odata(run_marker(run_id))}')",
        order_by="ts_startedon asc",
    )
    return [row for row in rows if run_id_of(row) == run_id]


def latest_run_id(dataverse, job: str | None = None) -> str | None:
    """The newest run_id in ts_integrationjob; ``job`` keeps only "delta" or "full" runs."""
    marker = run_marker(f"{job}-" if job else "")
    rows = dataverse.retrieve_multiple(
        JOBS,
        select=["ts_name", "ts_startedon"],
        filter=f"contains(ts_name, '{escape_odata(marker)}')",
        order_by="ts_startedon desc",
        top=1,
        max_records=1,
    )
    return run_id_of(rows[0]) if rows else None


def summarize_run(run_id: str, jobs: list[dict], *, now: datetime | None = None) -> dict:
    """Total and per-job times of one run (see the module docstring)."""
    now = now or datetime.now(timezone.utc)
    stale_before = now - ACTIVE_JOB_HEARTBEAT
    rows = []
    finished: set[str] = set()
    for job in jobs:
        retry = bool(job.get("_ts_retryofjob_value"))
        if not retry and _done(job) and job.get("ts_codebase"):
            finished.add(job["ts_codebase"])
        started = _parse(job.get("ts_startedon"))
        status = job.get("ts_status")
        if status == STATUS_RUNNING:
            beat = _parse(job.get("ts_lastheartbeat")) or started
            if beat and beat < stale_before:
                state, ended = STATE_STOPPED, beat
                duration = _seconds(started, beat)
            else:
                state, ended = STATE_RUNNING, None
                duration = _seconds(started, now)
        else:
            state = STATUS_LABELS.get(status, str(status))
            ended = _parse(job.get("ts_completedon"))
            recorded = job.get("ts_durationseconds")
            duration = int(recorded) if recorded is not None else _seconds(started, ended)
        rows.append(
            {
                "job_id": job.get("ts_jobid"),
                "codebase": job.get("ts_codebase"),
                "type": TYPE_RETRY
                if retry
                else JOB_LABELS.get(job.get("ts_jobtype"), str(job.get("ts_jobtype"))),
                "state": state,
                "started": started,
                "ended": ended,
                "duration_s": duration,
            }
        )

    running = sum(1 for row in rows if row["state"] == STATE_RUNNING)
    expected = run_size(run_id)
    if expected is None:
        complete = bool(rows) and running == 0
    else:
        complete = running == 0 and len(finished) >= expected
    starts = [row["started"] for row in rows if row["started"]]
    ends = [row["ended"] for row in rows if row["ended"]]
    queued = run_queued_at(run_id) or (min(starts) if starts else None)
    last_end = (max(ends) if ends else None) if complete else now
    return {
        "run_id": run_id,
        "queued_at": queued,
        "last_end": last_end,
        "total_s": _seconds(queued, last_end),
        "work_s": sum(row["duration_s"] for row in rows),
        "countries": sorted({row["codebase"] for row in rows if row["codebase"]}),
        "expected": expected,
        "finished": sorted(finished),
        "running": running,
        "stopped": sum(1 for row in rows if row["state"] == STATE_STOPPED),
        "complete": complete,
        "jobs": rows,
    }


def format_duration(seconds: int) -> str:
    """3h 12m, 4m 05s, or 42s."""
    hours, rest = divmod(max(0, int(seconds)), 3600)
    minutes, secs = divmod(rest, 60)
    if hours:
        return f"{hours}h {minutes:02d}m"
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"
