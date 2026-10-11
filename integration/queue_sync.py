"""Azure Storage Queue messages and worker dispatch for scheduled syncs.

Each kind of work has its own queue and worker (QUEUES):

- onekey-full: one message per codebase from the monthly timer. The worker
  runs the full sync.
- onekey-retry: one message per codebase from the daily timer. The worker
  replays that codebase's pending failures, then queues its delta.
- onekey-delta: filled by the retry worker. The worker pulls the delta.

Retry and delta of a codebase are chained rather than queued together: the
busy check counts a running retry, so a delta started beside it is skipped.

Azure moves a message that fails maxDequeueCount deliveries to
``<queue>-poison`` and that name cannot be changed, so each worker writes its
own poison messages there too (poison_queue). Each delivery ends one way:

- done / skipped: the work ran, or the codebase was busy and the next run
  covers it. Azure deletes the message.
- poison: retrying cannot help: a malformed message, a message on another
  mode's queue, or work that failed on its last delivery (FULL_MAX_DELIVERIES,
  RETRY_MAX_DELIVERIES). The worker writes the body to the poison queue and
  returns, so Azure deletes the original. A poisoned retry still queues the
  delta.
- an exception: Azure shows the message again after host.json
  visibilityTimeout and moves it to the poison queue after maxDequeueCount
  deliveries.

A timer run stamps its messages with a run_id and the moment it queued them
(new_run), and a retry passes both on to its delta. The run_id also records
how many countries the run queued. The workers log each message's time and
the run's time so far (run_progress), and the run_id goes into each job's
ts_name, so integration.run_report can total a whole run.

Fields a message carries that this version does not know are logged and
ignored, so a message from a newer version still runs after a rollback.
Breaking changes bump MESSAGE_VERSION instead.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any


logger = logging.getLogger(__name__)

MESSAGE_VERSION = 1

QUEUES = {
    "full": "onekey-full",
    "delta": "onekey-delta",
    "retry": "onekey-retry",
}
MODES = set(QUEUES)

# A failed full sync starts again from the first phase and runs for hours.
# Give it this many deliveries instead of host.json's maxDequeueCount (5).
# Busy deliveries count too: the worker only sees the dequeue count.
FULL_MAX_DELIVERIES = 2

# A failed retry leaves its rows Pending for the next run, and the delta waits
# for the retry to end, so one more delivery is enough.
RETRY_MAX_DELIVERIES = 2

OUTCOME_DONE = "done"
OUTCOME_SKIPPED = "skipped"
OUTCOME_POISON = "poison"

# A run_id goes into log lines and ts_name, so it holds no spaces.
_RUN_ID = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$")


def poison_queue(queue: str) -> str:
    """The queue Azure moves this queue's messages to after maxDequeueCount."""
    return f"{queue}-poison"


class QueueMessageError(ValueError):
    """The queue message is malformed or unsupported."""


class QueueWorkFailed(RuntimeError):
    """A sync returned an unsuccessful result and should be retried.

    ``full_sync`` is True when a full sync ran, including a delta message
    whose codebase had no completed job yet and was synced in full.
    """

    def __init__(self, message: str, *, full_sync: bool = False):
        super().__init__(message)
        self.full_sync = full_sync


@dataclass(frozen=True)
class SyncQueueMessage:
    mode: str
    codebase: str
    # Only for manual replays. Scheduled deltas leave it unset and read up to
    # the moment the worker runs.
    date_to: str | None = None
    # Pending failures per retry run; only read by the retry worker.
    retry_limit: int = 100
    # False when the timer already synced the choice options for this run.
    choices: bool = True
    # The timer run this message belongs to, and when that run was queued
    # (ISO UTC). Unset on manual messages.
    run_id: str | None = None
    enqueued_at: str | None = None
    version: int = MESSAGE_VERSION

    def __post_init__(self) -> None:
        mode = self.mode.strip().lower()
        codebase = self.codebase.strip().upper()
        if self.version != MESSAGE_VERSION:
            raise QueueMessageError(f"Unsupported queue message version: {self.version}")
        if mode not in MODES:
            raise QueueMessageError(f"Unsupported sync mode: {self.mode!r}")
        if not codebase:
            raise QueueMessageError("codebase is required")
        if mode != "delta" and self.date_to is not None:
            raise QueueMessageError("date_to is only valid for delta work")
        if self.retry_limit < 0:
            raise QueueMessageError("retry_limit cannot be negative")
        if not isinstance(self.choices, bool):
            raise QueueMessageError("choices must be true or false")
        if self.run_id is not None and not (
            isinstance(self.run_id, str) and _RUN_ID.match(self.run_id)
        ):
            raise QueueMessageError(f"Invalid run_id: {self.run_id!r}")
        if self.enqueued_at is not None and _parse_utc(self.enqueued_at) is None:
            raise QueueMessageError(f"Invalid enqueued_at: {self.enqueued_at!r}")
        object.__setattr__(self, "mode", mode)
        object.__setattr__(self, "codebase", codebase)

    def encode(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"), sort_keys=True)

    @classmethod
    def decode(
        cls, value: str | bytes | dict[str, Any], mode: str | None = None
    ) -> "SyncQueueMessage":
        """Parse a message body. ``mode`` fills in a body that has none.

        Unknown fields are logged and dropped (see the module docstring).
        """
        try:
            payload = value if isinstance(value, dict) else json.loads(value)
            if not isinstance(payload, dict):
                raise TypeError("message must be a JSON object")
            allowed = {
                "mode",
                "codebase",
                "date_to",
                "retry_limit",
                "choices",
                "run_id",
                "enqueued_at",
                "version",
            }
            unknown = set(payload) - allowed
            if unknown:
                logger.warning(
                    "Ignoring unknown queue message fields: %s", ", ".join(sorted(unknown))
                )
                payload = {key: payload[key] for key in payload if key in allowed}
            if mode and "mode" not in payload:
                payload = {**payload, "mode": mode}
            return cls(**payload)
        except QueueMessageError:
            raise
        except (json.JSONDecodeError, UnicodeDecodeError, TypeError, AttributeError) as exc:
            raise QueueMessageError(f"Invalid queue message: {exc}") from exc


@dataclass
class WorkOutcome:
    status: str
    item: SyncQueueMessage | None = None
    result: dict | None = None
    reason: str | None = None
    # Encoded message for onekey-delta, once a retry is over.
    next_delta: str | None = None


def _parse_utc(value: str) -> datetime | None:
    try:
        moment = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


def _iso(moment: datetime) -> str:
    return moment.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def new_run(job: str, countries: int, now: datetime | None = None) -> tuple[str, str]:
    """Return (run_id, enqueued_at) for a timer run that queues ``countries`` codebases.

    The run_id is the kind of run, when it was queued and how many countries
    it queued, e.g. delta-20261011T000001Z-7, so a report can tell when every
    country is done.
    """
    moment = now or datetime.now(timezone.utc)
    stamp = f"{moment.astimezone(timezone.utc):%Y%m%dT%H%M%SZ}"
    return f"{job}-{stamp}-{int(countries)}", _iso(moment)


def scheduled_messages(
    mode: str,
    codebases: list[str],
    *,
    retry_limit: int = 100,
    choices: bool = True,
    run_id: str | None = None,
    enqueued_at: str | None = None,
) -> list[str]:
    """Build one independently retryable message per distinct codebase.

    Messages carry no date_to: a delta window end fixed at enqueue time would
    end before the worker starts, and the gap would never be read.
    """
    unique = dict.fromkeys(
        code.strip().upper() for code in codebases if code and code.strip()
    )
    return [
        SyncQueueMessage(
            mode=mode,
            codebase=codebase,
            retry_limit=retry_limit,
            choices=choices,
            run_id=run_id,
            enqueued_at=enqueued_at,
        ).encode()
        for codebase in unique
    ]


def delta_after(item: SyncQueueMessage) -> str:
    """The delta message a retry queues for its codebase once it is over.

    It stays in the retry's run, so the run's time runs until the delta ends.
    """
    return SyncQueueMessage(
        mode="delta",
        codebase=item.codebase,
        choices=item.choices,
        run_id=item.run_id,
        enqueued_at=item.enqueued_at,
    ).encode()


def run_progress(
    item: SyncQueueMessage, started_at: datetime, finished_at: datetime
) -> dict | None:
    """How long this message took, and how long its run has taken so far.

    ``elapsed_s`` counts from when the timer queued the run, so the largest
    value among a run's messages is the run's total time. None for a message
    outside a timer run.
    """
    if not item.run_id:
        return None
    enqueued = _parse_utc(item.enqueued_at) if item.enqueued_at else None
    return {
        "run_id": item.run_id,
        "duration_s": max(0, int((finished_at - started_at).total_seconds())),
        "elapsed_s": (
            max(0, int((finished_at - enqueued).total_seconds())) if enqueued else None
        ),
    }


def dispatch(message: SyncQueueMessage) -> dict:
    """Run a queued sync and raise when Azure should retry the message.

    A delta or retry whose codebase is busy is skipped: the next delta starts
    from the last window that was covered and pending rows stay Pending, so
    nothing is lost. A busy full sync raises SyncBusy and is retried later. A
    delta for a codebase with no completed job runs as a full sync
    (``promoted_to_full`` in the result).
    """
    from integration.scheduled_sync import (
        IF_BUSY_RAISE,
        IF_BUSY_SKIP,
        retry_from_env,
        run_from_env,
    )

    if message.mode == "retry":
        result = retry_from_env(
            codebases=[message.codebase],
            limit=message.retry_limit,
            if_busy=IF_BUSY_SKIP,
            run_id=message.run_id,
        )
    else:
        result = run_from_env(
            codebases=[message.codebase],
            full=message.mode == "full",
            retry=False,
            date_to=message.date_to,
            if_busy=IF_BUSY_RAISE if message.mode == "full" else IF_BUSY_SKIP,
            sync_choices=message.choices,
            run_id=message.run_id,
        )
    if result.get("skipped"):
        return result
    if not result.get("success"):
        ran = ran_mode(message, result)
        detail = result.get(ran) or {}
        errors = "; ".join(str(error) for error in detail.get("errors") or [])
        raise QueueWorkFailed(
            f"{ran} sync failed for codebase {message.codebase}: "
            f"job={job_ids(detail)} errors={errors[:500] or 'none recorded'}",
            full_sync=ran == "full",
        )
    return result


def ran_mode(message: SyncQueueMessage, result: dict | None) -> str:
    """The sync that actually ran: the message mode, or full when promoted."""
    return "full" if (result or {}).get("promoted_to_full") else message.mode


def job_ids(detail: dict) -> str | None:
    """The ts_jobid a sync wrote, or the comma-separated ids of a retry's jobs."""
    return detail.get("job_id") or ",".join(detail.get("jobs") or []) or None


def _delivery_limit(item: SyncQueueMessage, exc: BaseException) -> int | None:
    """Deliveries this work gets before the worker poisons it; None leaves it to Azure."""
    if item.mode == "full" or getattr(exc, "full_sync", False):
        return FULL_MAX_DELIVERIES
    if item.mode == "retry":
        return RETRY_MAX_DELIVERIES
    return None


def handle_message(
    body: str | bytes, dequeue_count: int, mode: str | None = None
) -> WorkOutcome:
    """Run one delivery of a queue message.

    ``mode`` is the mode of the queue the message came from: a body without
    a mode takes it, and a body with another mode is poisoned. Returns the
    outcome, or raises to make Azure retry the message.
    """
    from integration.scheduled_sync import SyncBusy

    try:
        item = SyncQueueMessage.decode(body, mode=mode)
    except QueueMessageError as exc:
        logger.error("Queue message rejected, retrying cannot fix it: %s", exc)
        return WorkOutcome(OUTCOME_POISON, reason=str(exc))
    if mode and item.mode != mode:
        reason = f"{item.mode} message on {QUEUES[mode]}; put it on {QUEUES[item.mode]}"
        logger.error("Queue message rejected, retrying cannot fix it: %s", reason)
        return WorkOutcome(OUTCOME_POISON, item=item, reason=reason)

    logger.info(
        "Queue work started: mode=%s codebase=%s delivery=%d",
        item.mode,
        item.codebase,
        dequeue_count,
    )
    next_delta = delta_after(item) if item.mode == "retry" else None
    try:
        result = dispatch(item)
    except SyncBusy as exc:
        logger.warning("Queue work deferred: %s", exc)
        raise
    except Exception as exc:
        limit = _delivery_limit(item, exc)
        if limit and dequeue_count >= limit:
            work = "Retry" if item.mode == "retry" else "Full sync"
            logger.exception(
                "%s for %s failed on delivery %d; not retrying",
                work,
                item.codebase,
                dequeue_count,
            )
            return WorkOutcome(
                OUTCOME_POISON,
                item=item,
                reason=f"{work.lower()} failed on delivery {dequeue_count}: {exc}"[:1000],
                next_delta=next_delta,
            )
        raise
    if result.get("skipped"):
        return WorkOutcome(OUTCOME_SKIPPED, item=item, result=result)
    return WorkOutcome(OUTCOME_DONE, item=item, result=result, next_delta=next_delta)
