"""Azure Storage Queue messages and worker dispatch for scheduled syncs.

The timers put one SyncQueueMessage per codebase on onekey-queue, and
scheduled_onekey_worker passes each delivery to handle_message:

- done / skipped: the sync ran, or a delta found its codebase busy and left
  the window to the next delta. Azure deletes the message.
- poison: retrying cannot help (a malformed message, or a full sync that
  already failed on FULL_MAX_DELIVERIES deliveries). The worker writes the
  body to onekey-queue-poison and returns, so Azure deletes the original.
- an exception: Azure shows the message again after host.json
  visibilityTimeout and moves it to onekey-queue-poison after
  maxDequeueCount deliveries.
"""

from __future__ import annotations

import json
import logging
from dataclasses import asdict, dataclass
from typing import Any


logger = logging.getLogger(__name__)

MESSAGE_VERSION = 1
MODES = {"delta", "full"}

# A failed full sync starts again from the first phase and runs for hours.
# Give it this many deliveries instead of host.json's maxDequeueCount (5).
# Busy deliveries count too: the worker only sees the dequeue count.
FULL_MAX_DELIVERIES = 2

OUTCOME_DONE = "done"
OUTCOME_SKIPPED = "skipped"
OUTCOME_POISON = "poison"


class QueueMessageError(ValueError):
    """The queue message is malformed or unsupported."""


class QueueWorkFailed(RuntimeError):
    """A sync returned an unsuccessful result and should be retried."""


@dataclass(frozen=True)
class SyncQueueMessage:
    mode: str
    codebase: str
    # Only for manual replays. Scheduled deltas leave it unset and read up to
    # the moment the worker runs, after the retry step.
    date_to: str | None = None
    retry_limit: int = 100
    # False when the timer already synced the choice options for this run.
    choices: bool = True
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
        if mode == "full" and self.date_to is not None:
            raise QueueMessageError("date_to is only valid for delta work")
        if self.retry_limit < 0:
            raise QueueMessageError("retry_limit cannot be negative")
        if not isinstance(self.choices, bool):
            raise QueueMessageError("choices must be true or false")
        object.__setattr__(self, "mode", mode)
        object.__setattr__(self, "codebase", codebase)

    def encode(self) -> str:
        return json.dumps(asdict(self), separators=(",", ":"), sort_keys=True)

    @classmethod
    def decode(cls, value: str | bytes | dict[str, Any]) -> "SyncQueueMessage":
        try:
            payload = value if isinstance(value, dict) else json.loads(value)
            if not isinstance(payload, dict):
                raise TypeError("message must be a JSON object")
            allowed = {"mode", "codebase", "date_to", "retry_limit", "choices", "version"}
            unknown = set(payload) - allowed
            if unknown:
                raise QueueMessageError(
                    f"Unknown queue message fields: {', '.join(sorted(unknown))}"
                )
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


def scheduled_messages(
    mode: str,
    codebases: list[str],
    *,
    retry_limit: int = 100,
    choices: bool = True,
) -> list[str]:
    """Build one independently retryable message per distinct codebase.

    Delta messages carry no date_to: a window end fixed at enqueue time would
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
        ).encode()
        for codebase in unique
    ]


def dispatch(message: SyncQueueMessage) -> dict:
    """Run a queued sync and raise when Azure should retry the message.

    A delta whose codebase is busy is skipped: the next delta starts from the
    last window that was covered, so nothing is lost. A busy full sync raises
    SyncBusy and is retried later.
    """
    from integration.scheduled_sync import IF_BUSY_RAISE, IF_BUSY_SKIP, run_from_env

    result = run_from_env(
        codebases=[message.codebase],
        full=message.mode == "full",
        date_to=message.date_to,
        retry_limit=message.retry_limit,
        if_busy=IF_BUSY_RAISE if message.mode == "full" else IF_BUSY_SKIP,
        sync_choices=message.choices,
    )
    if result.get("skipped"):
        return result
    if not result.get("success"):
        detail = result.get(message.mode) or {}
        errors = "; ".join(str(error) for error in detail.get("errors") or [])
        raise QueueWorkFailed(
            f"{message.mode} sync failed for codebase {message.codebase}: "
            f"job={detail.get('job_id')} errors={errors[:500] or 'none recorded'}"
        )
    return result


def handle_message(body: str | bytes, dequeue_count: int) -> WorkOutcome:
    """Run one delivery of a queue message.

    Returns the outcome, or raises to make Azure retry the message.
    """
    from integration.scheduled_sync import SyncBusy

    try:
        item = SyncQueueMessage.decode(body)
    except QueueMessageError as exc:
        logger.error("Queue message rejected, retrying cannot fix it: %s", exc)
        return WorkOutcome(OUTCOME_POISON, reason=str(exc))

    logger.info(
        "Queue work started: mode=%s codebase=%s delivery=%d",
        item.mode,
        item.codebase,
        dequeue_count,
    )
    try:
        result = dispatch(item)
    except SyncBusy as exc:
        logger.warning("Queue work deferred: %s", exc)
        raise
    except Exception as exc:
        if item.mode == "full" and dequeue_count >= FULL_MAX_DELIVERIES:
            logger.exception(
                "Full sync for %s failed on delivery %d; not retrying",
                item.codebase,
                dequeue_count,
            )
            return WorkOutcome(
                OUTCOME_POISON,
                item=item,
                reason=f"full sync failed on delivery {dequeue_count}: {exc}"[:1000],
            )
        raise
    if result.get("skipped"):
        return WorkOutcome(OUTCOME_SKIPPED, item=item, result=result)
    return WorkOutcome(OUTCOME_DONE, item=item, result=result)
