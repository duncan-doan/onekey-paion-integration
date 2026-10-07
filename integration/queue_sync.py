"""Azure Storage Queue messages and worker dispatch for scheduled syncs."""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from typing import Any


MESSAGE_VERSION = 1
MODES = {"delta", "full"}


class QueueMessageError(ValueError):
    """The queue message is malformed or unsupported."""


class QueueWorkFailed(RuntimeError):
    """A sync returned an unsuccessful result and should be retried."""


@dataclass(frozen=True)
class SyncQueueMessage:
    mode: str
    codebase: str
    date_to: str | None = None
    retry_limit: int = 100
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
            allowed = {"mode", "codebase", "date_to", "retry_limit", "version"}
            unknown = set(payload) - allowed
            if unknown:
                raise QueueMessageError(
                    f"Unknown queue message fields: {', '.join(sorted(unknown))}"
                )
            return cls(**payload)
        except QueueMessageError:
            raise
        except (json.JSONDecodeError, TypeError) as exc:
            raise QueueMessageError(f"Invalid queue message: {exc}") from exc


def scheduled_messages(
    mode: str,
    codebases: list[str],
    *,
    date_to: str | None = None,
    retry_limit: int = 100,
) -> list[str]:
    """Build one independently retryable queue message per codebase."""
    return [
        SyncQueueMessage(
            mode=mode,
            codebase=codebase,
            date_to=date_to if mode == "delta" else None,
            retry_limit=retry_limit,
        ).encode()
        for codebase in codebases
    ]


def dispatch(message: SyncQueueMessage) -> dict:
    """Run a queued sync and raise when Azure should retry the message."""
    from integration.scheduled_sync import run_from_env

    result = run_from_env(
        codebases=[message.codebase],
        full=message.mode == "full",
        date_to=message.date_to,
        retry_limit=message.retry_limit,
    )
    if not result.get("success"):
        raise QueueWorkFailed(
            f"{message.mode} sync failed for codebase {message.codebase}"
        )
    return result
