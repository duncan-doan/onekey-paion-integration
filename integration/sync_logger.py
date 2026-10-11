"""Comprehensive logging for OneKey to Dataverse synchronization.

Provides structured logging for:
- Sync operations (start, progress, completion)
- Record-level success/failure
- Validation errors
- Relationship resolution
- Performance metrics

Lines read as ``<sentence>: key=value ... | context``. The sentence is for
people; the ``key=value`` numbers stay queryable (KQL ``parse-kv``); the context
ends with the first 8 characters of the integration job ID, so one run is one
Application Insights query (``traces | where message has "<job>"``).
Per-record successes are DEBUG; records are identified by OneKey ID and
Dataverse GUID only, never by personal names or field values.
"""

from __future__ import annotations

import json
import logging
import time
from collections import defaultdict
from contextlib import contextmanager
from dataclasses import dataclass, field, asdict
from datetime import datetime
from pathlib import Path
from typing import TYPE_CHECKING

from .env import log_dir as env_log_dir

if TYPE_CHECKING:
    from .validation import ValidationResult

logger = logging.getLogger(__name__)

LOG_FORMAT = "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"


def kv(**fields) -> str:
    """Format fields as ``key=value`` pairs for one log line, skipping None.

    Lists are comma-joined. A value with spaces, quotes or ``=`` is JSON-quoted
    so the line stays parseable (KQL ``parse`` / ``extract``).
    """
    parts = []
    for key, value in fields.items():
        if value is None:
            continue
        if isinstance(value, float):
            text = f"{value:.1f}"
        elif isinstance(value, (list, tuple, set)):
            text = ",".join(str(item) for item in value)
        else:
            text = str(value)
        if not text or any(ch.isspace() or ch in '="' for ch in text):
            text = json.dumps(text)
        parts.append(f"{key}={text}")
    return " ".join(parts)


def msg(lead: str, context: dict | None = None, **fields) -> str:
    """``<lead>: key=value ... | context`` for one log line."""
    text = lead
    body = kv(**fields)
    if body:
        text += ": " + body
    tail = kv(**(context or {}))
    if tail:
        text += " | " + tail
    return text


def short_job(job_id: str | None) -> str | None:
    """First 8 characters of a job ID; enough to find the run in App Insights."""
    return job_id[:8] if job_id else None


_PHASE_NAMES = {
    "accounts": "Accounts",
    "activity_affiliations": "Activity affiliations",
    "addresses": "Addresses",
    "codebase": "Codebase",
    "contacts": "Contacts",
    "extract": "OneKey extract",
    "hco_relations": "HCO relations",
    "hierarchy": "Hierarchy",
    "picklists": "Picklists",
    "validation": "Validation",
}

_ENTITY_NAMES = {
    "account": "Account",
    "contact": "Contact",
    "customeraddress": "Address",
    "ts_affiliation": "Affiliation",
}

_SYNC_NAMES = {
    "affiliations_only": "Affiliations-only sync",
    "delta": "Delta sync",
    "full": "Full sync",
    "hco_only": "HCO-only sync",
    "hcp_only": "HCP-only sync",
}


def phase_name(phase: str) -> str:
    """Readable name of an engine phase key (``hco_relations`` -> ``HCO relations``)."""
    if phase in _PHASE_NAMES:
        return _PHASE_NAMES[phase]
    text = phase.replace("_", " ")
    return text[:1].upper() + text[1:]


def _phase_lead(phase: str, context: dict) -> tuple[str, dict]:
    """Phase name for the lead; a codebase phase names its codebase there."""
    if phase == "codebase" and context.get("codbase"):
        rest = {key: value for key, value in context.items() if key != "codbase"}
        return f"Codebase {context['codbase']}", rest
    return phase_name(phase), context


def entity_name(entity: str) -> str:
    return _ENTITY_NAMES.get(entity, entity)


def _past(operation: str) -> str:
    return {"create": "created", "update": "updated"}.get(operation, operation)


def _clip(text, limit: int = 500) -> str:
    text = " ".join(str(text).split())
    return text if len(text) <= limit else text[: limit - 3] + "..."


# Entity results summarized as processed/created/updated/skipped/failed.
_ENTITY_KEYS = ("accounts", "addresses", "affiliations", "contacts", "activity_affiliations")
_COUNT_FIELDS = ("processed", "created", "updated", "skipped", "failed")


def entity_counts(results: dict | None) -> dict[str, str]:
    """``{"accounts": "3166/12/3154/0", ...}`` for the entities a sync touched."""
    counts = {}
    for key in _ENTITY_KEYS:
        stats = (results or {}).get(key)
        if stats and any(stats.get(name) for name in _COUNT_FIELDS):
            counts[key] = "/".join(str(int(stats.get(name) or 0)) for name in _COUNT_FIELDS)
    return counts


def _open_file_handler(log_file: Path) -> logging.FileHandler | None:
    """File handler for ``log_file``, or None when its directory cannot be written."""
    try:
        log_file.parent.mkdir(parents=True, exist_ok=True)
        handler = logging.FileHandler(log_file, encoding="utf-8")
    except OSError as exc:
        logger.warning("Log file disabled, cannot write %s: %s", log_file, exc)
        return None
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(logging.Formatter(LOG_FORMAT))
    return handler


@dataclass
class RecordLog:
    """Log entry for a single record operation."""
    timestamp: str
    operation: str  # "create", "update", "skip", "error"
    entity_type: str  # "account", "contact"
    source_id: str  # OneKey EID
    target_id: str | None  # Dataverse GUID
    success: bool
    message: str | None = None
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)
    duration_ms: float = 0


@dataclass
class SyncStats:
    """Statistics for a sync run."""
    started_at: str = ""
    completed_at: str = ""
    duration_seconds: float = 0
    
    # Counts by entity type
    accounts_processed: int = 0
    accounts_created: int = 0
    accounts_updated: int = 0
    accounts_skipped: int = 0
    accounts_failed: int = 0
    
    contacts_processed: int = 0
    contacts_created: int = 0
    contacts_updated: int = 0
    contacts_skipped: int = 0
    contacts_failed: int = 0
    
    # Validation stats
    validation_errors: int = 0
    validation_warnings: int = 0
    
    # Relationship stats
    relationships_resolved: int = 0
    relationships_failed: int = 0
    orphan_contacts: int = 0  # Contacts without valid account link


@dataclass
class SyncSession:
    """Complete sync session log."""
    session_id: str
    sync_type: str  # "full", "delta", "sample"
    codebases: list[str] = field(default_factory=list)
    date_from: str | None = None
    date_to: str | None = None
    stats: SyncStats = field(default_factory=SyncStats)
    counts: dict[str, str] = field(default_factory=dict)
    records: list[RecordLog] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization."""
        return {
            "session_id": self.session_id,
            "sync_type": self.sync_type,
            "codebases": self.codebases,
            "date_from": self.date_from,
            "date_to": self.date_to,
            "stats": asdict(self.stats),
            "counts": self.counts,
            "record_count": len(self.records),
            "error_count": len(self.errors),
        }


class SyncLogger:
    """Structured logger for sync operations."""
    
    def __init__(self, session_id: str = None, log_dir: Path = None):
        self.session_id = session_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        # None when SYNC_LOG_DIR is off or the directory is not writable.
        self.log_dir = Path(log_dir) if log_dir else env_log_dir()

        self.session = SyncSession(
            session_id=self.session_id,
            sync_type="unknown",
        )
        self._start_time = None
        self._operation_times: dict[str, list[float]] = defaultdict(list)
        # Set by the sync engine when a job begins; added to every line.
        self.job_id: str | None = None
        self._file_handler: logging.FileHandler | None = None

        # Set up file logging
        self._setup_file_logging()

    def _setup_file_logging(self):
        """Configure file handler for this session, unless file logs are off."""
        if self.log_dir is None:
            return
        log_file = self.log_dir / f"sync_{self.session_id}.log"
        file_handler = _open_file_handler(log_file)
        if file_handler is None:
            self.log_dir = None
            return

        # Add to root logger
        logging.getLogger().addHandler(file_handler)
        self._file_handler = file_handler
        logger.info(msg("Sync log file", {"session": self.session_id}, path=log_file))

    def close(self) -> None:
        """Detach and close this session's log file.

        A warm Function instance builds an engine, and so a SyncLogger, per
        queue message. Left on the root logger, each file handler would stay
        open and copy every later run's lines into this session's file.
        """
        handler, self._file_handler = self._file_handler, None
        if handler is not None:
            logging.getLogger().removeHandler(handler)
            handler.close()
    
    def start_sync(
        self,
        sync_type: str,
        codebases: list[str],
        date_from: str = None,
        date_to: str = None,
    ):
        """Log sync start."""
        self._start_time = time.time()
        self.session.sync_type = sync_type
        self.session.codebases = codebases
        self.session.date_from = date_from
        self.session.date_to = date_to
        self.session.stats.started_at = datetime.now().isoformat()

        logger.info(
            "==== %s ====",
            msg(
                f"{self._sync_name()} started",
                {"job": self.job},
                date_from=date_from,
                date_to=date_to,
            ),
        )

    def end_sync(self, success: bool = True, results: dict | None = None):
        """Log sync completion with the engine's counts and write the summary."""
        stats = self.session.stats
        stats.completed_at = datetime.now().isoformat()
        if self._start_time:
            stats.duration_seconds = time.time() - self._start_time
        for entity, prefix in (("accounts", "accounts"), ("contacts", "contacts")):
            counted = (results or {}).get(entity) or {}
            for name in _COUNT_FIELDS:
                setattr(stats, f"{prefix}_{name}", int(counted.get(name) or 0))
        self.session.counts = entity_counts(results)
        hierarchy = (results or {}).get("hierarchy") or {}

        logger.log(
            logging.INFO if success else logging.WARNING,
            "==== %s ====",
            msg(
                f"{self._sync_name()} finished in {stats.duration_seconds:.1f}s"
                + (" (processed/created/updated/skipped/failed)" if self.session.counts else ""),
                {"job": self.job},
                success=success,
                **self.session.counts,
                hierarchy_linked=hierarchy.get("linked"),
                orphan_contacts=stats.orphan_contacts,
                relationships_failed=stats.relationships_failed,
                validation_errors=stats.validation_errors,
                validation_warnings=stats.validation_warnings,
                errors=len((results or {}).get("errors") or []),
            ),
        )

        # Write session summary to JSON
        self._write_summary()

    def _write_summary(self):
        """Write session summary to JSON file."""
        if self.log_dir is None:
            return
        summary_file = self.log_dir / f"sync_{self.session_id}_summary.json"
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump(self.session.to_dict(), f, indent=2)
        logger.info(msg("Sync summary file", {"job": self.job}, path=summary_file))
    
    @contextmanager
    def log_operation(self, operation: str, entity_type: str, source_id: str):
        """Context manager to log and time an operation."""
        start = time.time()
        record = RecordLog(
            timestamp=datetime.now().isoformat(),
            operation=operation,
            entity_type=entity_type,
            source_id=source_id,
            target_id=None,
            success=False,
        )
        
        try:
            yield record
            record.success = True
        except Exception as e:
            record.success = False
            record.message = str(e)
            record.errors.append(str(e))
            logger.error(
                "Operation %s failed for %s %s: %s",
                operation, entity_type, source_id, e
            )
            raise
        finally:
            record.duration_ms = (time.time() - start) * 1000
            self._operation_times[f"{entity_type}_{operation}"].append(record.duration_ms)
            self.session.records.append(record)
            
            # Update stats
            if entity_type == "account":
                self.session.stats.accounts_processed += 1
                if record.success:
                    if operation == "create":
                        self.session.stats.accounts_created += 1
                    elif operation == "update":
                        self.session.stats.accounts_updated += 1
                    elif operation == "skip":
                        self.session.stats.accounts_skipped += 1
                else:
                    self.session.stats.accounts_failed += 1
            elif entity_type == "contact":
                self.session.stats.contacts_processed += 1
                if record.success:
                    if operation == "create":
                        self.session.stats.contacts_created += 1
                    elif operation == "update":
                        self.session.stats.contacts_updated += 1
                    elif operation == "skip":
                        self.session.stats.contacts_skipped += 1
                else:
                    self.session.stats.contacts_failed += 1
    
    @property
    def job(self) -> str | None:
        """Short job ID for the context part of each line."""
        return short_job(self.job_id)

    def _sync_name(self) -> str:
        name = _SYNC_NAMES.get(self.session.sync_type, f"{self.session.sync_type} sync")
        codes = ",".join(self.session.codebases or [])
        return f"{name} {codes}".strip()

    def log_record_success(
        self,
        entity_type: str,
        source_id: str,
        target_id: str,
        operation: str,
    ):
        """Log one upserted record at DEBUG; the phase line carries the counts."""
        logger.debug(
            "%s %s %s: target=%s | job=%s",
            entity_name(entity_type), source_id, _past(operation), target_id, self.job,
        )

    def log_record_error(
        self,
        entity_type: str,
        source_id: str,
        operation: str,
        error: str,
    ):
        """Log failed record operation."""
        logger.error(
            msg(
                f"{entity_name(entity_type)} {source_id} failed ({operation})",
                {"job": self.job},
                error=_clip(error),
            )
        )
        self.session.errors.append(f"{entity_type} {source_id}: {error}")

    def log_validation_result(self, validation: "ValidationResult", context: str = ""):
        """Log validation results, without the offending source values."""
        for level, issues, counter in (
            (logging.ERROR, validation.errors, "validation_errors"),
            (logging.WARNING, validation.warnings, "validation_warnings"),
        ):
            kind = "error" if level == logging.ERROR else "warning"
            for issue in issues:
                logger.log(
                    level,
                    msg(
                        f"Validation {kind}" + (f" ({context})" if context else ""),
                        {"job": self.job},
                        entity=getattr(issue, "entity", None),
                        field=getattr(issue, "field", None),
                        type=getattr(issue, "error_type", None),
                        message=_clip(getattr(issue, "message", issue)),
                    ),
                )
                setattr(self.session.stats, counter, getattr(self.session.stats, counter) + 1)

    def log_relationship_resolved(
        self,
        source_entity: str,
        source_id: str,
        target_entity: str,
        target_id: str,
    ):
        """Log successful relationship resolution."""
        logger.debug(
            "%s %s linked to %s: target=%s | job=%s",
            entity_name(source_entity), source_id, target_entity, target_id, self.job,
        )
        self.session.stats.relationships_resolved += 1

    def log_relationship_failed(
        self,
        source_entity: str,
        source_id: str,
        target_entity: str,
        lookup_key: str,
        reason: str,
    ):
        """Log failed relationship resolution."""
        logger.warning(
            msg(
                f"{entity_name(source_entity)} {source_id} not linked to {target_entity}",
                {"job": self.job},
                key=lookup_key,
                reason=_clip(reason),
            )
        )
        self.session.stats.relationships_failed += 1

    def log_orphan_contact(self, contact_id: str, workplace_eid: str):
        """Log contact without valid account link."""
        logger.warning(
            msg(f"Contact {contact_id} has no account", {"job": self.job}, workplace=workplace_eid)
        )
        self.session.stats.orphan_contacts += 1

    def log_progress(self, entity_type: str, current: int, total: int):
        """Log progress through a phase, once per batch."""
        pct = (current / total * 100) if total > 0 else 0
        logger.info(
            msg(
                f"{phase_name(entity_type)} progress",
                {"job": self.job},
                done=current,
                total=total,
                pct=pct,
            )
        )

    def phase_started(self, phase: str, **context) -> float:
        """Log the start of a phase; return the start time for :meth:`phase_finished`."""
        name, context = _phase_lead(phase, context)
        logger.info(msg(f"{name} started", {**context, "job": self.job}))
        return time.monotonic()

    def phase_finished(
        self,
        phase: str,
        started: float,
        stats: dict | None = None,
        outcome: str = "ok",
        error: str | None = None,
        **context,
    ) -> None:
        """Log the end of a phase with duration, counts and records per second."""
        seconds = time.monotonic() - started
        counts = {
            key: value
            for key, value in (stats or {}).items()
            if isinstance(value, (int, float)) and not isinstance(value, bool)
        }
        processed = counts.get("processed")
        name, context = _phase_lead(phase, context)
        if outcome == "ok":
            lead = f"{name} done in {seconds:.1f}s"
        elif outcome == "error":
            lead = f"{name} failed after {seconds:.1f}s"
        else:
            lead = f"{name} {outcome} after {seconds:.1f}s"
        logger.log(
            logging.INFO if outcome == "ok" else logging.WARNING,
            msg(
                lead,
                {**context, "job": self.job},
                error=error,
                **counts,
                per_s=round(processed / seconds) if processed and seconds > 0 else None,
            ),
        )


def setup_logging(
    level: int = logging.INFO,
    log_dir: Path = None,
    include_console: bool = True,
):
    """Configure logging for the integration.
    
    Args:
        level: Logging level (default INFO)
        log_dir: Directory for log files (default: SYNC_LOG_DIR, else logs/)
        include_console: Whether to also log to console
    """
    log_dir = Path(log_dir) if log_dir else env_log_dir()

    # Configure root logger
    root = logging.getLogger()
    root.setLevel(level)
    
    # Console handler
    if include_console:
        console = logging.StreamHandler()
        console.setLevel(level)
        console.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)-7s | %(message)s",
            datefmt="%H:%M:%S"
        ))
        root.addHandler(console)
    
    # Log to file unless SYNC_LOG_DIR is off or the directory is not writable
    log_file = None
    if log_dir is not None:
        log_file = log_dir / f"integration_{datetime.now().strftime('%Y%m%d')}.log"
        file_handler = _open_file_handler(log_file)
        if file_handler is None:
            log_file = None
        else:
            root.addHandler(file_handler)

    logger.info(
        msg("Logging configured", level=logging.getLevelName(level), file=log_file or "off")
    )
