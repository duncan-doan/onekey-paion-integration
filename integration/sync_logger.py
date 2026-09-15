"""Comprehensive logging for OneKey to Dataverse synchronization.

Provides structured logging for:
- Sync operations (start, progress, completion)
- Record-level success/failure
- Validation errors
- Relationship resolution
- Performance metrics
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

if TYPE_CHECKING:
    from .validation import ValidationResult

logger = logging.getLogger(__name__)

# Default log directory
LOG_DIR = Path(__file__).resolve().parent.parent / "logs"


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
            "record_count": len(self.records),
            "error_count": len(self.errors),
        }


class SyncLogger:
    """Structured logger for sync operations."""
    
    def __init__(self, session_id: str = None, log_dir: Path = None):
        self.session_id = session_id or datetime.now().strftime("%Y%m%d_%H%M%S")
        self.log_dir = Path(log_dir or LOG_DIR)
        self.log_dir.mkdir(parents=True, exist_ok=True)
        
        self.session = SyncSession(
            session_id=self.session_id,
            sync_type="unknown",
        )
        self._start_time = None
        self._operation_times: dict[str, list[float]] = defaultdict(list)
        
        # Set up file logging
        self._setup_file_logging()
    
    def _setup_file_logging(self):
        """Configure file handler for this session."""
        log_file = self.log_dir / f"sync_{self.session_id}.log"
        file_handler = logging.FileHandler(log_file, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
        ))
        
        # Add to root logger
        logging.getLogger().addHandler(file_handler)
        logger.info("Sync session %s logging to %s", self.session_id, log_file)
    
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
            "=" * 60 + "\n"
            "SYNC STARTED\n"
            "  Session:    %s\n"
            "  Type:       %s\n"
            "  Codebases:  %s\n"
            "  Date range: %s to %s\n" +
            "=" * 60,
            self.session_id,
            sync_type,
            ", ".join(codebases),
            date_from or "N/A",
            date_to or "N/A",
        )
    
    def end_sync(self, success: bool = True):
        """Log sync completion and write summary."""
        self.session.stats.completed_at = datetime.now().isoformat()
        if self._start_time:
            self.session.stats.duration_seconds = time.time() - self._start_time
        
        status = "COMPLETED" if success else "FAILED"
        stats = self.session.stats
        
        logger.info(
            "=" * 60 + "\n"
            "SYNC %s\n"
            "  Duration:         %.2f seconds\n"
            "  Accounts:         %d processed (%d created, %d updated, %d failed)\n"
            "  Contacts:         %d processed (%d created, %d updated, %d failed)\n"
            "  Orphan contacts:  %d\n"
            "  Validation:       %d errors, %d warnings\n"
            "  Relationships:    %d resolved, %d failed\n" +
            "=" * 60,
            status,
            stats.duration_seconds,
            stats.accounts_processed,
            stats.accounts_created,
            stats.accounts_updated,
            stats.accounts_failed,
            stats.contacts_processed,
            stats.contacts_created,
            stats.contacts_updated,
            stats.contacts_failed,
            stats.orphan_contacts,
            stats.validation_errors,
            stats.validation_warnings,
            stats.relationships_resolved,
            stats.relationships_failed,
        )
        
        # Write session summary to JSON
        self._write_summary()
    
    def _write_summary(self):
        """Write session summary to JSON file."""
        summary_file = self.log_dir / f"sync_{self.session_id}_summary.json"
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump(self.session.to_dict(), f, indent=2)
        logger.info("Sync summary written to %s", summary_file)
    
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
    
    def log_record_success(
        self,
        entity_type: str,
        source_id: str,
        target_id: str,
        operation: str,
        message: str = None,
    ):
        """Log successful record operation."""
        logger.info(
            "[%s] %s %s -> %s%s",
            operation.upper(),
            entity_type,
            source_id,
            target_id,
            f" ({message})" if message else "",
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
            "[%s FAILED] %s %s: %s",
            operation.upper(),
            entity_type,
            source_id,
            error,
        )
        self.session.errors.append(f"{entity_type} {source_id}: {error}")
    
    def log_validation_result(self, validation: "ValidationResult", context: str = ""):
        """Log validation results."""
        for error in validation.errors:
            logger.error("[VALIDATION] %s%s", f"{context}: " if context else "", error)
            self.session.stats.validation_errors += 1
        
        for warning in validation.warnings:
            logger.warning("[VALIDATION] %s%s", f"{context}: " if context else "", warning)
            self.session.stats.validation_warnings += 1
    
    def log_relationship_resolved(
        self,
        source_entity: str,
        source_id: str,
        target_entity: str,
        target_id: str,
    ):
        """Log successful relationship resolution."""
        logger.debug(
            "[RELATIONSHIP] %s %s -> %s %s",
            source_entity, source_id, target_entity, target_id
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
            "[RELATIONSHIP FAILED] %s %s -> %s (key=%s): %s",
            source_entity, source_id, target_entity, lookup_key, reason
        )
        self.session.stats.relationships_failed += 1
    
    def log_orphan_contact(self, contact_id: str, workplace_eid: str):
        """Log contact without valid account link."""
        logger.warning(
            "[ORPHAN] Contact %s has no valid account for workplace %s",
            contact_id, workplace_eid
        )
        self.session.stats.orphan_contacts += 1
    
    def log_progress(self, entity_type: str, current: int, total: int):
        """Log sync progress."""
        pct = (current / total * 100) if total > 0 else 0
        logger.info(
            "[PROGRESS] %s: %d/%d (%.1f%%)",
            entity_type, current, total, pct
        )
    
    def log_phase(self, phase: str, message: str = ""):
        """Log sync phase transition."""
        logger.info("-" * 40)
        logger.info("[PHASE] %s%s", phase, f": {message}" if message else "")
        logger.info("-" * 40)


def setup_logging(
    level: int = logging.INFO,
    log_dir: Path = None,
    include_console: bool = True,
):
    """Configure logging for the integration.
    
    Args:
        level: Logging level (default INFO)
        log_dir: Directory for log files
        include_console: Whether to also log to console
    """
    log_dir = Path(log_dir or LOG_DIR)
    log_dir.mkdir(parents=True, exist_ok=True)
    
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
    
    # Always log to file
    log_file = log_dir / f"integration_{datetime.now().strftime('%Y%m%d')}.log"
    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(logging.Formatter(
        "%(asctime)s | %(levelname)-7s | %(name)s | %(message)s"
    ))
    root.addHandler(file_handler)
    
    logger.info("Logging configured: level=%s, file=%s", logging.getLevelName(level), log_file)
