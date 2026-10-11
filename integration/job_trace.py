"""Dataverse trace for an integration sync job.

One ``ts_integrationjob`` row is created when a job starts and updated after
each phase. Failed source records are buffered in memory and written to
``ts_integrationfailure`` in batches, so tracing does not sit on the
per-record write path.

Trace writes never raise into the sync. A Dataverse failure while recording
the job is logged and the sync continues. Local sync logs stay the fallback.
"""

from __future__ import annotations

import json
import logging
import time
import uuid
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from dataverse.client import chunked, escape_odata
from dataverse.exceptions import DataverseAmbiguousWriteError, DataverseHTTPError

if TYPE_CHECKING:
    from dataverse import DataverseClient

logger = logging.getLogger(__name__)

JOBS = "ts_integrationjobs"
FAILURES = "ts_integrationfailures"

# Choice values are the ones created on paion-crm-test (publisher prefix 12687).
JOB_FULL = 126870001
JOB_DELTA = 126870002
JOB_AFFILIATIONS = 126870003
JOB_HCO = 126870004
JOB_HCP = 126870005
JOB_ADDRESSES = 126870006
JOB_HIERARCHY = 126870007

JOB_LABELS = {
    JOB_FULL: "Full",
    JOB_DELTA: "Delta",
    JOB_AFFILIATIONS: "Affiliations Only",
    JOB_HCO: "HCO Only",
    JOB_HCP: "HCP Only",
    JOB_ADDRESSES: "Addresses Only",
    JOB_HIERARCHY: "Hierarchy Only",
}

STATUS_RUNNING = 126870001
STATUS_SUCCEEDED = 126870002
STATUS_PARTIAL = 126870003
STATUS_FAILED = 126870004
STATUS_ABANDONED = 126870005

STATUS_LABELS = {
    STATUS_RUNNING: "Running",
    STATUS_SUCCEEDED: "Succeeded",
    STATUS_PARTIAL: "Partially Succeeded",
    STATUS_FAILED: "Failed",
    STATUS_ABANDONED: "Abandoned",
}

TRIGGER_MANUAL = 126870001
TRIGGER_TIMER = 126870002
TRIGGER_DAEMON = 126870003

TRIGGER_BY_NAME = {
    "manual": TRIGGER_MANUAL,
    "timer": TRIGGER_TIMER,
    "daemon": TRIGGER_DAEMON,
}

PHASE_PICKLISTS = 126870001
PHASE_HCO = 126870002
PHASE_ADDRESSES = 126870003
PHASE_HIERARCHY = 126870004
# The live phase choice has no HCO Relations value. HCO–HCO rows use HCO.
PHASE_HCO_RELATIONS = 126870002
PHASE_HCP = 126870005
PHASE_HCP_ACTIVITY = 126870006

PHASE_LABELS = {
    PHASE_PICKLISTS: "Picklists",
    PHASE_HCO: "HCO",
    PHASE_ADDRESSES: "Addresses",
    PHASE_HIERARCHY: "Hierarchy",
    PHASE_HCP: "HCP",
    PHASE_HCP_ACTIVITY: "HCP Activity",
}

ENTITY_ACCOUNT = 126870001
ENTITY_CONTACT = 126870002
ENTITY_ADDRESS = 126870003
ENTITY_AFFILIATION = 126870004

ENTITY_BY_NAME = {
    "account": ENTITY_ACCOUNT,
    "contact": ENTITY_CONTACT,
    "customeraddress": ENTITY_ADDRESS,
    "ts_affiliation": ENTITY_AFFILIATION,
}

ENTITY_LABELS = {
    ENTITY_ACCOUNT: "account",
    ENTITY_CONTACT: "contact",
    ENTITY_ADDRESS: "customeraddress",
    ENTITY_AFFILIATION: "affiliation",
}

OP_VALIDATE = 126870001
OP_CREATE = 126870002
OP_UPDATE = 126870003
OP_UPSERT = 126870004
OP_RELATE = 126870005

OP_BY_NAME = {
    "validate": OP_VALIDATE,
    "create": OP_CREATE,
    "update": OP_UPDATE,
    "upsert": OP_UPSERT,
    "bulk_upsert": OP_UPSERT,
    "transform": OP_VALIDATE,
    "relate": OP_RELATE,
}

OP_LABELS = {
    OP_VALIDATE: "Validate",
    OP_CREATE: "Create",
    OP_UPDATE: "Update",
    OP_UPSERT: "Upsert",
    OP_RELATE: "Relate",
}

CAT_VALIDATION = 126870001
CAT_MAPPING = 126870002
CAT_LOOKUP = 126870003
CAT_TIMEOUT = 126870004
CAT_UNKNOWN = 126870005
# This org has no Dataverse error-category choice. HTTP errors use Unknown
# and keep the status on ts_httpstatus. Timeouts still use CAT_TIMEOUT.
CAT_DATAVERSE = CAT_UNKNOWN

CAT_BY_NAME = {
    "validation": CAT_VALIDATION,
    "mapping": CAT_MAPPING,
    "lookup": CAT_LOOKUP,
    "dataverse": CAT_DATAVERSE,
    "timeout": CAT_TIMEOUT,
    "unknown": CAT_UNKNOWN,
}

RETRY_PENDING = 126870001
RETRY_RETRIED = 126870002
RETRY_RESOLVED = 126870003
RETRY_IGNORED = 126870004
MAX_RETRY_ATTEMPTS = 5

_RETRYABLE_HTTP = frozenset({429, 500, 502, 503, 504})
_ENTITY_COUNT_KEYS = (
    "accounts",
    "addresses",
    "affiliations",
    "activity_affiliations",
    "contacts",
)


def clip(value, limit: int) -> str:
    text = "" if value is None else str(value).replace("\r", " ").replace("\n", " ")
    text = " ".join(text.split())
    if len(text) <= limit:
        return text
    if limit <= 3:
        return text[:limit]
    return text[: limit - 3] + "..."


def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def as_dataverse_datetime(value: str | None) -> str | None:
    if not value:
        return None
    text = str(value).strip()
    if not text:
        return None
    if "T" not in text:
        return f"{text}T00:00:00Z"
    if text.endswith("Z") or "+" in text[10:]:
        return text
    return text + "Z"


def job_codebase(codebases: list[str]) -> str:
    """Return the ts_codebase value stored for a job over these codebases."""
    codes = [c.strip().upper() for c in codebases if c and str(c).strip()]
    joined = ",".join(codes) or "NA"
    return joined if len(joined) <= 10 else (codes[0][:10] if codes else "NA")


def summarize_results(results: dict | None) -> dict:
    """Roll phase stats into the job-level counters."""
    totals = {
        "processed": 0,
        "created": 0,
        "updated": 0,
        "skipped": 0,
        "failed": 0,
    }
    if not results:
        return totals

    for key in _ENTITY_COUNT_KEYS:
        stats = results.get(key) or {}
        totals["processed"] += int(stats.get("processed") or 0)
        totals["created"] += int(stats.get("created") or 0)
        totals["updated"] += int(stats.get("updated") or 0)
        totals["skipped"] += int(stats.get("skipped") or 0)
        totals["failed"] += int(stats.get("failed") or 0)

    hierarchy = results.get("hierarchy") or {}
    linked = int(hierarchy.get("linked") or 0)
    totals["processed"] += linked
    totals["updated"] += linked
    totals["skipped"] += int(hierarchy.get("already_linked") or 0)
    totals["failed"] += int(hierarchy.get("failed") or 0)
    totals["failed"] += int(hierarchy.get("missing_child") or 0)
    totals["failed"] += int(hierarchy.get("missing_parent") or 0)
    return totals


def phase_counts_json(results: dict | None) -> str | None:
    if not results:
        return None
    payload = {}
    for key in _ENTITY_COUNT_KEYS:
        if key in results and results[key]:
            payload[key] = {
                name: int((results[key] or {}).get(name) or 0)
                for name in ("processed", "created", "updated", "skipped", "failed")
            }
    if results.get("hierarchy"):
        payload["hierarchy"] = {
            name: int((results["hierarchy"] or {}).get(name) or 0)
            for name in (
                "edges",
                "linked",
                "failed",
                "already_linked",
                "missing_child",
                "missing_parent",
            )
        }
    if "picklists" in results:
        payload["picklists"] = len(results.get("picklists") or [])
    if results.get("by_process_code"):
        payload["by_process_code"] = results["by_process_code"]
    if "picklist_gaps" in results:
        payload["picklist_gaps"] = int(results.get("picklist_gaps") or 0)
    if not payload:
        return None
    return clip(json.dumps(payload, separators=(",", ":"), default=str), 10000)


def error_summary(results: dict | None, aborted: bool = False) -> str | None:
    parts: list[str] = []
    if results:
        validation = results.get("validation")
        if validation:
            parts.append(str(validation))
        for err in results.get("errors") or []:
            text = str(err).strip()
            if text and text not in parts:
                parts.append(text)
    if aborted and not parts:
        parts.append("Sync stopped before completion")
    if not parts:
        return None
    return clip(" | ".join(parts), 2000)


def trace_status(results: dict | None, *, aborted: bool, failure_rows: int) -> int:
    """Map a finished sync onto ts_status.

    Succeeded: the job completed and nothing failed.
    Partially Succeeded: the job wrote some data, or finished with record failures.
    Failed: the job stopped before any data was written.
    """
    stats = summarize_results(results)
    progress = stats["processed"] + stats["created"] + stats["updated"] + stats["failed"]
    progress += stats["skipped"] + failure_rows
    errors = []
    if results:
        errors.extend(results.get("errors") or [])
        if results.get("validation"):
            errors.append(results["validation"])
    failed = stats["failed"] or failure_rows
    if aborted:
        return STATUS_PARTIAL if progress else STATUS_FAILED
    if not results or not results.get("success"):
        return STATUS_PARTIAL if progress or failed else STATUS_FAILED
    if failed or errors:
        return STATUS_PARTIAL
    return STATUS_SUCCEEDED


def classify_failure(exc: BaseException | None, category: str | None, retryable: bool | None):
    """Return (category value, retryable, http status, error code)."""
    status = getattr(exc, "status_code", None)
    code = getattr(exc, "code", None)
    text = str(exc).lower() if exc else ""
    if category:
        cat = CAT_BY_NAME.get(category, CAT_UNKNOWN)
    elif status == 504 or "timed out" in text or "timeout" in text:
        cat = CAT_TIMEOUT
    elif isinstance(exc, DataverseHTTPError) or (status is not None and status >= 400):
        cat = CAT_DATAVERSE
    else:
        cat = CAT_UNKNOWN

    if retryable is None:
        if cat in (CAT_VALIDATION, CAT_MAPPING, CAT_LOOKUP):
            retryable = False
        elif cat == CAT_TIMEOUT or status in _RETRYABLE_HTTP:
            retryable = True
        elif isinstance(exc, DataverseHTTPError) and status is None:
            retryable = True
        else:
            retryable = False
    return cat, bool(retryable), status, code


# A running job refreshes ts_lastheartbeat at most this often, after any
# OneKey or Dataverse request and before each OneKey retry wait (see
# JobTracer.beat). A Running job whose heartbeat is several intervals old has
# stopped.
HEARTBEAT_EVERY_SECONDS = 300


class JobTracer:
    """Writes ``ts_integrationjob`` and ``ts_integrationfailure`` rows."""

    def __init__(
        self,
        dataverse: "DataverseClient | None",
        *,
        trigger: int = TRIGGER_MANUAL,
        retry_of_job_id: str | None = None,
        batch_size: int = 100,
    ):
        self.dataverse = dataverse
        self.trigger = trigger
        self.retry_of_job_id = retry_of_job_id
        self.batch_size = max(1, int(batch_size or 100))
        self._can_trace = dataverse is not None and hasattr(dataverse, "create")
        self.enabled = self._can_trace
        self.job_id: str | None = None
        self.job_guid: str | None = None
        self._started_at: datetime | None = None
        self._pending: dict[str, dict] = {}
        self._seen: set[str] = set()
        self.written_failures = 0
        self._finished = False
        self.persist_failures = True
        self.recent_failures: list[dict] = []
        self._last_beat: float | None = None
        self._beating = False

    def begin(
        self,
        *,
        job_type: int,
        codebases: list[str],
        dry_run: bool = False,
        date_from: str | None = None,
        date_to: str | None = None,
    ) -> None:
        """Create the job row. Failures here disable tracing for this run."""
        self._finished = False
        self._pending.clear()
        self._seen.clear()
        self.recent_failures.clear()
        self.written_failures = 0
        self.job_guid = None
        self.enabled = self._can_trace and not dry_run
        self.job_id = str(uuid.uuid4())
        self._started_at = datetime.now(timezone.utc)
        self._last_beat = None
        if not self.enabled:
            logger.info("Integration job trace skipped (dry-run or no Dataverse writes)")
            return

        try:
            payload = self._job_payload(
                job_type=job_type,
                codebases=codebases,
                date_from=date_from,
                date_to=date_to,
                status=STATUS_RUNNING,
            )
            self.job_guid = self.dataverse.create(JOBS, payload)
            self._last_beat = time.monotonic()
            logger.info(
                "Integration job %s started (%s)",
                self.job_id,
                self.job_guid,
            )
        except Exception:
            logger.warning(
                "Could not create integration job %s; sync will continue without Dataverse trace",
                self.job_id,
                exc_info=True,
            )
            self.enabled = False
            self.job_guid = None

    def record_failure(
        self,
        *,
        phase: int,
        entity: str,
        source_id: str | None,
        operation: str,
        message: str,
        external_id: str | None = None,
        category: str | None = None,
        retryable: bool | None = None,
        exc: BaseException | None = None,
    ) -> None:
        """Buffer one failed source record. Nothing is written until flush.

        ``recent_failures`` is filled even when the job row was not created,
        so a retry can tell which source IDs failed again.
        """
        try:
            entity_value = ENTITY_BY_NAME.get(entity, ENTITY_ACCOUNT)
            operation_value = OP_BY_NAME.get(operation, OP_UPSERT)
            cat, is_retryable, http_status, error_code = classify_failure(
                exc, category, retryable
            )
            source = clip(source_id or "unknown", 100)
            external = clip(external_id, 200) if external_id else ""
            self.recent_failures.append(
                {
                    "source_id": source,
                    "external_id": external,
                    "retryable": is_retryable,
                    "message": clip(message or "Unknown error", 2000),
                }
            )
            if (
                not self.persist_failures
                or not self.enabled
                or not self.job_guid
                or not self.job_id
            ):
                return
            phase_label = PHASE_LABELS.get(phase, str(phase))
            op_label = OP_LABELS.get(operation_value, str(operation_value))
            failure_key = clip(
                "|".join(
                    part.replace("|", "/")
                    for part in (self.job_id, phase_label, source, op_label)
                ),
                300,
            )
            if failure_key in self._seen:
                return
            self._seen.add(failure_key)
            entity_label = ENTITY_LABELS.get(entity_value, entity)
            row = {
                "ts_name": clip(f"{entity_label} {source}", 250),
                "ts_failurekey": failure_key,
                "ts_IntegrationJob@odata.bind": f"/{JOBS}({self.job_guid})",
                "ts_sourceid": source,
                "ts_entitytype": entity_value,
                "ts_phase": phase,
                "ts_operation": operation_value,
                "ts_errorcategory": cat,
                "ts_errormessage": clip(message or "Unknown error", 2000),
                "ts_isretryable": is_retryable,
                "ts_retrystatus": RETRY_PENDING,
                "ts_attemptcount": 1,
                "ts_occurredon": utc_now(),
            }
            external = clip(external_id, 200) if external_id else ""
            if external:
                row["ts_externalid"] = external
            if http_status is not None:
                row["ts_httpstatus"] = int(http_status)
            if error_code:
                row["ts_errorcode"] = clip(error_code, 100)
            self._pending[failure_key] = row
        except Exception:
            logger.warning("Could not buffer integration failure", exc_info=True)

    def flush(self, heartbeat: bool = False) -> None:
        """Write buffered failure rows. Counts stay unchanged."""
        if not self.enabled or not self.job_guid or self._finished:
            return
        try:
            self._flush()
            if heartbeat:
                self._last_beat = time.monotonic()
                self.dataverse.update(
                    JOBS,
                    self.job_guid,
                    {"ts_lastheartbeat": utc_now()},
                )
        except Exception:
            logger.warning(
                "Could not flush integration failures for job %s",
                self.job_id,
                exc_info=True,
            )

    def beat(self) -> None:
        """Refresh ts_lastheartbeat if HEARTBEAT_EVERY_SECONDS have passed.

        Called after every OneKey and Dataverse request, and before each OneKey
        retry wait, while a job runs, so a live job's heartbeat stays fresh
        inside long phases and long throttling backoffs. The update is a
        Dataverse request itself; ``_beating`` stops it from beating again.
        """
        if self._beating or not self.enabled or not self.job_guid or self._finished:
            return
        now = time.monotonic()
        if self._last_beat is not None and now - self._last_beat < HEARTBEAT_EVERY_SECONDS:
            return
        self._last_beat = now
        self._beating = True
        try:
            self.dataverse.update(JOBS, self.job_guid, {"ts_lastheartbeat": utc_now()})
        except Exception:
            logger.warning(
                "Could not refresh the heartbeat of job %s", self.job_id, exc_info=True
            )
        finally:
            self._beating = False

    def checkpoint(self, results: dict | None) -> None:
        """Flush buffered failures and refresh counts while the job is running."""
        if not self.enabled or not self.job_guid or self._finished:
            return
        try:
            self._flush()
            self._last_beat = time.monotonic()
            self.dataverse.update(JOBS, self.job_guid, self._progress_payload(results))
        except Exception:
            logger.warning(
                "Could not update integration job %s",
                self.job_id,
                exc_info=True,
            )

    def finish(self, results: dict | None, aborted: bool = False) -> None:
        """Flush remaining failures and set the terminal job status."""
        if self._finished:
            return
        self._finished = True
        if not self.enabled or not self.job_guid:
            return
        try:
            self._flush()
            status = trace_status(
                results, aborted=aborted, failure_rows=self.written_failures
            )
            payload = self._progress_payload(results)
            payload["ts_status"] = status
            payload["ts_completedon"] = utc_now()
            payload["ts_durationseconds"] = self._duration_seconds()
            summary = error_summary(results, aborted=aborted)
            if summary:
                payload["ts_errorsummary"] = summary
            self.dataverse.update(JOBS, self.job_guid, payload)
            if isinstance(results, dict):
                results["job_id"] = self.job_id
                results["trace_status"] = STATUS_LABELS.get(status, str(status))
                results["trace_failures"] = self.written_failures
            logger.info(
                "Integration job %s %s (%d failure row(s))",
                self.job_id,
                STATUS_LABELS.get(status, status),
                self.written_failures,
            )
        except Exception:
            logger.warning(
                "Could not finish integration job %s",
                self.job_id,
                exc_info=True,
            )

    def _flush(self) -> None:
        rows = list(self._pending.values())
        self._pending.clear()
        if not rows:
            return
        create_multiple = getattr(self.dataverse, "create_multiple", None)
        for batch in chunked(rows, self.batch_size):
            try:
                if create_multiple is None:
                    raise AttributeError("create_multiple is not available")
                create_multiple(
                    FAILURES, batch, logical_name="ts_integrationfailure"
                )
                self.written_failures += len(batch)
            except Exception as exc:
                if isinstance(exc, DataverseAmbiguousWriteError):
                    # The batch may have been created; write only what is missing.
                    missing = self._not_written(batch)
                    self.written_failures += len(batch) - len(missing)
                    batch = missing
                logger.warning(
                    "CreateMultiple failed for %d integration failure(s); writing one by one",
                    len(batch),
                    exc_info=True,
                )
                for row in batch:
                    try:
                        self.dataverse.create(FAILURES, row)
                        self.written_failures += 1
                    except Exception:
                        logger.warning(
                            "Could not write integration failure %s",
                            row.get("ts_failurekey"),
                            exc_info=True,
                        )

    def _not_written(self, rows: list[dict]) -> list[dict]:
        """Rows of an uncertain CreateMultiple that are not in Dataverse.

        ``ts_failurekey`` includes the job ID, so it identifies each row. If the
        check itself fails, all rows are written: a duplicate queue row only
        means one extra retry, a missing one means none.
        """
        find_many = getattr(self.dataverse, "find_many_by_field", None)
        if find_many is None:
            return rows
        try:
            found = find_many(
                FAILURES,
                "ts_failurekey",
                [row.get("ts_failurekey") for row in rows],
                select=["ts_failurekey"],
            )
        except Exception:
            logger.warning(
                "Could not check which integration failures were written", exc_info=True
            )
            return rows
        return [row for row in rows if row.get("ts_failurekey") not in found]

    def _duration_seconds(self) -> int:
        if self._started_at is None:
            return 0
        elapsed = datetime.now(timezone.utc) - self._started_at
        return max(0, int(elapsed.total_seconds()))

    def _progress_payload(self, results: dict | None) -> dict:
        totals = summarize_results(results)
        payload = {
            "ts_processed": totals["processed"],
            "ts_createdcount": totals["created"],
            "ts_updatedcount": totals["updated"],
            "ts_skipped": totals["skipped"],
            "ts_failedcount": totals["failed"],
            "ts_lastheartbeat": utc_now(),
        }
        phases = phase_counts_json(results)
        if phases:
            payload["ts_phasecounts"] = phases
        summary = error_summary(results)
        if summary:
            payload["ts_errorsummary"] = summary
        return payload

    def _job_payload(
        self,
        *,
        job_type: int,
        codebases: list[str],
        date_from: str | None,
        date_to: str | None,
        status: int,
    ) -> dict:
        codes = [c.strip().upper() for c in codebases if c and str(c).strip()]
        joined = ",".join(codes) or "NA"
        codebase = job_codebase(codebases)
        label = JOB_LABELS.get(job_type, "Sync")
        started = self._started_at or datetime.now(timezone.utc)
        payload = {
            "ts_name": clip(
                f"{joined} {label} {started.strftime('%Y-%m-%d %H:%M')}", 200
            ),
            "ts_jobid": self.job_id,
            "ts_jobtype": int(job_type),
            "ts_codebase": codebase,
            "ts_status": int(status),
            "ts_startedon": utc_now(),
            "ts_dryrun": False,
            "ts_processed": 0,
            "ts_createdcount": 0,
            "ts_updatedcount": 0,
            "ts_skipped": 0,
            "ts_failedcount": 0,
            "ts_lastheartbeat": utc_now(),
            "ts_trigger": int(self.trigger or TRIGGER_MANUAL),
        }
        watermark_from = as_dataverse_datetime(date_from)
        watermark_to = as_dataverse_datetime(date_to)
        if watermark_from:
            payload["ts_watermarkfrom"] = watermark_from
        if watermark_to:
            payload["ts_watermarkto"] = watermark_to
        retry_bind = self._retry_bind()
        if retry_bind:
            payload["ts_Retryofjob@odata.bind"] = retry_bind
        return payload

    def _retry_bind(self) -> str | None:
        job_key = (self.retry_of_job_id or "").strip()
        if not job_key or self.dataverse is None:
            return None
        find_one = getattr(self.dataverse, "find_one", None)
        if find_one is None:
            return None
        try:
            row = find_one(
                JOBS,
                filter=f"ts_jobid eq '{escape_odata(job_key)}'",
                select=["ts_integrationjobid"],
            )
        except Exception:
            logger.warning(
                "Could not resolve retry job %s", job_key, exc_info=True
            )
            return None
        if not row:
            logger.warning("Retry job %s was not found; leaving ts_retryofjob empty", job_key)
            return None
        guid = row.get("ts_integrationjobid")
        if not guid:
            return None
        return f"/{JOBS}({guid})"
