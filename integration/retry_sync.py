"""Retry OneKey records listed on ``ts_integrationfailure``.

The scheduled sync runs this before the delta pull. Delta only returns records
that changed inside the watermark window, so an ID that failed and then stays
unchanged is stored here and replayed through the normal transform and upsert
path.

Each original failure row stays the queue item. A retry does not insert a
second queue row. The row is marked Retried while it is in progress, then
Resolved, Pending, or Ignored.
"""

from __future__ import annotations

import logging
from collections import defaultdict

from dataverse.client import escape_odata
from integration.job_trace import (
    ENTITY_ACCOUNT,
    ENTITY_ADDRESS,
    ENTITY_AFFILIATION,
    ENTITY_CONTACT,
    FAILURES,
    JOB_ADDRESSES,
    JOB_AFFILIATIONS,
    JOB_FULL,
    JOB_HCO,
    JOB_HCP,
    JOB_HIERARCHY,
    MAX_RETRY_ATTEMPTS,
    PHASE_ADDRESSES,
    PHASE_HCO,
    PHASE_HCP,
    PHASE_HCP_ACTIVITY,
    PHASE_HIERARCHY,
    RETRY_IGNORED,
    RETRY_PENDING,
    RETRY_RESOLVED,
    RETRY_RETRIED,
    STATUS_RUNNING,
    TRIGGER_MANUAL,
    clip,
)
from onekey import ACTIVITY, EXACT, INDIVIDUAL, WORKPLACE, search_field
from onekey.parse import compose_hcp_result, flatten_hco

logger = logging.getLogger(__name__)

# Account, address, hierarchy, HCO affiliation, contact, then activity affiliation.
PHASE_STEPS = (
    ("accounts", PHASE_HCO, ENTITY_ACCOUNT),
    ("addresses", PHASE_ADDRESSES, ENTITY_ADDRESS),
    ("hierarchy", PHASE_HIERARCHY, ENTITY_ACCOUNT),
    ("affiliations", PHASE_HCO, ENTITY_AFFILIATION),
    ("contacts", PHASE_HCP, ENTITY_CONTACT),
    ("activity_affiliations", PHASE_HCP_ACTIVITY, ENTITY_AFFILIATION),
)


def pending_filter(
    *,
    codebase: str | None = None,
    job_id: str | None = None,
    source_ids: list[str] | None = None,
    max_attempts: int = MAX_RETRY_ATTEMPTS,
) -> str:
    """OData filter for retryable failures whose parent job is not running."""
    parts = [
        f"ts_retrystatus eq {RETRY_PENDING}",
        "ts_isretryable eq true",
        f"ts_attemptcount lt {int(max_attempts)}",
        f"ts_IntegrationJob/ts_status ne {STATUS_RUNNING}",
    ]
    if codebase:
        parts.append(
            f"ts_IntegrationJob/ts_codebase eq '{escape_odata(codebase.upper())}'"
        )
    if job_id:
        parts.append(f"ts_IntegrationJob/ts_jobid eq '{escape_odata(job_id)}'")
    if source_ids:
        quoted = " or ".join(
            f"ts_sourceid eq '{escape_odata(source_id)}'"
            for source_id in source_ids
            if source_id
        )
        if quoted:
            parts.append(f"({quoted})")
    return " and ".join(parts)


def fetch_pending_failures(
    dataverse,
    *,
    codebase: str | None = None,
    job_id: str | None = None,
    source_ids: list[str] | None = None,
    limit: int = 100,
) -> list[dict]:
    """Return normalized pending failure rows, oldest first."""
    rows = dataverse.retrieve_multiple(
        FAILURES,
        select=[
            "ts_integrationfailureid",
            "ts_sourceid",
            "ts_externalid",
            "ts_phase",
            "ts_entitytype",
            "ts_attemptcount",
            "ts_errormessage",
            "ts_isretryable",
        ],
        filter=pending_filter(
            codebase=codebase, job_id=job_id, source_ids=source_ids
        ),
        order_by="ts_occurredon asc",
        expand=(
            "ts_IntegrationJob($select=ts_integrationjobid,ts_jobid,"
            "ts_codebase,ts_status)"
        ),
        max_records=limit,
    )
    return [_normalize_failure(row) for row in rows if row.get("ts_sourceid")]


def retry_pending_failures(
    engine,
    *,
    codebase: str | None = None,
    job_id: str | None = None,
    source_ids: list[str] | None = None,
    limit: int = 100,
    dry_run: bool = False,
    trigger: int = TRIGGER_MANUAL,
    rows: list[dict] | None = None,
) -> dict:
    """Claim pending failures, replay them, and update each queue row.

    ``rows`` skips the Dataverse query. Tests pass already-normalized rows.
    """
    pending = rows if rows is not None else fetch_pending_failures(
        engine.dataverse,
        codebase=codebase,
        job_id=job_id,
        source_ids=source_ids,
        limit=limit,
    )
    summary = {
        "success": True,
        "selected": len(pending),
        "resolved": 0,
        "pending": 0,
        "ignored": 0,
        "jobs": [],
        "errors": [],
    }
    if not pending:
        logger.info("No pending retryable integration failures")
        return summary

    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in pending:
        grouped[(row["parent_job_id"], row["codebase"])].append(row)

    previous_persist = engine.tracer.persist_failures
    engine.tracer.persist_failures = False
    try:
        for (parent_job_id, group_code), group_rows in grouped.items():
            job_summary = _retry_group(
                engine,
                parent_job_id=parent_job_id,
                codebase=group_code,
                rows=group_rows,
                dry_run=dry_run,
                trigger=trigger,
            )
            summary["resolved"] += job_summary["resolved"]
            summary["pending"] += job_summary["pending"]
            summary["ignored"] += job_summary["ignored"]
            if job_summary.get("job_id"):
                summary["jobs"].append(job_summary["job_id"])
            summary["errors"].extend(job_summary.get("errors") or [])
            if not job_summary["success"]:
                summary["success"] = False
    finally:
        engine.tracer.persist_failures = previous_persist
    return summary


def resolve_pending_for_ids(dataverse, source_ids: set[str] | list[str]) -> int:
    """Mark pending failures Resolved when a later delta upserts those IDs."""
    ids = [str(source_id) for source_id in source_ids if source_id]
    if not ids:
        return 0
    resolved = 0
    for start in range(0, len(ids), 20):
        chunk = ids[start:start + 20]
        quoted = " or ".join(
            f"ts_sourceid eq '{escape_odata(source_id)}'" for source_id in chunk
        )
        rows = dataverse.retrieve_multiple(
            FAILURES,
            select=["ts_integrationfailureid", "ts_sourceid"],
            filter=f"ts_retrystatus eq {RETRY_PENDING} and ({quoted})",
            top=len(chunk),
        )
        for row in rows:
            dataverse.update(
                FAILURES,
                row["ts_integrationfailureid"],
                {"ts_retrystatus": RETRY_RESOLVED},
            )
            resolved += 1
    if resolved:
        logger.info("Resolved %d pending failure(s) from the delta upsert", resolved)
    return resolved


def _retry_group(
    engine,
    *,
    parent_job_id: str,
    codebase: str,
    rows: list[dict],
    dry_run: bool,
    trigger: int,
) -> dict:
    present = {
        name
        for name, phase, entity in PHASE_STEPS
        if any(row["phase"] == phase and row["entity"] == entity for row in rows)
    }
    result = _empty_retry_result(codebase)
    if not present:
        logger.warning(
            "Skipping %d failure row(s) for job %s with no retry handler",
            len(rows),
            parent_job_id,
        )
        return result
    if dry_run:
        logger.info(
            "DRY RUN retry %d failure(s) for job %s (%s): %s",
            len(rows),
            parent_job_id,
            codebase,
            ", ".join(sorted(present)),
        )
        return result

    engine.tracer.trigger = trigger
    engine.tracer.retry_of_job_id = parent_job_id or None
    engine._begin_job(_job_type(present), [codebase], dry_run=False)
    try:
        for name, phase, entity in PHASE_STEPS:
            matched = [
                row for row in rows
                if row["phase"] == phase and row["entity"] == entity
            ]
            if not matched:
                continue
            _retry_step(engine, name, codebase, matched, result)
        if result["pending"] or result["ignored"] or result["errors"]:
            result["success"] = False
            if result["pending"] or result["ignored"]:
                result["errors"].append(
                    f"retry resolved={result['resolved']} "
                    f"pending={result['pending']} ignored={result['ignored']}"
                )
        engine._checkpoint(result)
    except Exception as exc:
        result["success"] = False
        result["errors"].append(str(exc))
        logger.exception("Retry failed for parent job %s", parent_job_id)
    finally:
        engine._finish_job(result)
        result["job_id"] = engine.tracer.job_id
    return result


def _retry_step(engine, name: str, codebase: str, rows: list[dict], result: dict) -> None:
    claimed: list[dict] = []
    finalized: set[str] = set()
    started = False
    try:
        for row in rows:
            _update_failure(engine.dataverse, row["id"], RETRY_RETRIED)
            claimed.append(row)
        engine.tracer.recent_failures.clear()
        engine._last_step_stats = None
        started = True
        missing = _execute_step(engine, name, codebase, rows)
        _apply_step_stats(engine, name, result)
        failed = list(engine.tracer.recent_failures)
        for source_id in missing:
            failed.append(
                {
                    "source_id": source_id,
                    "external_id": "",
                    "retryable": False,
                    "message": "OneKey record was not found",
                }
            )
        for row in claimed:
            outcome = _outcome(row, _matching_failure(row, failed))
            _update_failure(
                engine.dataverse,
                row["id"],
                outcome["status"],
                attempt=outcome["attempt"],
                message=outcome["message"],
            )
            finalized.add(row["id"])
            result[outcome["bucket"]] += 1
    except Exception as exc:
        result["errors"].append(f"{name}: {exc}")
        result["success"] = False
        logger.exception("Retry step %s failed for %s", name, codebase)
        for row in claimed:
            if row["id"] in finalized:
                continue
            if not started:
                _release_claim(engine.dataverse, row)
                continue
            attempt = row["attempt"] + 1
            status = RETRY_IGNORED if attempt >= MAX_RETRY_ATTEMPTS else RETRY_PENDING
            _update_failure(
                engine.dataverse,
                row["id"],
                status,
                attempt=attempt,
                message=str(exc),
            )
            result["ignored" if status == RETRY_IGNORED else "pending"] += 1


def _execute_step(engine, name: str, codebase: str, rows: list[dict]) -> list[str]:
    if name == "accounts":
        flats, missing = _workplaces(engine, codebase, rows)
        if flats:
            stats, _, _ = engine._sync_hcos(codebase, flattened=flats)
            engine._last_step_stats = ("accounts", stats)
        return missing
    if name == "addresses":
        flats, missing = _workplaces(engine, codebase, rows)
        if flats:
            engine._last_step_stats = (
                "addresses",
                engine._sync_addresses(flats),
            )
        return missing
    if name == "hierarchy":
        flats, missing = _workplaces(engine, codebase, rows)
        if flats:
            allow = set()
            for flat in flats:
                if flat.get("workplaceEid"):
                    allow.add(str(flat["workplaceEid"]))
                if flat.get("parentWorkplaceEid"):
                    allow.add(str(flat["parentWorkplaceEid"]))
            engine._last_step_stats = (
                "hierarchy",
                engine._sync_hierarchy(flats, allowed_workplace_eids=allow),
            )
        return missing
    if name == "affiliations":
        raws, missing = _raw_workplaces(engine, codebase, rows)
        if raws:
            allow = _relation_allow_set(raws)
            engine._contracted_workplace_eids |= allow
            engine._last_step_stats = (
                "affiliations",
                engine._sync_hco_relations(raws, allowed_workplace_eids=allow),
            )
        return missing
    if name == "contacts":
        composed, missing = _composed_hcps(engine, codebase, rows)
        if composed:
            engine._last_step_stats = (
                "contacts",
                engine._sync_hcps(codebase, composed=composed),
            )
        return missing
    if name == "activity_affiliations":
        packets, missing = _activities(engine, codebase, rows)
        if packets:
            engine._last_step_stats = (
                "activity_affiliations",
                engine._sync_activity_affiliations(packets),
            )
        return missing
    return []


def _apply_step_stats(engine, name: str, result: dict) -> None:
    stored = getattr(engine, "_last_step_stats", None)
    engine._last_step_stats = None
    if not stored or stored[0] != name:
        return
    key, stats = stored
    if not isinstance(stats, dict):
        return
    if key == "hierarchy":
        for field, value in stats.items():
            if field in result["hierarchy"]:
                result["hierarchy"][field] += int(value or 0)
        return
    if key in result:
        for field in ("processed", "created", "updated", "failed"):
            result[key][field] += int(stats.get(field) or 0)


def _workplaces(engine, codebase: str, rows: list[dict]) -> tuple[list[dict], list[str]]:
    flats = []
    missing = []
    seen = set()
    for row in rows:
        source_id = row["source_id"]
        if source_id in seen:
            continue
        seen.add(source_id)
        raw = engine.onekey.get_by_id(source_id, WORKPLACE, cod_base=codebase)
        if not raw:
            missing.append(source_id)
            continue
        flats.append(flatten_hco(raw))
    return flats, missing


def _raw_workplaces(engine, codebase: str, rows: list[dict]) -> tuple[list[dict], list[str]]:
    raws = []
    missing = []
    seen = set()
    for row in rows:
        source_id = row["source_id"]
        if source_id in seen:
            continue
        seen.add(source_id)
        raw = engine.onekey.get_by_id(source_id, WORKPLACE, cod_base=codebase)
        if not raw:
            missing.append(source_id)
            continue
        raws.append(raw)
    return raws, missing


def _relation_allow_set(raws: list[dict]) -> set[str]:
    """Both ends of each relation already present on the workplace packet."""
    allow = set()
    for raw in raws:
        workplace = raw.get("workplace") or {}
        owner = workplace.get("workplaceEid") or raw.get("onekeyEid")
        if owner:
            allow.add(str(owner))
        for relation in workplace.get("relations") or []:
            related = relation.get("onekeyEid")
            if related:
                allow.add(str(related))
    return allow


def _composed_hcps(engine, codebase: str, rows: list[dict]) -> tuple[list[dict], list[str]]:
    composed = []
    missing = []
    seen = set()
    for row in rows:
        individual_eid = row["source_id"]
        if individual_eid in seen:
            continue
        seen.add(individual_eid)
        individual = engine.onekey.get_by_id(
            individual_eid, INDIVIDUAL, cod_base=codebase
        )
        activity = _main_activity(engine, codebase, individual_eid)
        workplace_eid = ((activity or {}).get("activity") or {}).get("workplaceEid")
        workplace = (
            engine.onekey.get_by_id(workplace_eid, WORKPLACE, cod_base=codebase)
            if workplace_eid
            else None
        )
        if not individual or not activity or not workplace:
            missing.append(individual_eid)
            continue
        composed.append(compose_hcp_result(individual, activity, workplace))
    return composed, missing


def _main_activity(engine, codebase: str, individual_eid: str) -> dict | None:
    matches = engine.onekey.search(
        ACTIVITY,
        [search_field("activity.individualEid", individual_eid, EXACT)],
        cod_base=codebase,
        result_size=20,
    )
    if not matches:
        return None
    chosen = matches[0]
    for record in matches:
        if (record.get("activity") or {}).get("isMainActivity"):
            chosen = record
            break
    activity_eid = (chosen.get("activity") or {}).get("activityEid")
    if not activity_eid:
        return chosen
    return engine.onekey.get_by_id(activity_eid, ACTIVITY, cod_base=codebase) or chosen


def _activities(engine, codebase: str, rows: list[dict]) -> tuple[list[dict], list[str]]:
    packets = []
    missing = []
    seen = set()
    for row in rows:
        source_id = row["source_id"]
        if source_id in seen:
            continue
        seen.add(source_id)
        raw = engine.onekey.get_by_id(source_id, ACTIVITY, cod_base=codebase)
        if not raw:
            missing.append(source_id)
            continue
        activity = raw.get("activity") or {}
        if activity.get("workplaceEid"):
            engine._contracted_workplace_eids.add(str(activity["workplaceEid"]))
        if activity.get("individualEid"):
            engine._contracted_individual_eids.add(str(activity["individualEid"]))
        packets.append(raw)
    return packets, missing


def _matching_failure(row: dict, failures: list[dict]) -> dict | None:
    source_id = row["source_id"]
    for failure in failures:
        if failure.get("source_id") == source_id:
            return failure
        external = failure.get("external_id") or ""
        if source_id and source_id in {part.strip() for part in external.split("-")}:
            return failure
    return None


def _outcome(row: dict, failure: dict | None) -> dict:
    if failure is None:
        return {
            "status": RETRY_RESOLVED,
            "attempt": row["attempt"],
            "message": None,
            "bucket": "resolved",
        }
    attempt = row["attempt"] + 1
    if failure.get("retryable") and attempt < MAX_RETRY_ATTEMPTS:
        status = RETRY_PENDING
        bucket = "pending"
    else:
        status = RETRY_IGNORED
        bucket = "ignored"
    return {
        "status": status,
        "attempt": attempt,
        "message": failure.get("message"),
        "bucket": bucket,
    }


def _release_claim(dataverse, row: dict) -> None:
    """Put a claimed row back to Pending when the replay never started."""
    try:
        _update_failure(dataverse, row["id"], RETRY_PENDING, attempt=row["attempt"])
    except Exception:
        logger.warning("Could not release retry claim %s", row.get("id"), exc_info=True)


def _update_failure(
    dataverse,
    record_id: str,
    status: int,
    attempt: int | None = None,
    message: str | None = None,
) -> None:
    payload = {"ts_retrystatus": int(status)}
    if attempt is not None:
        payload["ts_attemptcount"] = int(attempt)
    if message:
        payload["ts_errormessage"] = clip(message, 2000)
    dataverse.update(FAILURES, record_id, payload)


def _normalize_failure(row: dict) -> dict:
    parent = row.get("ts_IntegrationJob") or {}
    source_id = str(row.get("ts_sourceid") or "")
    codebase = str(parent.get("ts_codebase") or "").split(",")[0].strip().upper()
    if not codebase:
        codebase = source_id[:3].upper()
    return {
        "id": row.get("ts_integrationfailureid"),
        "source_id": source_id,
        "external_id": row.get("ts_externalid") or "",
        "phase": row.get("ts_phase"),
        "entity": row.get("ts_entitytype"),
        "attempt": int(row.get("ts_attemptcount") or 1),
        "parent_job_id": parent.get("ts_jobid") or "",
        "parent_guid": parent.get("ts_integrationjobid")
        or row.get("_ts_integrationjob_value"),
        "codebase": codebase,
    }


def _job_type(steps: set[str]) -> int:
    if steps == {"accounts"}:
        return JOB_HCO
    if steps == {"addresses"}:
        return JOB_ADDRESSES
    if steps == {"hierarchy"}:
        return JOB_HIERARCHY
    if steps == {"contacts"}:
        return JOB_HCP
    if steps and steps <= {"affiliations", "activity_affiliations"}:
        return JOB_AFFILIATIONS
    return JOB_FULL


def _empty_retry_result(codebase: str) -> dict:
    return {
        "success": True,
        "resolved": 0,
        "pending": 0,
        "ignored": 0,
        "job_id": None,
        "codebases": [codebase],
        "mode": "retry",
        "errors": [],
        "accounts": {"processed": 0, "created": 0, "updated": 0, "failed": 0},
        "addresses": {"processed": 0, "created": 0, "updated": 0, "failed": 0},
        "hierarchy": {
            "edges": 0,
            "linked": 0,
            "failed": 0,
            "already_linked": 0,
            "missing_child": 0,
            "missing_parent": 0,
        },
        "affiliations": {"processed": 0, "created": 0, "updated": 0, "failed": 0},
        "contacts": {"processed": 0, "created": 0, "updated": 0, "failed": 0},
        "activity_affiliations": {
            "processed": 0,
            "created": 0,
            "updated": 0,
            "failed": 0,
        },
    }
