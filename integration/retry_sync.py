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
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone

from dataverse.client import chunked, escape_odata
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

# Above this many upserted IDs, resolve by reading the pending rows once.
_RESOLVE_SCAN_AT = 1000

# A parent job still Running with no heartbeat for this long was killed (timeout,
# crash) and will never finish. The tracer writes a heartbeat after every phase.
STALE_JOB_AFTER = timedelta(hours=12)

# A row left Retried this long was claimed by a run that stopped (killed,
# timed out) before recording the outcome; the next run takes it back.
CLAIM_EXPIRES_AFTER = timedelta(hours=2)

# OneKey IDs start with their codebase: WUKE00001259, WDEH02963729, WFRH00453901.
_CODEBASE_PREFIX = re.compile(r"W[A-Z]{2}")

# Marks a cache miss, since a OneKey record that was not found is cached as None.
_NOT_FETCHED = object()

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
    now: datetime | None = None,
) -> str:
    """OData filter for retryable failures whose parent job is not running.

    Nothing marks a killed job Abandoned, so a parent stuck at Running with a
    heartbeat older than STALE_JOB_AFTER counts as finished; otherwise its
    failures would never be retried. Likewise a row left Retried for
    CLAIM_EXPIRES_AFTER belongs to a retry run that stopped, and is selected
    again.
    """
    now = now or datetime.now(timezone.utc)
    stale = now - STALE_JOB_AFTER
    expired = now - CLAIM_EXPIRES_AFTER
    parts = [
        f"(ts_retrystatus eq {RETRY_PENDING} or (ts_retrystatus eq {RETRY_RETRIED}"
        f" and modifiedon lt {expired.strftime('%Y-%m-%dT%H:%M:%SZ')}))",
        "ts_isretryable eq true",
        f"ts_attemptcount lt {int(max_attempts)}",
        f"(ts_IntegrationJob/ts_status ne {STATUS_RUNNING}"
        f" or ts_IntegrationJob/ts_lastheartbeat lt {stale.strftime('%Y-%m-%dT%H:%M:%SZ')})",
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
            "ts_retrystatus",
        ],
        filter=pending_filter(
            codebase=codebase, job_id=job_id, source_ids=source_ids
        ),
        order_by="ts_occurredon asc",
        expand=(
            "ts_IntegrationJob($select=ts_integrationjobid,ts_jobid,"
            "ts_codebase,ts_status)"
        ),
        # Without $top the first page holds up to 5,000 expanded rows.
        top=limit,
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
    # A row still Retried was claimed by a run that stopped before recording
    # the outcome. Count that run as an attempt, so a record that keeps
    # stopping the worker ends up Ignored instead of being reclaimed forever.
    expired = [row for row in pending if row.get("claim_expired")]
    for row in expired:
        row["attempt"] += 1
    exhausted = [row for row in expired if row["attempt"] >= MAX_RETRY_ATTEMPTS]
    if expired:
        logger.warning(
            "Recovered %d expired retry claim(s); %d reached the attempt limit",
            len(expired),
            len(exhausted),
        )
    if exhausted:
        if not dry_run:
            written = _update_failures(
                engine.dataverse,
                [
                    (
                        row["id"],
                        _failure_payload(
                            RETRY_IGNORED,
                            attempt=row["attempt"],
                            message="Retry run stopped before recording the outcome "
                            f"{MAX_RETRY_ATTEMPTS} times",
                        ),
                    )
                    for row in exhausted
                ],
            )
            summary["ignored"] += len(written)
        exhausted_ids = {row["id"] for row in exhausted}
        pending = [row for row in pending if row["id"] not in exhausted_ids]

    if not pending:
        logger.info("No pending retryable integration failures")
        return summary

    grouped: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for row in pending:
        grouped[(row["parent_job_id"], row["codebase"])].append(row)

    # Retry steps widen the engine's contracted allowlists and change tracer
    # settings. Restore both, so a sync that reuses this engine (the delta after
    # the retry) is not filtered by the handful of retried IDs.
    tracer = engine.tracer
    saved_tracer = (tracer.persist_failures, tracer.trigger, tracer.retry_of_job_id)
    saved_workplaces = set(engine._contracted_workplace_eids)
    saved_individuals = set(engine._contracted_individual_eids)
    tracer.persist_failures = False
    engine._retry_onekey_cache = {}
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
        tracer.persist_failures, tracer.trigger, tracer.retry_of_job_id = saved_tracer
        engine._contracted_workplace_eids.clear()
        engine._contracted_workplace_eids.update(saved_workplaces)
        engine._contracted_individual_eids.clear()
        engine._contracted_individual_eids.update(saved_individuals)
        engine._retry_onekey_cache = None
    return summary


def resolve_pending_for_ids(
    dataverse,
    source_ids: set[str] | list[str],
    exclude_job_guid: str | None = None,
) -> int:
    """Mark pending failures Resolved when a later sync upserts those IDs.

    ``exclude_job_guid`` is the job that produced ``source_ids``. Its own failure
    rows share source IDs with its successes (a workplace's account upserts
    while its address or parent link fails), so they stay Pending.

    A delta's IDs are looked up 20 at a time. A full sync upserts far more IDs
    than there are pending rows, so above _RESOLVE_SCAN_AT the pending rows
    are read once and matched here.
    """
    ids = sorted({str(source_id) for source_id in source_ids if source_id})
    if not ids:
        return 0
    select = ["ts_integrationfailureid", "ts_sourceid", "_ts_integrationjob_value"]
    if len(ids) > _RESOLVE_SCAN_AT:
        wanted = set(ids)
        rows = [
            row
            for row in dataverse.retrieve_multiple(
                FAILURES, select=select, filter=f"ts_retrystatus eq {RETRY_PENDING}"
            )
            if row.get("ts_sourceid") in wanted
        ]
    else:
        rows = []
        for start in range(0, len(ids), 20):
            chunk = ids[start:start + 20]
            quoted = " or ".join(
                f"ts_sourceid eq '{escape_odata(source_id)}'" for source_id in chunk
            )
            # No $top: one source ID can have several pending rows.
            rows.extend(
                dataverse.retrieve_multiple(
                    FAILURES,
                    select=select,
                    filter=f"ts_retrystatus eq {RETRY_PENDING} and ({quoted})",
                )
            )
    if exclude_job_guid:
        current = exclude_job_guid.lower()
        rows = [
            row
            for row in rows
            if str(row.get("_ts_integrationjob_value") or "").lower() != current
        ]
    # A row that cannot be updated stays Pending for the next retry run.
    resolved = len(
        _update_failures(
            dataverse,
            [(row["ts_integrationfailureid"], _failure_payload(RETRY_RESOLVED)) for row in rows],
        )
    )
    if resolved:
        logger.info("Resolved %d pending failure(s) from the sync upsert", resolved)
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
    dataverse = engine.dataverse
    claimed: list[dict] = []
    finalized: set[str] = set()
    started = False
    try:
        # Claim every row (Retried) before replaying; if any row cannot be
        # claimed, release the others and skip the replay.
        # An expired claim's lost attempt is saved now, in case this run stops too.
        claimed_ids = _update_failures(
            dataverse,
            [
                (
                    row["id"],
                    _failure_payload(
                        RETRY_RETRIED,
                        attempt=row["attempt"] if row.get("claim_expired") else None,
                    ),
                )
                for row in rows
            ],
        )
        claimed = [row for row in rows if row["id"] in claimed_ids]
        if len(claimed) < len(rows):
            raise RuntimeError(
                f"could not claim {len(rows) - len(claimed)} of {len(rows)} failure row(s)"
            )
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
        outcomes = [(row, _outcome(row, _matching_failure(row, failed))) for row in claimed]
        written = _update_failures(
            dataverse,
            [
                (
                    row["id"],
                    _failure_payload(
                        outcome["status"],
                        attempt=outcome["attempt"],
                        message=outcome["message"],
                    ),
                )
                for row, outcome in outcomes
            ],
        )
        for row, outcome in outcomes:
            if row["id"] in written:
                finalized.add(row["id"])
                result[outcome["bucket"]] += 1
        if len(finalized) < len(outcomes):
            raise RuntimeError(
                f"could not record the outcome of {len(outcomes) - len(finalized)} failure row(s)"
            )
    except Exception as exc:
        result["errors"].append(f"{name}: {exc}")
        result["success"] = False
        logger.exception("Retry step %s failed for %s", name, codebase)
        # Rows not finalized: release the claim if the replay never started,
        # otherwise count a failed attempt.
        updates: list[tuple[str, dict]] = []
        buckets: dict[str, str] = {}
        for row in claimed:
            if row["id"] in finalized:
                continue
            if not started:
                updates.append(
                    (row["id"], _failure_payload(RETRY_PENDING, attempt=row["attempt"]))
                )
                continue
            attempt = row["attempt"] + 1
            status = RETRY_IGNORED if attempt >= MAX_RETRY_ATTEMPTS else RETRY_PENDING
            updates.append(
                (row["id"], _failure_payload(status, attempt=attempt, message=str(exc)))
            )
            buckets[row["id"]] = "ignored" if status == RETRY_IGNORED else "pending"
        for record_id in _update_failures(dataverse, updates):
            if record_id in buckets:
                result[buckets[record_id]] += 1


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


def _get_by_id(engine, eid: str, entity_type: str, codebase: str) -> dict | None:
    """OneKey record by ID, fetched at most once per retry run.

    By-ID reads count against the OneKey subscription, and one failed workplace
    is often retried in several steps (account, address, hierarchy, relations),
    while many HCPs share a main workplace.
    """
    cache = getattr(engine, "_retry_onekey_cache", None)
    key = (entity_type, eid, codebase)
    if cache is not None:
        cached = cache.get(key, _NOT_FETCHED)
        if cached is not _NOT_FETCHED:
            return cached
    record = engine.onekey.get_by_id(eid, entity_type, cod_base=codebase)
    if cache is not None:
        cache[key] = record
    return record


def _workplaces(engine, codebase: str, rows: list[dict]) -> tuple[list[dict], list[str]]:
    flats = []
    missing = []
    seen = set()
    for row in rows:
        source_id = row["source_id"]
        if source_id in seen:
            continue
        seen.add(source_id)
        raw = _get_by_id(engine, source_id, WORKPLACE, codebase)
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
        raw = _get_by_id(engine, source_id, WORKPLACE, codebase)
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
        # Stop at the first missing piece: each lookup is another OneKey call.
        individual = _get_by_id(engine, individual_eid, INDIVIDUAL, codebase)
        activity = _main_activity(engine, codebase, individual_eid) if individual else None
        workplace_eid = ((activity or {}).get("activity") or {}).get("workplaceEid")
        workplace = (
            _get_by_id(engine, workplace_eid, WORKPLACE, codebase) if workplace_eid else None
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
    return _get_by_id(engine, activity_eid, ACTIVITY, codebase) or chosen


def _activities(engine, codebase: str, rows: list[dict]) -> tuple[list[dict], list[str]]:
    packets = []
    missing = []
    seen = set()
    for row in rows:
        source_id = row["source_id"]
        if source_id in seen:
            continue
        seen.add(source_id)
        raw = _get_by_id(engine, source_id, ACTIVITY, codebase)
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


def _failure_payload(
    status: int,
    attempt: int | None = None,
    message: str | None = None,
) -> dict:
    payload = {"ts_retrystatus": int(status)}
    if attempt is not None:
        payload["ts_attemptcount"] = int(attempt)
    if message:
        payload["ts_errormessage"] = clip(message, 2000)
    return payload


def _update_failures(dataverse, updates: list[tuple[str, dict]]) -> set[str]:
    """Write per-row payloads to ts_integrationfailure; return the ids written.

    One UpdateMultiple per batch, each row keeping its own values. UpdateMultiple
    is all-or-nothing, so a rejected batch is retried row by row. A row that
    still fails is logged and left unchanged; like the other trace writes, this
    never raises into the sync.
    """
    update_multiple = getattr(dataverse, "update_multiple", None)
    batch_size = getattr(getattr(dataverse, "config", None), "bulk_batch_size", None) or 100
    written: set[str] = set()
    for batch in chunked(updates, batch_size):
        if update_multiple is not None:
            try:
                update_multiple(
                    FAILURES,
                    [
                        {"ts_integrationfailureid": record_id, **payload}
                        for record_id, payload in batch
                    ],
                    logical_name="ts_integrationfailure",
                )
                written.update(record_id for record_id, _ in batch)
                continue
            except Exception as exc:
                logger.warning(
                    "UpdateMultiple failed for %d failure row(s) (%s); updating one by one",
                    len(batch),
                    exc,
                )
        for record_id, payload in batch:
            try:
                dataverse.update(FAILURES, record_id, payload)
                written.add(record_id)
            except Exception:
                logger.warning(
                    "Could not update integration failure %s", record_id, exc_info=True
                )
    return written


def _normalize_failure(row: dict) -> dict:
    parent = row.get("ts_IntegrationJob") or {}
    source_id = str(row.get("ts_sourceid") or "")
    # Prefer the codebase in the OneKey ID: a job over several codebases stores
    # them all ("WUK,WDE"), or only the first when they do not fit.
    prefix = source_id[:3].upper()
    codes = [
        code.strip().upper()
        for code in str(parent.get("ts_codebase") or "").split(",")
        if code.strip()
    ]
    codebase = prefix if _CODEBASE_PREFIX.fullmatch(prefix) else (codes[0] if codes else prefix)
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
        # Still Retried: the run that claimed it stopped before the outcome.
        "claim_expired": row.get("ts_retrystatus") == RETRY_RETRIED,
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
