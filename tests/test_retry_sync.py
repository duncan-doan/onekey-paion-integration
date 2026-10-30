"""Offline tests for the integration failure retry queue."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from dataverse.exceptions import DataverseHTTPError
from integration.dynamic_sync import DynamicSyncEngine
from integration.job_trace import (
    ENTITY_ACCOUNT,
    ENTITY_AFFILIATION,
    ENTITY_CONTACT,
    FAILURES,
    JOB_DELTA,
    JOB_FULL,
    JOB_HCO,
    JOBS,
    MAX_RETRY_ATTEMPTS,
    PHASE_HCO,
    PHASE_HCP,
    PHASE_HCP_ACTIVITY,
    RETRY_IGNORED,
    RETRY_PENDING,
    RETRY_RESOLVED,
    RETRY_RETRIED,
    STATUS_RUNNING,
    STATUS_SUCCEEDED,
    TRIGGER_MANUAL,
    TRIGGER_TIMER,
    JobTracer,
)
from integration.retry_sync import (
    _normalize_failure,
    _update_failures,
    fetch_pending_failures,
    pending_filter,
    resolve_pending_for_ids,
    retry_pending_failures,
)
from integration.scheduled_sync import (
    covered_window,
    delta_window,
    run_full,
    run_retry_then_delta,
)


class FakeDataverse:
    def __init__(self, rows=None):
        self.rows = rows or []
        self.created = []
        self.updated = []
        self.queries = []

    def create(self, entity_set, data, return_record=False):
        self.created.append((entity_set, data))
        return f"guid-{len(self.created)}"

    def update(self, entity_set, record_id, data):
        self.updated.append((entity_set, record_id, dict(data)))
        return record_id

    def retrieve_multiple(self, entity_set, **kwargs):
        self.queries.append((entity_set, kwargs))
        return list(self.rows)

    def find_one(self, entity_set, filter=None, select=None):
        if filter and "parent-job" in filter:
            return {"ts_integrationjobid": "parent-guid"}
        return None


class Engine:
    def __init__(self, dataverse, onekey=None):
        self.dataverse = dataverse
        self.onekey = onekey or FakeOneKey()
        self.tracer = JobTracer(dataverse)
        self.succeeded_ids = set()
        self.succeeded_records = set()
        self.sync_logger = SimpleNamespace(job_id=None)
        self._contracted_workplace_eids = set()
        self._contracted_individual_eids = set()
        self.calls = []

    _begin_job = DynamicSyncEngine._begin_job
    _checkpoint = DynamicSyncEngine._checkpoint
    _finish_job = DynamicSyncEngine._finish_job
    _http_snapshot = DynamicSyncEngine._http_snapshot
    _log_http_summary = DynamicSyncEngine._log_http_summary
    _set_request_hook = DynamicSyncEngine._set_request_hook

    def _sync_hcos(self, codebase, flattened=None, **kwargs):
        self.calls.append(("accounts", codebase, [row.get("workplaceEid") for row in flattened]))
        return (
            {"processed": len(flattened), "created": 0, "updated": len(flattened), "failed": 0},
            flattened,
            [],
        )

    def _sync_hco_relations(self, raws, **kwargs):
        self.calls.append(("affiliations", kwargs.get("allowed_workplace_eids")))
        self.tracer.record_failure(
            phase=PHASE_HCO,
            entity="ts_affiliation",
            source_id="WUKE00009999",
            operation="upsert",
            message="Dataverse timeout",
            external_id="WUKE00001259 - WUKE00009999 - HIE",
            exc=TimeoutError("timed out"),
        )
        return {"processed": 1, "created": 0, "updated": 0, "failed": 1}


class FakeOneKey:
    def __init__(self, found=True):
        self.found = found

    def get_by_id(self, eid, entity_type, cod_base=None):
        if not self.found:
            return None
        return {
            "entityType": "WORKPLACE",
            "onekeyEid": eid,
            "codBase": cod_base,
            "workplace": {
                "workplaceEid": eid,
                "officialName": "Retry Clinic",
                "relations": [
                    {
                        "typeCode": "workplaceToWorkplaceLink",
                        "onekeyEid": "WUKE00009999",
                        "linkCode": "HIE",
                        "isParent": False,
                    }
                ],
            },
        }


def _row(
    record_id="fail-1",
    source_id="WUKE00001259",
    phase=PHASE_HCO,
    entity=ENTITY_ACCOUNT,
    attempt=1,
    parent="parent-job",
    codebase="WUK",
):
    return {
        "id": record_id,
        "source_id": source_id,
        "external_id": "",
        "phase": phase,
        "entity": entity,
        "attempt": attempt,
        "parent_job_id": parent,
        "parent_guid": "parent-guid",
        "codebase": codebase,
    }


def _failure_updates(dataverse):
    return [item for item in dataverse.updated if item[0] == FAILURES]


def test_pending_filter_excludes_running_parent_and_max_attempts():
    text = pending_filter(codebase="wuk", job_id="parent-job", source_ids=["WUKE00001259"])
    assert f"ts_retrystatus eq {RETRY_PENDING}" in text
    assert "ts_isretryable eq true" in text
    assert "ts_attemptcount lt 5" in text
    assert f"ts_IntegrationJob/ts_status ne {STATUS_RUNNING}" in text
    assert "ts_IntegrationJob/ts_codebase eq 'WUK'" in text
    assert "ts_IntegrationJob/ts_jobid eq 'parent-job'" in text
    assert "ts_sourceid eq 'WUKE00001259'" in text


def test_pending_filter_can_keep_one_step():
    # retry_failed_sync --phase: filtered in the query, so --limit counts only
    # that step's rows instead of the oldest rows of any step.
    text = pending_filter(steps=["affiliations"])
    assert f"(ts_phase eq {PHASE_HCO} and ts_entitytype eq {ENTITY_AFFILIATION})" in text
    assert f"ts_entitytype eq {ENTITY_ACCOUNT})" not in text
    assert pending_filter(steps=["nope"]).endswith("ts_phase eq -1")


def test_fetch_normalizes_parent_codebase():
    dataverse = FakeDataverse(
        rows=[
            {
                "ts_integrationfailureid": "fail-1",
                "ts_sourceid": "WUKE00001259",
                "ts_externalid": "",
                "ts_phase": PHASE_HCO,
                "ts_entitytype": ENTITY_ACCOUNT,
                "ts_attemptcount": 2,
                "ts_IntegrationJob": {
                    "ts_integrationjobid": "parent-guid",
                    "ts_jobid": "parent-job",
                    "ts_codebase": "WUK,WFR",
                    "ts_status": 126870002,
                },
            }
        ]
    )
    rows = fetch_pending_failures(dataverse, codebase="WUK", limit=10)
    assert rows[0]["codebase"] == "WUK"
    assert rows[0]["parent_job_id"] == "parent-job"
    assert rows[0]["attempt"] == 2
    assert dataverse.queries[0][1]["expand"].startswith("ts_IntegrationJob(")


def test_dry_run_does_not_patch():
    dataverse = FakeDataverse()
    engine = Engine(dataverse)
    summary = retry_pending_failures(engine, dry_run=True, rows=[_row()])
    assert summary["selected"] == 1
    assert summary["resolved"] == 0
    assert dataverse.updated == []
    assert dataverse.created == []
    assert engine.calls == []


def test_success_resolves_original_row_and_binds_retry_job():
    dataverse = FakeDataverse()
    engine = Engine(dataverse)
    summary = retry_pending_failures(engine, rows=[_row()])
    assert summary["selected"] == 1
    assert summary["resolved"] == 1
    assert summary["pending"] == 0
    assert summary["ignored"] == 0
    assert summary["success"] is True
    assert engine.tracer.persist_failures is True
    failure_writes = _failure_updates(dataverse)
    assert [item[2]["ts_retrystatus"] for item in failure_writes] == [
        RETRY_RETRIED,
        RETRY_RESOLVED,
    ]
    assert failure_writes[1][2]["ts_attemptcount"] == 1
    job = dataverse.created[0][1]
    assert dataverse.created[0][0] == JOBS
    assert job["ts_jobtype"] == JOB_HCO
    assert job["ts_Retryofjob@odata.bind"] == f"/{JOBS}(parent-guid)"
    assert all(entity != FAILURES for entity, _payload in dataverse.created)
    assert engine.calls[0][0] == "accounts"


def test_retryable_failure_returns_to_pending_and_increments():
    dataverse = FakeDataverse()
    engine = Engine(dataverse)

    def fail_sync(codebase, flattened=None, **kwargs):
        engine.tracer.record_failure(
            phase=PHASE_HCO,
            entity="account",
            source_id="WUKE00001259",
            operation="upsert",
            message="too many requests",
            exc=type("HTTP", (), {"status_code": 429, "__str__": lambda self: "429"})(),
        )
        return (
            {"processed": 1, "created": 0, "updated": 0, "failed": 1},
            flattened,
            [],
        )

    engine._sync_hcos = fail_sync
    summary = retry_pending_failures(engine, rows=[_row(attempt=2)])
    assert summary["pending"] == 1
    # Failing again is an outcome, not a failed run: no redelivery.
    assert summary["success"] is True
    final = _failure_updates(dataverse)[-1][2]
    assert final["ts_retrystatus"] == RETRY_PENDING
    assert final["ts_attemptcount"] == 3
    assert all(entity != FAILURES for entity, _payload in dataverse.created)


def test_max_attempts_marks_ignored():
    dataverse = FakeDataverse()
    engine = Engine(dataverse)

    def fail_sync(codebase, flattened=None, **kwargs):
        engine.tracer.record_failure(
            phase=PHASE_HCO,
            entity="account",
            source_id="WUKE00001259",
            operation="upsert",
            message="timeout",
            exc=TimeoutError("timed out"),
        )
        return (
            {"processed": 1, "created": 0, "updated": 0, "failed": 1},
            flattened,
            [],
        )

    engine._sync_hcos = fail_sync
    summary = retry_pending_failures(engine, rows=[_row(attempt=4)])
    assert summary["ignored"] == 1
    final = _failure_updates(dataverse)[-1][2]
    assert final["ts_retrystatus"] == RETRY_IGNORED
    assert final["ts_attemptcount"] == 5


def test_missing_onekey_record_is_ignored():
    dataverse = FakeDataverse()
    engine = Engine(dataverse, onekey=FakeOneKey(found=False))
    summary = retry_pending_failures(engine, rows=[_row()])
    assert summary["ignored"] == 1
    assert engine.calls == []
    final = _failure_updates(dataverse)[-1][2]
    assert final["ts_retrystatus"] == RETRY_IGNORED
    assert "not found" in final["ts_errormessage"]


def test_relation_failure_matches_external_id():
    dataverse = FakeDataverse()
    engine = Engine(dataverse)
    summary = retry_pending_failures(
        engine,
        rows=[
            _row(
                source_id="WUKE00001259",
                phase=PHASE_HCO,
                entity=ENTITY_AFFILIATION,
            )
        ],
    )
    assert summary["pending"] == 1
    assert "WUKE00009999" in engine.calls[0][1]


def test_sync_exception_is_not_left_retried():
    dataverse = FakeDataverse()
    engine = Engine(dataverse)

    def explode(codebase, flattened=None, **kwargs):
        raise RuntimeError("batch down")

    engine._sync_hcos = explode
    summary = retry_pending_failures(engine, rows=[_row(attempt=1)])
    assert summary["pending"] == 1
    statuses = [item[2]["ts_retrystatus"] for item in _failure_updates(dataverse)]
    assert statuses == [RETRY_RETRIED, RETRY_PENDING]
    assert _failure_updates(dataverse)[-1][2]["ts_attemptcount"] == 2


def test_claim_failure_releases_without_increment():
    dataverse = FakeDataverse()
    engine = Engine(dataverse)
    calls = {"n": 0}

    def update(entity_set, record_id, data):
        calls["n"] += 1
        if calls["n"] == 2:
            raise RuntimeError("claim failed")
        dataverse.updated.append((entity_set, record_id, dict(data)))
        return record_id

    dataverse.update = update
    rows = [_row(record_id="fail-1"), _row(record_id="fail-2", source_id="WUKE00000002")]
    summary = retry_pending_failures(engine, rows=rows)
    assert summary["pending"] == 0
    assert summary["resolved"] == 0
    released = [
        item for item in dataverse.updated
        if item[0] == FAILURES and item[2].get("ts_retrystatus") == RETRY_PENDING
    ]
    assert released[0][2]["ts_attemptcount"] == 1
    assert engine.calls == []


def test_resolve_pending_for_succeeded_ids():
    dataverse = FakeDataverse(
        rows=[{"ts_integrationfailureid": "fail-9", "ts_sourceid": "WUKE00001259"}]
    )
    count = resolve_pending_for_ids(dataverse, {"WUKE00001259", "WUKE00001259 - ADDR"})
    assert count == 1
    assert dataverse.updated[0][2] == {"ts_retrystatus": RETRY_RESOLVED}
    assert "ts_sourceid eq 'WUKE00001259'" in dataverse.queries[0][1]["filter"]


def test_resolution_requires_the_successful_phase_entity_and_external_key():
    from integration.job_trace import ENTITY_ADDRESS, PHASE_ADDRESSES, PHASE_HIERARCHY

    def pending(record_id, phase, entity, external=""):
        return {
            "ts_integrationfailureid": record_id, "ts_sourceid": "WUKH1",
            "ts_phase": phase, "ts_entitytype": entity, "ts_externalid": external,
        }

    dataverse = FakeDataverse(rows=[
        pending("account", PHASE_HCO, ENTITY_ACCOUNT, "WUKH1"),
        pending("address", PHASE_ADDRESSES, ENTITY_ADDRESS, "WUKH1 - ADDR1"),
        pending("hierarchy", PHASE_HIERARCHY, ENTITY_ACCOUNT),
        pending("edge-1", PHASE_HCO, ENTITY_AFFILIATION, "WUKH1 - WUKH2 - HIE"),
        pending("edge-2", PHASE_HCO, ENTITY_AFFILIATION, "WUKH1 - WUKH3 - HIE"),
    ])
    resolved = resolve_pending_for_ids(
        dataverse, {"WUKH1"}, succeeded_records={
            (PHASE_HCO, ENTITY_ACCOUNT, "WUKH1", "WUKH1"),
            (PHASE_HCO, ENTITY_AFFILIATION, "WUKH1", "WUKH1 - WUKH2 - HIE"),
        },
    )
    assert resolved == 2
    assert {record_id for _, record_id, _ in dataverse.updated} == {"account", "edge-1"}


def test_a_current_failure_blocks_resolution_of_the_same_operation():
    dataverse = FakeDataverse(rows=[{
        "ts_integrationfailureid": "old-failure", "ts_sourceid": "WUKH1",
        "ts_phase": PHASE_HCO, "ts_entitytype": ENTITY_ACCOUNT, "ts_externalid": "WUKH1",
    }])
    resolved = resolve_pending_for_ids(
        dataverse, {"WUKH1"},
        succeeded_records={(PHASE_HCO, ENTITY_ACCOUNT, "WUKH1", "WUKH1")},
        failed_records=[{
            "phase": PHASE_HCO, "entity": ENTITY_ACCOUNT,
            "source_id": "WUKH1", "external_id": "WUKH1",
        }],
    )
    assert resolved == 0
    assert dataverse.updated == []


@pytest.mark.parametrize("status", [401, 403, 429, 503])
def test_failure_row_updates_do_not_fan_out_after_a_service_error(status):
    dataverse = FakeDataverse()
    dataverse.config = SimpleNamespace(bulk_batch_size=1)
    calls = []

    def unavailable(*args, **kwargs):
        calls.append(args)
        raise DataverseHTTPError("service failure", status_code=status)

    dataverse.update_multiple = unavailable
    written = _update_failures(dataverse, [
        ("failure-1", {"ts_retrystatus": RETRY_PENDING}),
        ("failure-2", {"ts_retrystatus": RETRY_PENDING}),
    ])
    assert written == set()
    assert len(calls) == 1
    assert dataverse.updated == []


def test_scheduled_job_retries_then_delta_then_resolves():
    dataverse = FakeDataverse(
        rows=[{"ts_integrationfailureid": "fail-9", "ts_sourceid": "WUKE00001259",
               "ts_phase": PHASE_HCO, "ts_entitytype": ENTITY_ACCOUNT}]
    )
    engine = Engine(dataverse)
    # Left behind by a retry group; the delta must not inherit it.
    engine.tracer.retry_of_job_id = "parent-job"
    seen = {}

    def sync_delta(**kwargs):
        seen["retry_of_job_id"] = engine.tracer.retry_of_job_id
        engine.succeeded_ids.add("WUKE00001259")
        engine.succeeded_records.add((PHASE_HCO, ENTITY_ACCOUNT, "WUKE00001259", "WUKE00001259"))
        return {"success": True, "job_id": "delta-job", "mode": "delta"}

    engine.sync_delta = sync_delta
    result = run_retry_then_delta(
        engine,
        codebases=["WUK"],
        date_from="2026-09-29",
        date_to="2026-09-30",
    )
    assert result["success"] is True
    assert result["resolved_by_delta"] == 1
    assert engine.tracer.trigger == TRIGGER_TIMER
    assert seen["retry_of_job_id"] is None


def test_delta_window_starts_from_last_covering_job():
    dataverse = FakeDataverse(
        rows=[
            # Lost a codebase: its window was not covered, so it is skipped.
            {
                "ts_jobid": "delta-2",
                "ts_startedon": "2026-10-01T00:00:03Z",
                "ts_errorsummary": "WUK: OneKey 503",
            },
            # Picklist errors alone do not stop the HCO/HCP phases.
            {
                "ts_jobid": "full-1",
                "ts_startedon": "2026-09-30T00:00:04Z",
                "ts_errorsummary": "picklists: metadata denied",
            },
        ]
    )
    iso_from, iso_to = delta_window(dataverse, ["WUK"], date_to="2026-10-02")
    assert iso_from == "2026-09-29T18:00:04Z"  # less WINDOW_OVERLAP (6 h)
    assert iso_to == "2026-10-02T23:59:59Z"
    entity_set, query = dataverse.queries[0]
    assert entity_set == JOBS
    assert "ts_codebase eq 'WUK'" in query["filter"]
    assert f"ts_jobtype eq {JOB_FULL}" in query["filter"]
    assert f"ts_jobtype eq {JOB_DELTA}" in query["filter"]
    assert query["order_by"] == "ts_startedon desc"
    # A multi-phase retry is typed Full; only ts_retryofjob tells it apart.
    assert "_ts_retryofjob_value eq null" in query["filter"]


def test_delta_window_explicit_dates_skip_lookup():
    dataverse = FakeDataverse()
    iso_from, iso_to = delta_window(
        dataverse, ["WUK"], date_from="2026-09-28", date_to="2026-09-30"
    )
    assert (iso_from, iso_to) == ("2026-09-28T00:00:00Z", "2026-09-30T23:59:59Z")
    assert dataverse.queries == []


def test_delta_window_falls_back_to_yesterday_without_job():
    iso_from, _ = delta_window(FakeDataverse(), ["WUK"])
    yesterday = datetime.now(timezone.utc) - timedelta(days=1)
    assert iso_from == yesterday.strftime("%Y-%m-%dT00:00:00Z")


def test_covered_window():
    assert covered_window({"ts_errorsummary": None})
    assert covered_window({"ts_errorsummary": "picklists: a | picklists: b"})
    assert not covered_window({"ts_errorsummary": "Sync stopped before completion"})
    assert not covered_window({"ts_errorsummary": "picklists: a | WUK: boom"})


def test_scheduled_full_resolves_pending():
    dataverse = FakeDataverse(
        rows=[{"ts_integrationfailureid": "fail-9", "ts_sourceid": "WUKE00001259",
               "ts_phase": PHASE_HCO, "ts_entitytype": ENTITY_ACCOUNT}]
    )
    engine = Engine(dataverse)

    def sync_full(**kwargs):
        engine.calls.append(("full", kwargs["codebases"]))
        engine.succeeded_ids.add("WUKE00001259")
        engine.succeeded_records.add((PHASE_HCO, ENTITY_ACCOUNT, "WUKE00001259", "WUKE00001259"))
        return {"success": True, "job_id": "full-job", "mode": "full"}

    engine.sync_full = sync_full
    result = run_full(engine, codebases=["WUK"])
    assert result["success"] is True
    assert result["resolved_by_full"] == 1
    assert engine.calls == [("full", ["WUK"])]
    assert engine.tracer.trigger == TRIGGER_TIMER


def test_resolve_pending_scans_once_for_large_id_sets():
    dataverse = FakeDataverse(
        rows=[
            {"ts_integrationfailureid": "fail-1", "ts_sourceid": "WUKE00000001"},
            {"ts_integrationfailureid": "fail-2", "ts_sourceid": "WUKE99999999"},
        ]
    )
    ids = {f"WUKE{n:08d}" for n in range(1, 1500)}
    assert resolve_pending_for_ids(dataverse, ids) == 1
    assert len(dataverse.queries) == 1
    # Only this codebase's pending rows are read.
    assert dataverse.queries[0][1]["filter"] == (
        f"ts_retrystatus eq {RETRY_PENDING} and (startswith(ts_sourceid,'WUK'))"
    )
    assert dataverse.updated == [(FAILURES, "fail-1", {"ts_retrystatus": RETRY_RESOLVED})]


class BulkDataverse(FakeDataverse):
    """FakeDataverse with UpdateMultiple, a batch size of 2, and optional failures."""

    def __init__(self, rows=None, fail_bulk=False, fail_ids=()):
        super().__init__(rows=rows)
        self.config = SimpleNamespace(bulk_batch_size=2)
        self.fail_bulk = fail_bulk
        self.fail_ids = set(fail_ids)
        self.bulk = []

    def update_multiple(self, entity_set, records, logical_name=None):
        if self.fail_bulk:
            raise DataverseHTTPError("UpdateMultiple is not enabled", status_code=400)
        self.bulk.append((entity_set, logical_name, [dict(r) for r in records]))

    def update(self, entity_set, record_id, data):
        if record_id in self.fail_ids:
            raise DataverseHTTPError("Record not found", status_code=404)
        return super().update(entity_set, record_id, data)


PENDING_ROWS = [
    {"ts_integrationfailureid": f"fail-{n}", "ts_sourceid": f"WUKE0000000{n}"}
    for n in range(1, 4)
]
PENDING_IDS = {row["ts_sourceid"] for row in PENDING_ROWS}


def test_resolve_pending_uses_update_multiple_per_batch():
    dataverse = BulkDataverse(rows=PENDING_ROWS)
    assert resolve_pending_for_ids(dataverse, PENDING_IDS) == 3
    assert dataverse.updated == []
    assert [
        (entity_set, logical, [r["ts_integrationfailureid"] for r in records])
        for entity_set, logical, records in dataverse.bulk
    ] == [
        (FAILURES, "ts_integrationfailure", ["fail-1", "fail-2"]),
        (FAILURES, "ts_integrationfailure", ["fail-3"]),
    ]
    assert dataverse.bulk[0][2][0] == {
        "ts_integrationfailureid": "fail-1",
        "ts_retrystatus": RETRY_RESOLVED,
    }


def test_resolve_pending_falls_back_to_patch_when_update_multiple_fails():
    dataverse = BulkDataverse(rows=PENDING_ROWS, fail_bulk=True)
    assert resolve_pending_for_ids(dataverse, PENDING_IDS) == 3
    assert [item[1] for item in dataverse.updated] == ["fail-1", "fail-2", "fail-3"]


def test_resolve_pending_skips_a_row_that_cannot_be_updated():
    dataverse = BulkDataverse(rows=PENDING_ROWS, fail_bulk=True, fail_ids={"fail-2"})
    # fail-2 stays Pending for the next retry run; the others are resolved.
    assert resolve_pending_for_ids(dataverse, PENDING_IDS) == 2
    assert [item[1] for item in dataverse.updated] == ["fail-1", "fail-3"]


TWO_ROWS = [_row(record_id="fail-1"), _row(record_id="fail-2", source_id="WUKE00000002")]


def test_retry_claims_and_outcomes_use_update_multiple():
    dataverse = BulkDataverse()
    engine = Engine(dataverse)
    summary = retry_pending_failures(engine, rows=TWO_ROWS)
    assert summary["resolved"] == 2
    assert _failure_updates(dataverse) == []
    claims, outcomes = [records for _, _, records in dataverse.bulk]
    assert claims == [
        {"ts_integrationfailureid": "fail-1", "ts_retrystatus": RETRY_RETRIED},
        {"ts_integrationfailureid": "fail-2", "ts_retrystatus": RETRY_RETRIED},
    ]
    assert outcomes == [
        {"ts_integrationfailureid": "fail-1", "ts_retrystatus": RETRY_RESOLVED, "ts_attemptcount": 1},
        {"ts_integrationfailureid": "fail-2", "ts_retrystatus": RETRY_RESOLVED, "ts_attemptcount": 1},
    ]


def test_one_failed_outcome_write_does_not_spoil_the_others():
    dataverse = BulkDataverse(fail_bulk=True)
    engine = Engine(dataverse)
    patch = dataverse.update

    def update(entity_set, record_id, data):
        if record_id == "fail-1" and data.get("ts_retrystatus") == RETRY_RESOLVED:
            raise DataverseHTTPError("Record is locked", status_code=409)
        return patch(entity_set, record_id, data)

    dataverse.update = update
    summary = retry_pending_failures(engine, rows=TWO_ROWS)
    # fail-2's outcome is still written. fail-1's could not be: that is
    # Dataverse, not the row, so it stays Pending without spending an attempt.
    assert summary["resolved"] == 1
    assert summary["pending"] == 1
    last = {record_id: data for _, record_id, data in _failure_updates(dataverse)}
    assert last["fail-2"]["ts_retrystatus"] == RETRY_RESOLVED
    assert last["fail-1"]["ts_retrystatus"] == RETRY_PENDING
    assert last["fail-1"]["ts_attemptcount"] == 1


def test_resolve_skips_rows_written_by_the_same_job():
    # Same workplace: the account upserted, but its address failed in that run.
    dataverse = FakeDataverse(
        rows=[
            {"ts_integrationfailureid": "old", "ts_sourceid": "WUKE00001259",
             "_ts_integrationjob_value": "job-yesterday"},
            {"ts_integrationfailureid": "new", "ts_sourceid": "WUKE00001259",
             "_ts_integrationjob_value": "job-today"},
        ]
    )
    resolved = resolve_pending_for_ids(
        dataverse, {"WUKE00001259"}, exclude_job_guid="JOB-TODAY"
    )
    assert resolved == 1
    assert [item[1] for item in dataverse.updated] == ["old"]


def test_resolve_reads_every_pending_row_for_an_id():
    dataverse = FakeDataverse(
        rows=[
            {"ts_integrationfailureid": "fail-1", "ts_sourceid": "WUKE00001259"},
            {"ts_integrationfailureid": "fail-2", "ts_sourceid": "WUKE00001259"},
        ]
    )
    assert resolve_pending_for_ids(dataverse, {"WUKE00001259"}) == 2
    assert "top" not in dataverse.queries[0][1]


def test_pending_filter_retries_failures_of_a_stale_running_job():
    now = datetime(2026, 10, 3, 12, 0, tzinfo=timezone.utc)
    text = pending_filter(now=now)
    assert (
        f"(ts_IntegrationJob/ts_status ne {STATUS_RUNNING}"
        " or ts_IntegrationJob/ts_lastheartbeat lt 2026-10-03T11:00:00Z)"
    ) in text


def test_fetch_asks_for_only_the_limit():
    dataverse = FakeDataverse()
    fetch_pending_failures(dataverse, limit=25)
    assert dataverse.queries[0][1]["top"] == 25


@pytest.mark.parametrize(
    "source_id, parent_codebase, expected",
    [
        ("WDEH02963729", "WUK,WDE", "WDE"),  # multi-codebase job
        ("WFRH00453901", "WUK", "WFR"),      # codebases truncated to the first
        ("12345", "WUK", "WUK"),             # not a OneKey ID: parent codebase
    ],
)
def test_normalize_takes_codebase_from_the_onekey_id(source_id, parent_codebase, expected):
    row = _normalize_failure(
        {
            "ts_integrationfailureid": "fail-1",
            "ts_sourceid": source_id,
            "ts_IntegrationJob": {"ts_jobid": "parent-job", "ts_codebase": parent_codebase},
        }
    )
    assert row["codebase"] == expected


def test_retry_restores_engine_and_tracer_state():
    engine = Engine(FakeDataverse())
    engine._contracted_workplace_eids.add("WUKE00000001")
    engine.tracer.trigger = TRIGGER_TIMER
    engine.tracer.retry_of_job_id = None
    rows = [_row(entity=ENTITY_AFFILIATION)]
    retry_pending_failures(engine, rows=rows, trigger=TRIGGER_MANUAL)
    # The affiliation step allowed the related HCO; the delta after it must not.
    assert engine.calls[0][1] >= {"WUKE00001259", "WUKE00009999"}
    assert engine._contracted_workplace_eids == {"WUKE00000001"}
    assert engine._contracted_individual_eids == set()
    assert engine.tracer.trigger == TRIGGER_TIMER
    assert engine.tracer.retry_of_job_id is None
    assert engine.tracer.persist_failures is True


def test_retry_fetches_each_onekey_record_once():
    onekey = FakeOneKey()
    fetched = []
    get_by_id = onekey.get_by_id

    def counting(eid, entity_type, cod_base=None):
        fetched.append((entity_type, eid))
        return get_by_id(eid, entity_type, cod_base=cod_base)

    onekey.get_by_id = counting
    engine = Engine(FakeDataverse(), onekey=onekey)
    # One workplace failed as an account and as an HCO relation.
    rows = [_row(record_id="fail-1"), _row(record_id="fail-2", entity=ENTITY_AFFILIATION)]
    retry_pending_failures(engine, rows=rows)
    assert [name for name, *_ in engine.calls] == ["accounts", "affiliations"]
    assert fetched == [("WORKPLACE", "WUKE00001259")]


def test_pending_filter_takes_back_expired_claims():
    now = datetime(2026, 10, 3, 12, 0, tzinfo=timezone.utc)
    text = pending_filter(now=now)
    assert (
        f"(ts_retrystatus eq {RETRY_PENDING} or (ts_retrystatus eq {RETRY_RETRIED}"
        " and modifiedon lt 2026-10-03T10:00:00Z))"
    ) in text


def test_fetch_marks_rows_still_claimed():
    dataverse = FakeDataverse(
        rows=[
            {"ts_integrationfailureid": "fail-1", "ts_sourceid": "WUKE00001259",
             "ts_retrystatus": RETRY_RETRIED, "ts_attemptcount": 2},
            {"ts_integrationfailureid": "fail-2", "ts_sourceid": "WUKE00000002",
             "ts_retrystatus": RETRY_PENDING, "ts_attemptcount": 1},
        ]
    )
    rows = fetch_pending_failures(dataverse)
    assert [row["claim_expired"] for row in rows] == [True, False]
    assert "ts_retrystatus" in dataverse.queries[0][1]["select"]


def _expired(attempt):
    row = _row(attempt=attempt)
    row["claim_expired"] = True
    return row


def test_expired_claim_counts_the_stopped_run_as_an_attempt():
    dataverse = FakeDataverse()
    engine = Engine(dataverse)
    summary = retry_pending_failures(engine, rows=[_expired(1)])
    assert summary["resolved"] == 1
    claim, outcome = _failure_updates(dataverse)
    # Saved at claim time, in case this run stops as well.
    assert claim[2] == {"ts_retrystatus": RETRY_RETRIED, "ts_attemptcount": 2}
    assert outcome[2] == {"ts_retrystatus": RETRY_RESOLVED, "ts_attemptcount": 2}


def test_expired_claim_at_the_limit_is_ignored_without_replay():
    dataverse = FakeDataverse()
    engine = Engine(dataverse)
    summary = retry_pending_failures(engine, rows=[_expired(MAX_RETRY_ATTEMPTS - 1)])
    assert summary["ignored"] == 1
    assert engine.calls == []
    [(_, record_id, payload)] = _failure_updates(dataverse)
    assert record_id == "fail-1"
    assert payload["ts_retrystatus"] == RETRY_IGNORED
    assert payload["ts_attemptcount"] == MAX_RETRY_ATTEMPTS
    assert "stopped before recording the outcome" in payload["ts_errormessage"]


def test_expired_claim_at_the_limit_is_left_alone_in_a_dry_run():
    dataverse = FakeDataverse()
    engine = Engine(dataverse)
    retry_pending_failures(engine, rows=[_expired(MAX_RETRY_ATTEMPTS - 1)], dry_run=True)
    assert dataverse.updated == []


def test_two_parent_jobs_open_two_retry_jobs():
    dataverse = FakeDataverse()
    engine = Engine(dataverse)
    summary = retry_pending_failures(
        engine,
        rows=[
            _row(record_id="a", parent="parent-job"),
            _row(record_id="b", parent="other-job", source_id="WFRE00000001", codebase="WFR"),
        ],
    )
    assert summary["resolved"] == 2
    assert len(dataverse.created) == 2
    assert {payload["ts_codebase"] for _entity, payload in dataverse.created} == {"WUK", "WFR"}
    assert JOB_FULL not in {payload["ts_jobtype"] for _entity, payload in dataverse.created}


def test_pending_filter_scopes_by_id_prefix_or_job_codebase():
    text = pending_filter(codebase="wde")
    assert (
        "(startswith(ts_sourceid,'WDE') or (ts_IntegrationJob/ts_codebase eq 'WDE'"
        " and not startswith(ts_sourceid,'W')))"
    ) in text


def test_fetch_keeps_only_rows_of_the_requested_codebase():
    old_job = {"ts_jobid": "job-1", "ts_codebase": "WUK"}
    dataverse = FakeDataverse(
        rows=[
            # A WDE ID under an older multi-country job stored as "WUK".
            {"ts_integrationfailureid": "f1", "ts_sourceid": "WDEH00000001", "ts_IntegrationJob": old_job},
            {"ts_integrationfailureid": "f2", "ts_sourceid": "WUKE00000002", "ts_IntegrationJob": old_job},
            # No codebase in the ID: the job's codebase decides.
            {"ts_integrationfailureid": "f3", "ts_sourceid": "addr-3", "ts_IntegrationJob": old_job},
        ]
    )
    assert [row["id"] for row in fetch_pending_failures(dataverse, codebase="WUK")] == ["f2", "f3"]
    assert [row["id"] for row in fetch_pending_failures(dataverse, codebase="wde")] == ["f1"]
    assert len(fetch_pending_failures(dataverse)) == 3


def test_a_failure_no_step_replays_is_ignored_not_left_pending():
    # A role failure recorded against an account (before 2026-10-24) has no
    # retry step. Left Pending it was picked again every run, oldest first,
    # and could fill the daily limit.
    dataverse = FakeDataverse()
    engine = Engine(dataverse)
    summary = retry_pending_failures(
        engine, rows=[_row(phase=PHASE_HCP_ACTIVITY, entity=ENTITY_ACCOUNT)]
    )
    assert summary["success"] is True
    assert summary["ignored"] == 1
    assert [payload["ts_retrystatus"] for _, _, payload in _failure_updates(dataverse)] == [RETRY_IGNORED]
    assert not any(entity_set == JOBS for entity_set, _ in dataverse.created)


@pytest.mark.parametrize("parent", ["parent-job", "job-that-cannot-be-found"])
def test_a_retry_job_is_named_retry_even_without_its_parent_link(parent):
    # The scheduler must never take a retry job for a Full job that covered
    # its window, and the ts_retryofjob link is set only when the lookup works.
    dataverse = FakeDataverse()
    engine = Engine(dataverse)
    retry_pending_failures(engine, rows=[_row(parent=parent)])
    [job] = [data for entity_set, data in dataverse.created if entity_set == JOBS]
    assert job["ts_name"].startswith("WUK Retry ")
    assert ("ts_Retryofjob@odata.bind" in job) is (parent == "parent-job")
    assert engine.tracer.retry is False  # the next job of this engine is not a retry


def test_an_interrupted_retry_is_not_recorded_as_succeeded():
    dataverse = FakeDataverse()
    engine = Engine(dataverse)

    def interrupted(*args, **kwargs):
        raise KeyboardInterrupt

    engine._sync_hcos = interrupted
    with pytest.raises(KeyboardInterrupt):
        retry_pending_failures(engine, rows=[_row()])
    final = [data for entity_set, _id, data in dataverse.updated if entity_set == JOBS][-1]
    assert final["ts_status"] != STATUS_SUCCEEDED


class ActivityOneKey:
    """Activities by individual and by id. Like OneKey outside the US, it refuses
    a search by individual id (HTTP 400, OKWS-3007)."""

    def __init__(self, activities):
        self.activities = {record["activity"]["activityEid"]: record for record in activities}
        self.calls = []

    def search(self, entity_type, fields, cod_base=None, result_size=None, **kwargs):
        self.calls.append(("search", entity_type))
        wanted = fields[0]["values"][0]
        return [
            {"activity": {key: record["activity"][key]
                          for key in ("activityEid", "individualEid", "workplaceEid", "isMainActivity", "stateCode")}}
            for record in self.activities.values()
            if record["activity"]["individualEid"] == wanted
        ]

    def get_by_id(self, eid, entity_type, cod_base=None):
        self.calls.append(("get_by_id", entity_type))
        if entity_type == "INDIVIDUAL":
            raise DataverseHTTPError("OKWS-3007 individual search is US only", status_code=400)
        if entity_type == "WORKPLACE":
            return {"codBase": cod_base, "workplace": {"workplaceEid": eid, "officialName": "Ward"}}
        return self.activities.get(eid)


class ContactEngine(Engine):
    """Accounts exist for ``accounts``; ``retired`` ones the out-of-contract cleanup retired."""

    def __init__(self, dataverse, onekey, accounts=("WUKH1", "WUKH2"), retired=()):
        super().__init__(dataverse, onekey)
        self.composed = []
        self.update_only = set()
        self.accounts = set(accounts)
        self._retired_account_eids = set(retired)

    def _active_activity(self, activity):
        return activity.get("stateCode") != "INA"

    def _lookup_accounts_bulk(self, workplace_eids):
        return {eid: f"account-{eid}" for eid in workplace_eids if eid in self.accounts}

    def _sync_hcps(self, codebase, composed=None, update_only=None, **kwargs):
        self.composed.extend(composed or [])
        self.update_only |= set(update_only or ())
        return {"processed": len(composed or []), "created": 0, "updated": len(composed or []), "failed": 0}


def _activity_record(activity_eid, workplace_eid, main, state="ACT"):
    return {
        "codBase": "WUK",
        "activity": {"activityEid": activity_eid, "individualEid": "WUKI1", "workplaceEid": workplace_eid,
                     "isMainActivity": main, "stateCode": state},
        "individual": {"individualEid": "WUKI1", "lastName": "Doe",
                       "ada": {"SP,1": {"listCode": "SP", "code": "AA", "rank": 1}}},
    }


def test_a_contact_is_retried_through_its_activity_never_by_individual_id():
    # Outside the US OneKey refuses a search by individual id, so a contact
    # failure row was never replayed: every retry failed until it was Ignored.
    onekey = ActivityOneKey([_activity_record("WUKA2", "WUKH2", False), _activity_record("WUKA1", "WUKH1", True)])
    engine = ContactEngine(FakeDataverse(), onekey)
    summary = retry_pending_failures(
        engine, rows=[_row(source_id="WUKI1", phase=PHASE_HCP, entity=ENTITY_CONTACT)]
    )
    [row] = engine.composed
    assert row["individual"]["individualEid"] == "WUKI1" and row["individual"]["ada"]  # full detail
    assert row["workplace"]["workplaceEid"] == "WUKH1"  # the main activity places it
    assert ("get_by_id", "INDIVIDUAL") not in onekey.calls
    assert summary["success"] is True and summary["resolved"] == 1


def test_a_contact_with_only_inactive_activities_is_retried_without_placement():
    onekey = ActivityOneKey([_activity_record("WUKA1", "WUKH1", True, state="INA")])
    engine = ContactEngine(FakeDataverse(), onekey)
    retry_pending_failures(engine, rows=[_row(source_id="WUKI1", phase=PHASE_HCP, entity=ENTITY_CONTACT)])
    [row] = engine.composed
    assert row["individual"]["individualEid"] == "WUKI1"
    assert row["workplace"] == {} and row["activity"] == {}


def test_a_contact_retry_places_it_only_at_a_contracted_workplace():
    # The main activity is at a workplace the cleanup retired; the full sync
    # places the contact through the secondary one, and so must the retry.
    onekey = ActivityOneKey([_activity_record("WUKA1", "WUKH9", True), _activity_record("WUKA2", "WUKH2", False)])
    engine = ContactEngine(FakeDataverse(), onekey, accounts={"WUKH2", "WUKH9"}, retired={"WUKH9"})
    retry_pending_failures(engine, rows=[_row(source_id="WUKI1", phase=PHASE_HCP, entity=ENTITY_CONTACT)])
    [row] = engine.composed
    assert row["workplace"]["workplaceEid"] == "WUKH2"
    assert engine.update_only == set()


def test_a_contact_retry_with_no_contracted_workplace_only_updates():
    # As in the full sync: no activity at a contracted workplace, so the
    # contact is refreshed if it exists but never created.
    onekey = ActivityOneKey([_activity_record("WUKA1", "WUKH9", True), _activity_record("WUKA2", "WUKH8", False)])
    engine = ContactEngine(FakeDataverse(), onekey, accounts={"WUKH9"}, retired={"WUKH9"})
    retry_pending_failures(engine, rows=[_row(source_id="WUKI1", phase=PHASE_HCP, entity=ENTITY_CONTACT)])
    [row] = engine.composed
    assert row["workplace"] == {} and row["activity"] == {}
    assert engine.update_only == {"WUKI1"}


def test_a_contact_retry_keeps_the_individuals_privacy_flag():
    record = _activity_record("WUKA1", "WUKH1", True, state="INA")
    record["isPrivacyLaw"] = False  # the activity record's own flag
    record["individual"]["isPrivacyLaw"] = True
    engine = ContactEngine(FakeDataverse(), ActivityOneKey([record]))
    retry_pending_failures(engine, rows=[_row(source_id="WUKI1", phase=PHASE_HCP, entity=ENTITY_CONTACT)])
    [row] = engine.composed
    assert row["isPrivacyLaw"] is True  # HCP-013 msdyn_gdproptout


def test_an_outage_during_a_retry_does_not_spend_the_rows_attempts():
    # Two deliveries a day used up the 5 attempts of good rows in about two
    # days of a OneKey or Dataverse outage, and Ignored rows never come back.
    dataverse = FakeDataverse()
    engine = Engine(dataverse)

    def outage(*args, **kwargs):
        raise DataverseHTTPError("Service unavailable", status_code=503)

    engine._sync_hcos = outage
    summary = retry_pending_failures(engine, rows=[_row(attempt=4)])
    assert summary["success"] is False and summary["pending"] == 1
    final = _failure_updates(dataverse)[-1][2]
    assert (final["ts_retrystatus"], final["ts_attemptcount"]) == (RETRY_PENDING, 4)
