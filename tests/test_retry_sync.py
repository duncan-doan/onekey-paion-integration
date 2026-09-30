"""Offline tests for the integration failure retry queue."""

from integration.dynamic_sync import DynamicSyncEngine
from integration.job_trace import (
    ENTITY_ACCOUNT,
    ENTITY_AFFILIATION,
    FAILURES,
    JOB_FULL,
    JOB_HCO,
    JOBS,
    PHASE_HCO,
    RETRY_IGNORED,
    RETRY_PENDING,
    RETRY_RESOLVED,
    RETRY_RETRIED,
    STATUS_RUNNING,
    JobTracer,
)
from integration.retry_sync import (
    fetch_pending_failures,
    pending_filter,
    resolve_pending_for_ids,
    retry_pending_failures,
)
from integration.scheduled_sync import delta_window, run_retry_then_delta


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
        self._contracted_workplace_eids = set()
        self._contracted_individual_eids = set()
        self.calls = []

    _begin_job = DynamicSyncEngine._begin_job
    _checkpoint = DynamicSyncEngine._checkpoint
    _finish_job = DynamicSyncEngine._finish_job

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
    assert summary["success"] is False
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


def test_scheduled_job_retries_then_delta_then_resolves(tmp_path):
    dataverse = FakeDataverse(
        rows=[{"ts_integrationfailureid": "fail-9", "ts_sourceid": "WUKE00001259"}]
    )
    engine = Engine(dataverse)

    def sync_delta(**kwargs):
        engine.succeeded_ids.add("WUKE00001259")
        return {"success": True, "job_id": "delta-job", "mode": "delta"}

    engine.sync_delta = sync_delta
    result = run_retry_then_delta(
        engine,
        codebases=["WUK"],
        date_from="2026-09-29T00:00:00Z",
        date_to="2026-09-30T23:59:59Z",
        watermark_date="2026-09-30",
        watermark_dir=tmp_path,
    )
    assert result["success"] is True
    assert result["resolved_by_delta"] == 1
    saved = (tmp_path / "sync_watermark_WUK.json").read_text(encoding="utf-8")
    assert "2026-09-30" in saved


def test_delta_window_uses_watermark(tmp_path):
    (tmp_path / "sync_watermark_WUK.json").write_text(
        '{"date_to": "2026-09-28"}', encoding="utf-8"
    )
    iso_from, iso_to, stored = delta_window(tmp_path, ["WUK"], date_to="2026-09-30")
    assert iso_from == "2026-09-28T00:00:00Z"
    assert iso_to == "2026-09-30T23:59:59Z"
    assert stored == "2026-09-30"


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
