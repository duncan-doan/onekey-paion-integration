"""Offline tests for Dataverse integration-job tracing."""

from dataverse.exceptions import DataverseHTTPError
from integration.dynamic_sync import DynamicSyncEngine
from integration.job_trace import (
    FAILURES,
    JOB_AFFILIATIONS,
    JOB_DELTA,
    JOB_FULL,
    JOB_HCO,
    JOB_HCP,
    JOBS,
    PHASE_HCO,
    STATUS_FAILED,
    STATUS_PARTIAL,
    STATUS_RUNNING,
    STATUS_SUCCEEDED,
    JobTracer,
    summarize_results,
    trace_status,
)
from integration.sync_logger import SyncLogger


class FakeDataverse:
    def __init__(self, fail_create=False):
        self.fail_create = fail_create
        self.created = []
        self.updated = []
        self.batches = []

    def create(self, entity_set, data, return_record=False):
        if self.fail_create and entity_set == JOBS:
            raise DataverseHTTPError("down", status_code=503)
        self.created.append((entity_set, data))
        return f"guid-{len(self.created)}"

    def update(self, entity_set, record_id, data):
        self.updated.append((entity_set, record_id, data))
        return record_id

    def create_multiple(self, entity_set, records, logical_name=None):
        self.batches.append((entity_set, logical_name, records))
        return [f"f-{i}" for i in range(len(records))]

    def retrieve_multiple(self, *args, **kwargs):
        return []

    def find_one(self, entity_set, filter, select=None):
        if "retry-job" in filter:
            return {"ts_integrationjobid": "retry-guid"}
        return None

    def find_many_by_field(self, *args, **kwargs):
        return {}


class EmptyOneKey:
    def iter_population(self, **kwargs):
        return iter(())

    def close(self):
        pass


def test_summarize_includes_hierarchy_misses():
    totals = summarize_results(
        {
            "accounts": {"processed": 2, "created": 1, "updated": 1, "failed": 0},
            "hierarchy": {
                "linked": 1,
                "failed": 1,
                "already_linked": 3,
                "missing_child": 2,
                "missing_parent": 4,
            },
        }
    )
    assert totals["processed"] == 3
    assert totals["updated"] == 2
    assert totals["skipped"] == 3
    assert totals["failed"] == 7


def test_trace_status_values():
    clean = {"success": True, "accounts": {"processed": 1, "created": 1, "updated": 0, "failed": 0}}
    assert trace_status(clean, aborted=False, failure_rows=0) == STATUS_SUCCEEDED

    partial = {"success": True, "accounts": {"processed": 1, "created": 0, "updated": 0, "failed": 1}}
    assert trace_status(partial, aborted=False, failure_rows=1) == STATUS_PARTIAL

    assert trace_status({"success": False, "errors": ["bad"]}, aborted=False, failure_rows=0) == STATUS_FAILED
    assert trace_status(clean, aborted=True, failure_rows=0) == STATUS_PARTIAL


def test_begin_creates_a_running_job_and_finish_marks_success():
    dataverse = FakeDataverse()
    tracer = JobTracer(dataverse)
    tracer.begin(job_type=JOB_FULL, codebases=["WUK", "WFR"])
    entity, payload = dataverse.created[0]
    assert entity == JOBS
    assert payload["ts_status"] == STATUS_RUNNING
    assert payload["ts_jobtype"] == JOB_FULL
    assert payload["ts_codebase"] == "WUK,WFR"
    assert payload["ts_name"].startswith("WUK,WFR Full ")
    assert payload["ts_dryrun"] is False

    results = {"success": True, "accounts": {"processed": 1, "created": 1, "updated": 0, "failed": 0}}
    tracer.finish(results)
    status = dataverse.updated[-1][2]["ts_status"]
    assert status == STATUS_SUCCEEDED
    assert results["job_id"] == payload["ts_jobid"]
    assert results["trace_failures"] == 0


def test_failure_rows_are_batched_and_retried_status_is_partial():
    dataverse = FakeDataverse()
    tracer = JobTracer(dataverse, batch_size=10)
    tracer.retry_of_job_id = "retry-job"
    tracer.begin(
        job_type=JOB_DELTA,
        codebases=["WUK"],
        date_from="2026-09-01",
        date_to="2026-09-02T23:59:59Z",
    )
    job = dataverse.created[0][1]
    assert job["ts_jobtype"] == JOB_DELTA
    assert job["ts_watermarkfrom"] == "2026-09-01T00:00:00Z"
    assert job["ts_watermarkto"] == "2026-09-02T23:59:59Z"
    assert job["ts_Retryofjob@odata.bind"] == f"/{JOBS}(retry-guid)"

    exc = DataverseHTTPError("throttled", status_code=429, code="0x80072321")
    tracer.record_failure(
        phase=PHASE_HCO,
        entity="account",
        source_id="WUKE00001260",
        operation="upsert",
        message="throttled",
        external_id="WUKE00001260",
        exc=exc,
    )
    tracer.record_failure(
        phase=PHASE_HCO,
        entity="account",
        source_id="WUKE00001260",
        operation="upsert",
        message="duplicate",
        exc=exc,
    )
    results = {
        "success": True,
        "accounts": {"processed": 0, "created": 0, "updated": 0, "failed": 1},
        "errors": [],
    }
    tracer.finish(results)

    assert len(dataverse.batches) == 1
    entity, logical, rows = dataverse.batches[0]
    assert entity == FAILURES
    assert logical == "ts_integrationfailure"
    assert len(rows) == 1
    row = rows[0]
    assert row["ts_sourceid"] == "WUKE00001260"
    assert row["ts_entitytype"] == 126870001
    assert row["ts_phase"] == PHASE_HCO
    assert row["ts_operation"] == 126870004
    assert row["ts_errorcategory"] == 126870005
    assert row["ts_httpstatus"] == 429
    assert row["ts_errorcode"] == "0x80072321"
    assert row["ts_isretryable"] is True
    assert row["ts_retrystatus"] == 126870001
    assert row["ts_failurekey"] == f"{job['ts_jobid']}|HCO|WUKE00001260|Upsert"
    assert dataverse.updated[-1][2]["ts_status"] == STATUS_PARTIAL
    assert dataverse.updated[-1][2]["ts_failedcount"] == 1


def test_trace_errors_do_not_escape():
    dataverse = FakeDataverse(fail_create=True)
    tracer = JobTracer(dataverse)
    tracer.begin(job_type=JOB_HCO, codebases=["WUK"])
    tracer.record_failure(
        phase=PHASE_HCO,
        entity="account",
        source_id="WUKE1",
        operation="validate",
        message="no",
        category="validation",
        retryable=False,
    )
    tracer.checkpoint({"success": False})
    tracer.finish({"success": False, "errors": ["down"]}, aborted=True)
    assert dataverse.batches == []
    assert dataverse.updated == []


def test_dry_run_skips_dataverse_trace():
    dataverse = FakeDataverse()
    tracer = JobTracer(dataverse)
    tracer.begin(job_type=JOB_FULL, codebases=["WUK"], dry_run=True)
    tracer.record_failure(
        phase=PHASE_HCO,
        entity="account",
        source_id="WUKE1",
        operation="upsert",
        message="no",
    )
    tracer.finish({"success": True})
    assert dataverse.created == []
    assert dataverse.updated == []


def _engine(tmp_path):
    dataverse = FakeDataverse()
    engine = DynamicSyncEngine(
        EmptyOneKey(),
        dataverse,
        sync_logger=SyncLogger(log_dir=tmp_path),
    )
    return engine, dataverse


def test_each_sync_mode_opens_an_integration_job(tmp_path):
    cases = [
        ("full", lambda engine: engine.sync_full(["WUK"], sync_picklists=False), JOB_FULL),
        ("hco", lambda engine: engine.sync_full(["WUK"], hco_only=True, sync_picklists=False), JOB_HCO),
        ("hcp", lambda engine: engine.sync_full(["WUK"], hcp_only=True, sync_picklists=False), JOB_HCP),
        (
            "delta",
            lambda engine: engine.sync_delta(
                ["WUK"],
                "2026-09-01T00:00:00Z",
                "2026-09-02T23:59:59Z",
                sync_picklists=False,
            ),
            JOB_DELTA,
        ),
        (
            "affiliations",
            lambda engine: engine.sync_affiliations_only(["WUK"]),
            JOB_AFFILIATIONS,
        ),
    ]
    for name, run, job_type in cases:
        engine, dataverse = _engine(tmp_path / name)
        result = run(engine)
        assert result["success"] is True, name
        assert result["trace_status"] == "Succeeded", name
        payload = dataverse.created[0][1]
        assert payload["ts_jobtype"] == job_type, name
        assert dataverse.updated[-1][2]["ts_status"] == STATUS_SUCCEEDED


def test_engine_records_a_failed_source_id(tmp_path):
    engine, dataverse = _engine(tmp_path)
    engine._begin_job(JOB_FULL, ["WUK"], dry_run=False)
    engine._trace_phase = PHASE_HCO
    engine._fail(
        "account",
        "WUKE00001260",
        "upsert",
        "missing parent",
        category="lookup",
        retryable=False,
    )
    results = {
        "success": True,
        "accounts": {"processed": 0, "created": 0, "updated": 0, "failed": 1},
        "errors": [],
    }
    engine._finish_job(results)
    assert dataverse.batches[0][2][0]["ts_sourceid"] == "WUKE00001260"
    assert results["trace_status"] == "Partially Succeeded"
