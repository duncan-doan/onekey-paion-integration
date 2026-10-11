"""Offline tests for Dataverse integration-job tracing."""

from types import SimpleNamespace

from dataverse.exceptions import DataverseAmbiguousWriteError, DataverseHTTPError
from integration import job_trace
from integration.dynamic_sync import DynamicSyncEngine
from integration.job_trace import (
    FAILURES,
    HEARTBEAT_EVERY_SECONDS,
    HEARTBEAT_RETRY_SECONDS,
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
        self.update_attempts = []
        self.batches = []

    def create(self, entity_set, data, return_record=False):
        if self.fail_create and entity_set == JOBS:
            raise DataverseHTTPError("down", status_code=503)
        self.created.append((entity_set, data))
        return f"guid-{len(self.created)}"

    def update(self, entity_set, record_id, data, attempts=None):
        self.updated.append((entity_set, record_id, data))
        self.update_attempts.append(attempts)
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


def test_failed_trace_batch_is_retained_and_retried_without_service_error_fanout():
    dataverse = FakeDataverse()
    tracer = JobTracer(dataverse)
    tracer.begin(job_type=JOB_FULL, codebases=["WUK"])
    tracer.record_failure(
        phase=PHASE_HCO, entity="account", source_id="WUKH1",
        operation="upsert", message="timeout", exc=TimeoutError(),
    )
    original = dataverse.create_multiple

    def unavailable(*args, **kwargs):
        raise DataverseHTTPError("throttled", status_code=429)

    dataverse.create_multiple = unavailable
    tracer.flush()
    assert len(tracer._pending) == 1
    assert all(entity != FAILURES for entity, _ in dataverse.created)

    dataverse.create_multiple = original
    tracer.flush()
    assert tracer._pending == {}
    assert tracer.written_failures == 1


def test_unpersisted_failure_rows_prevent_a_covering_job():
    dataverse = FakeDataverse()
    tracer = JobTracer(dataverse)
    tracer.begin(job_type=JOB_FULL, codebases=["WUK"])
    tracer.record_failure(
        phase=PHASE_HCO, entity="account", source_id="WUKH1",
        operation="upsert", message="timeout", exc=TimeoutError(),
    )

    def unavailable(*args, **kwargs):
        raise DataverseHTTPError("service unavailable", status_code=503)

    dataverse.create_multiple = unavailable
    result = {"success": True, "accounts": {"processed": 1, "failed": 1}}
    tracer.finish(result)
    assert result["success"] is False
    assert "could not persist 1 failure" in result["errors"][0]
    assert "trace:" in dataverse.updated[-1][2]["ts_errorsummary"]


def test_single_trace_write_failure_is_retained_until_next_checkpoint():
    dataverse = FakeDataverse()
    tracer = JobTracer(dataverse)
    tracer.begin(job_type=JOB_FULL, codebases=["WUK"])
    tracer.record_failure(
        phase=PHASE_HCO, entity="account", source_id="WUKH1",
        operation="upsert", message="timeout", exc=TimeoutError(),
    )
    dataverse.create_multiple = None
    original = dataverse.create

    def unavailable(*args, **kwargs):
        # What the client raises when it cannot reach Dataverse.
        raise DataverseHTTPError("transport unavailable")

    dataverse.create = unavailable
    tracer.flush()
    assert len(tracer._pending) == 1
    dataverse.create = original
    tracer.flush()
    assert tracer._pending == {}
    assert tracer.written_failures == 1


class KeyedFailureDataverse(FakeDataverse):
    """Failure rows keyed by ts_failurekey, unique as in Dataverse.

    ``reject`` holds columns that make a row fail with a 400.
    """

    def __init__(self, reject=()):
        super().__init__()
        self.stored = {}
        self.reject = set(reject)
        self.create_calls = 0

    def _store(self, row):
        if self.reject & set(row):
            raise DataverseHTTPError("bad value", status_code=400)
        if row["ts_failurekey"] in self.stored:
            raise DataverseHTTPError("A record with matching key values already exists", status_code=412)
        self.stored[row["ts_failurekey"]] = row

    def create(self, entity_set, data, return_record=False):
        if entity_set == FAILURES:
            self.create_calls += 1
            self._store(data)
        return super().create(entity_set, data, return_record)

    def create_multiple(self, entity_set, records, logical_name=None):
        # All or nothing, like CreateMultiple on a standard table.
        if any(
            self.reject & set(row) or row["ts_failurekey"] in self.stored for row in records
        ):
            raise DataverseHTTPError("bad value", status_code=400)
        for row in records:
            self.stored[row["ts_failurekey"]] = row
        return super().create_multiple(entity_set, records, logical_name)

    def find_many_by_field(self, entity_set, field, values, select=None, **kwargs):
        return {value: {field: value} for value in values if value in self.stored}


def _three_failures(tracer, **kwargs):
    for n in range(1, 4):
        tracer.record_failure(
            phase=PHASE_HCO, entity="account", source_id=f"WUKH{n}",
            operation="upsert", message="bad", **kwargs,
        )


def test_failure_rows_that_landed_behind_a_gateway_timeout_are_not_resent():
    dataverse = KeyedFailureDataverse()
    tracer = JobTracer(dataverse)
    tracer.begin(job_type=JOB_FULL, codebases=["WUK"])
    _three_failures(tracer)

    def landed_but_timed_out(entity_set, rows, logical_name=None):
        for row in rows:
            dataverse._store(row)
        raise DataverseAmbiguousWriteError("gateway returned 504", status_code=504)

    dataverse.create_multiple = landed_but_timed_out
    result = {"success": True}
    tracer.finish(result)
    assert len(dataverse.stored) == 3
    assert tracer.written_failures == 3
    assert dataverse.create_calls == 0
    # Written rows must not hold the job back from covering its window.
    assert result["success"] is True
    assert "errors" not in result


def test_a_rejected_failure_row_is_written_without_optional_columns():
    dataverse = KeyedFailureDataverse(reject={"ts_externalid"})
    dataverse.create_multiple = None
    tracer = JobTracer(dataverse)
    tracer.begin(job_type=JOB_FULL, codebases=["WUK"])
    _three_failures(tracer, external_id="bad-key")
    result = {"success": True}
    tracer.finish(result)
    assert len(dataverse.stored) == 3
    assert all("ts_externalid" not in row for row in dataverse.stored.values())
    assert tracer.written_failures == 3
    assert result["success"] is True


def test_a_failure_row_dataverse_always_rejects_does_not_fail_the_job():
    # The same rows would fail on every run: blocking the window would stall it.
    dataverse = KeyedFailureDataverse(reject={"ts_sourceid"})
    tracer = JobTracer(dataverse)
    tracer.begin(job_type=JOB_FULL, codebases=["WUK"])
    _three_failures(tracer)
    result = {"success": True, "accounts": {"processed": 3, "failed": 3}}
    tracer.finish(result)
    assert tracer.dropped_failures == 3
    assert tracer._pending == {}
    assert result["success"] is True
    assert result["trace_failures_dropped"] == 3
    final = dataverse.updated[-1][2]
    assert "ts_errorsummary" not in final
    assert '"trace_failures_dropped":3' in final["ts_phasecounts"]


def test_a_rejected_row_that_already_exists_counts_as_written():
    dataverse = KeyedFailureDataverse()
    dataverse.create_multiple = None
    tracer = JobTracer(dataverse)
    tracer.begin(job_type=JOB_FULL, codebases=["WUK"])
    _three_failures(tracer)
    first = next(iter(tracer._pending.values()))
    dataverse.stored[first["ts_failurekey"]] = first  # resent after an unknown outcome
    tracer.flush()
    assert tracer.written_failures == 3
    assert tracer.dropped_failures == 0


def test_an_outage_while_writing_rows_singly_stops_at_the_first_row():
    dataverse = KeyedFailureDataverse()
    dataverse.create_multiple = None
    tracer = JobTracer(dataverse)
    tracer.begin(job_type=JOB_FULL, codebases=["WUK"])
    _three_failures(tracer)

    def unavailable(entity_set, data, return_record=False):
        dataverse.create_calls += 1
        raise DataverseHTTPError("service unavailable", status_code=503)

    dataverse.create = unavailable
    tracer.flush()
    assert dataverse.create_calls == 1
    assert len(tracer._pending) == 3


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


class UncertainBatchDataverse(FakeDataverse):
    """CreateMultiple stores the first failure row, then times out."""

    def __init__(self):
        super().__init__()
        self.stored_keys = set()

    def create_multiple(self, entity_set, records, logical_name=None):
        self.stored_keys.add(records[0]["ts_failurekey"])
        raise DataverseAmbiguousWriteError("read timed out")

    def find_many_by_field(self, entity_set, match_field, values, select=None, **kwargs):
        return {value: {match_field: value} for value in values if value in self.stored_keys}


def test_uncertain_failure_batch_writes_only_the_missing_rows():
    dataverse = UncertainBatchDataverse()
    tracer = JobTracer(dataverse)
    tracer.begin(job_type=JOB_DELTA, codebases=["WUK"])
    for source_id in ("WUKE00000001", "WUKE00000002"):
        tracer.record_failure(
            phase=PHASE_HCO,
            entity="account",
            source_id=source_id,
            operation="upsert",
            message="Dataverse timeout",
        )
    tracer.flush()
    written_one_by_one = [data for entity_set, data in dataverse.created if entity_set == FAILURES]
    assert [row["ts_sourceid"] for row in written_one_by_one] == ["WUKE00000002"]
    assert tracer.written_failures == 2


# -- heartbeat -------------------------------------------------------------------


class HookedDataverse(FakeDataverse):
    """Runs after_request after each write, as DataverseClient.request does."""

    def __init__(self):
        super().__init__()
        self.after_request = None

    def update(self, entity_set, record_id, data, attempts=None):
        super().update(entity_set, record_id, data, attempts=attempts)
        if self.after_request:
            self.after_request()
        return record_id


def heartbeats(dataverse):
    return [u for u in dataverse.updated if set(u[2]) == {"ts_lastheartbeat"}]


def test_beat_refreshes_the_heartbeat_at_most_every_interval(monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(job_trace.time, "monotonic", lambda: clock[0])
    dataverse = HookedDataverse()
    tracer = JobTracer(dataverse)
    tracer.begin(job_type=JOB_FULL, codebases=["WUK"])
    clock[0] += HEARTBEAT_EVERY_SECONDS - 1
    tracer.beat()
    assert heartbeats(dataverse) == []
    clock[0] += 1
    # The heartbeat write runs after_request again; it must not recurse.
    dataverse.after_request = tracer.beat
    tracer.beat()
    assert len(heartbeats(dataverse)) == 1
    tracer.beat()
    assert len(heartbeats(dataverse)) == 1


def test_heartbeat_is_sent_once_and_retried_a_minute_after_a_failure(monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(job_trace.time, "monotonic", lambda: clock[0])
    dataverse = HookedDataverse()
    tracer = JobTracer(dataverse)
    tracer.begin(job_type=JOB_FULL, codebases=["WUK"])
    working = dataverse.update

    def throttled(entity_set, record_id, data, attempts=None):
        dataverse.update_attempts.append(attempts)
        raise DataverseHTTPError("throttled", status_code=429)

    dataverse.update = throttled
    clock[0] += HEARTBEAT_EVERY_SECONDS
    tracer.beat()
    # Once, not retried: a retry would wait out the throttling inside the
    # request that triggered the heartbeat.
    assert dataverse.update_attempts == [1]

    dataverse.update = working
    clock[0] += HEARTBEAT_RETRY_SECONDS - 1
    tracer.beat()
    assert heartbeats(dataverse) == []
    clock[0] += 1
    tracer.beat()
    assert len(heartbeats(dataverse)) == 1
    assert dataverse.update_attempts[-1] == 1


def test_beat_does_nothing_once_the_job_finished(monkeypatch):
    clock = [1000.0]
    monkeypatch.setattr(job_trace.time, "monotonic", lambda: clock[0])
    dataverse = HookedDataverse()
    tracer = JobTracer(dataverse)
    tracer.begin(job_type=JOB_FULL, codebases=["WUK"])
    tracer.finish({"success": True})
    clock[0] += HEARTBEAT_EVERY_SECONDS * 10
    tracer.beat()
    assert heartbeats(dataverse) == []


class HookEngine:
    _begin_job = DynamicSyncEngine._begin_job
    _finish_job = DynamicSyncEngine._finish_job
    _http_snapshot = DynamicSyncEngine._http_snapshot
    _log_http_summary = DynamicSyncEngine._log_http_summary
    _set_request_hook = DynamicSyncEngine._set_request_hook

    def __init__(self):
        self.succeeded_ids = set()
        self.succeeded_records = set()
        self.dataverse = HookedDataverse()
        self.onekey = SimpleNamespace(session=SimpleNamespace(after_request=None))
        self.tracer = JobTracer(self.dataverse)
        self.sync_logger = SyncLogger()


def test_engine_hooks_both_clients_to_the_tracer_while_a_job_runs():
    engine = HookEngine()
    engine._begin_job(JOB_FULL, ["WUK"], dry_run=False)
    assert engine.dataverse.after_request == engine.tracer.beat
    assert engine.onekey.session.after_request == engine.tracer.beat
    engine._finish_job({"success": True})
    assert engine.dataverse.after_request is None
    assert engine.onekey.session.after_request is None


def test_a_scheduled_run_id_goes_into_the_job_name():
    dataverse = FakeDataverse()
    tracer = JobTracer(dataverse)
    tracer.run_id = "delta-20261011T000001Z"
    tracer.begin(job_type=JOB_FULL, codebases=["WDE"])
    name = dataverse.created[0][1]["ts_name"]
    assert name.startswith("WDE Full ")
    assert name.endswith(" run=delta-20261011T000001Z")
