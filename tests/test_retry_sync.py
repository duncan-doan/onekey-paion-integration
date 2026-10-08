"""Offline tests for the integration failure retry queue."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from dataverse.exceptions import DataverseHTTPError
from integration.dynamic_sync import DynamicSyncEngine
from integration.job_trace import (
    ENTITY_ACCOUNT,
    ENTITY_AFFILIATION,
    FAILURES,
    JOB_DELTA,
    JOB_FULL,
    JOB_HCO,
    JOBS,
    MAX_RETRY_ATTEMPTS,
    PHASE_HCO,
    RETRY_IGNORED,
    RETRY_PENDING,
    RETRY_RESOLVED,
    RETRY_RETRIED,
    STATUS_RUNNING,
    TRIGGER_MANUAL,
    TRIGGER_TIMER,
    JobTracer,
)
from integration.retry_sync import (
    _normalize_failure,
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


def test_scheduled_job_retries_then_delta_then_resolves():
    dataverse = FakeDataverse(
        rows=[{"ts_integrationfailureid": "fail-9", "ts_sourceid": "WUKE00001259"}]
    )
    engine = Engine(dataverse)
    # Left behind by a retry group; the delta must not inherit it.
    engine.tracer.retry_of_job_id = "parent-job"
    seen = {}

    def sync_delta(**kwargs):
        seen["retry_of_job_id"] = engine.tracer.retry_of_job_id
        engine.succeeded_ids.add("WUKE00001259")
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
    assert iso_from == "2026-09-29T23:45:04Z"
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
        rows=[{"ts_integrationfailureid": "fail-9", "ts_sourceid": "WUKE00001259"}]
    )
    engine = Engine(dataverse)

    def sync_full(**kwargs):
        engine.calls.append(("full", kwargs["codebases"]))
        engine.succeeded_ids.add("WUKE00001259")
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
    assert dataverse.queries[0][1]["filter"] == f"ts_retrystatus eq {RETRY_PENDING}"
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
    # fail-2's outcome is still written; fail-1 counts as a failed attempt.
    assert summary["resolved"] == 1
    assert summary["pending"] == 1
    last = {record_id: data for _, record_id, data in _failure_updates(dataverse)}
    assert last["fail-2"]["ts_retrystatus"] == RETRY_RESOLVED
    assert last["fail-1"]["ts_retrystatus"] == RETRY_PENDING
    assert last["fail-1"]["ts_attemptcount"] == 2


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
