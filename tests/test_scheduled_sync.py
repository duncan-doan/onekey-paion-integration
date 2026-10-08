"""Offline tests for the scheduled sync window, busy check, and per-codebase retry."""

from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from integration import scheduled_sync
from integration.job_trace import JOBS, STATUS_RUNNING
from integration.scheduled_sync import (
    ACTIVE_JOB_HEARTBEAT,
    IF_BUSY_RAISE,
    IF_BUSY_SKIP,
    SyncBusy,
    active_job,
    delta_window,
    last_window_start,
    run_from_env,
    run_retry_then_delta,
    sync_choice_picklists,
    window_end,
)


class Jobs:
    """retrieve_multiple stub that records queries and returns fixed rows."""

    def __init__(self, rows=None):
        self.rows = rows or []
        self.queries = []

    def retrieve_multiple(self, entity_set, **kwargs):
        self.queries.append((entity_set, kwargs))
        return list(self.rows)


# -- window anchor -------------------------------------------------------------


def test_window_end_is_the_earlier_of_watermark_and_start():
    queued = {"ts_startedon": "2026-10-07T00:42:00Z", "ts_watermarkto": "2026-10-07T00:00:00Z"}
    assert window_end(queued) == datetime(2026, 10, 7, 0, 0, tzinfo=timezone.utc)
    full = {"ts_startedon": "2026-10-01T00:00:05Z", "ts_watermarkto": None}
    assert window_end(full) == datetime(2026, 10, 1, 0, 0, 5, tzinfo=timezone.utc)


def test_a_late_start_does_not_skip_the_wait():
    # Enqueued with date_to=00:00, failed once, ran at 00:42: the next window
    # must start at 00:00 - 15 min, not at 00:42 - 15 min.
    jobs = Jobs([
        {
            "ts_jobid": "delta-7",
            "ts_startedon": "2026-10-07T00:42:00Z",
            "ts_watermarkto": "2026-10-07T00:00:00Z",
            "ts_errorsummary": None,
        }
    ])
    iso_from, _ = delta_window(jobs, ["WUK"], date_to="2026-10-08T00:00:00Z")
    assert iso_from == "2026-10-06T23:45:00Z"
    assert "ts_watermarkto" in jobs.queries[0][1]["select"]


def test_a_replay_of_an_old_window_does_not_move_the_start():
    jobs = Jobs([
        # Manual replay of 15-22 Aug, started today after the nightly delta.
        {
            "ts_jobid": "replay",
            "ts_startedon": "2026-10-07T10:00:00Z",
            "ts_watermarkto": "2026-08-22T00:00:00Z",
            "ts_errorsummary": None,
        },
        {
            "ts_jobid": "nightly",
            "ts_startedon": "2026-10-07T00:00:20Z",
            "ts_watermarkto": "2026-10-07T00:00:10Z",
            "ts_errorsummary": None,
        },
    ])
    assert last_window_start(jobs, ["WUK"]) == "2026-10-07T00:00:10Z"


def test_a_job_that_lost_its_window_is_not_an_anchor():
    jobs = Jobs([
        {
            "ts_jobid": "broken",
            "ts_startedon": "2026-10-07T00:00:20Z",
            "ts_watermarkto": "2026-10-07T00:00:10Z",
            "ts_errorsummary": "WUK: OneKey 503",
        },
        {
            "ts_jobid": "previous",
            "ts_startedon": "2026-10-06T00:00:20Z",
            "ts_watermarkto": "2026-10-06T00:00:10Z",
            "ts_errorsummary": None,
        },
    ])
    assert last_window_start(jobs, ["WUK"]) == "2026-10-06T00:00:10Z"


# -- busy check ----------------------------------------------------------------


def test_active_job_looks_for_a_fresh_running_job_per_codebase():
    jobs = Jobs()
    now = datetime(2026, 10, 7, 12, 0, tzinfo=timezone.utc)
    assert active_job(jobs, ["WUK", "WDE"], now=now) is None
    assert [query[0] for query in jobs.queries] == [JOBS, JOBS]
    text = jobs.queries[1][1]["filter"]
    assert "ts_codebase eq 'WDE'" in text
    assert f"ts_status eq {STATUS_RUNNING}" in text
    fresh = (now - ACTIVE_JOB_HEARTBEAT).strftime("%Y-%m-%dT%H:%M:%SZ")
    assert f"ts_lastheartbeat ge {fresh}" in text


def test_active_job_returns_the_live_job_and_its_codebase():
    jobs = Jobs([{"ts_jobid": "job-1", "ts_lastheartbeat": "2026-10-07T11:58:00Z"}])
    running = active_job(jobs, ["WUK"])
    assert running["ts_jobid"] == "job-1"
    assert running["codebase"] == "WUK"


@pytest.fixture
def fake_engine(monkeypatch):
    engine = SimpleNamespace(dataverse=Jobs())
    ran = []

    @contextmanager
    def fake_engine_from_env(config_dir=None, bulk_batch_size=None):
        yield engine

    monkeypatch.setattr(scheduled_sync, "engine_from_env", fake_engine_from_env)
    monkeypatch.setattr(
        scheduled_sync,
        "run_retry_then_delta",
        lambda engine, **kwargs: ran.append(("delta", kwargs)) or {"success": True},
    )
    monkeypatch.setattr(
        scheduled_sync,
        "run_full",
        lambda engine, **kwargs: ran.append(("full", kwargs)) or {"success": True},
    )
    engine.ran = ran
    return engine


def test_busy_delta_is_skipped(fake_engine):
    fake_engine.dataverse.rows = [{"ts_jobid": "job-0", "ts_lastheartbeat": "x"}]
    result = run_from_env(codebases=["WUK"], if_busy=IF_BUSY_SKIP)
    assert result == {"success": True, "skipped": True, "busy_job": "job-0"}
    assert fake_engine.ran == []


def test_busy_full_sync_raises(fake_engine):
    fake_engine.dataverse.rows = [{"ts_jobid": "job-0", "ts_lastheartbeat": "x"}]
    with pytest.raises(SyncBusy, match="Codebase WUK is busy: job job-0"):
        run_from_env(codebases=["WUK"], full=True, if_busy=IF_BUSY_RAISE)
    assert fake_engine.ran == []


def test_idle_codebase_runs_and_passes_the_choices_flag(fake_engine):
    run_from_env(codebases=["WUK"], full=True, if_busy=IF_BUSY_RAISE, sync_choices=False)
    assert fake_engine.ran == [
        ("full", {"codebases": ["WUK"], "dry_run": False, "trigger": scheduled_sync.TRIGGER_TIMER, "sync_choices": False})
    ]


def test_cli_runs_do_not_check_for_a_busy_codebase(fake_engine):
    fake_engine.dataverse.rows = [{"ts_jobid": "job-0"}]
    run_from_env(codebases=["WUK"])
    assert fake_engine.dataverse.queries == []
    assert fake_engine.ran[0][0] == "delta"


# -- per-codebase retry --------------------------------------------------------


def test_each_codebase_retries_only_its_own_failures(monkeypatch):
    retried = []

    def fake_retry(engine, *, codebase, limit, dry_run, trigger):
        retried.append(codebase)
        return {"success": True, "selected": 2, "resolved": 1, "pending": 1,
                "ignored": 0, "jobs": [f"retry-{codebase}"], "errors": []}

    monkeypatch.setattr(scheduled_sync, "retry_pending_failures", fake_retry)
    monkeypatch.setattr(scheduled_sync, "resolve_pending_for_ids", lambda *a, **k: 0)
    engine = SimpleNamespace(
        tracer=SimpleNamespace(trigger=None, retry_of_job_id=None, job_guid=None),
        succeeded_ids=set(),
        dataverse=Jobs(),
        sync_delta=lambda **kwargs: {"success": True},
    )
    result = run_retry_then_delta(
        engine, codebases=["wuk", "WDE", "WUK"], date_from="2026-10-06", date_to="2026-10-07"
    )
    assert retried == ["WUK", "WDE"]
    assert result["retry"]["selected"] == 4
    assert result["retry"]["jobs"] == ["retry-WUK", "retry-WDE"]


# -- choice picklists ----------------------------------------------------------


def choice_engine(monkeypatch, summaries=None, error=None):
    calls = []

    def sync_picklists(codebases=None, **kwargs):
        calls.append(codebases)
        if error:
            raise error
        return summaries or []

    @contextmanager
    def fake_engine_from_env(config_dir=None, bulk_batch_size=None):
        yield SimpleNamespace(sync_picklists=sync_picklists)

    monkeypatch.setattr(scheduled_sync, "engine_from_env", fake_engine_from_env)
    return calls


def test_choice_picklists_sync_once_without_codebases(monkeypatch):
    calls = choice_engine(monkeypatch, summaries=[{"errors": 0}, {"errors": 0}])
    assert sync_choice_picklists() is True
    # No codebases: the lookup tables stay with each country's worker.
    assert calls == [None]


def test_choice_picklist_errors_leave_it_to_the_workers(monkeypatch):
    choice_engine(monkeypatch, summaries=[{"errors": 0}, {"errors": 2}])
    assert sync_choice_picklists() is False
    choice_engine(monkeypatch, error=RuntimeError("metadata denied"))
    assert sync_choice_picklists() is False
