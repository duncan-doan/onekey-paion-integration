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
    retry_from_env,
    run_delta,
    run_from_env,
    run_retry,
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
    monkeypatch.setattr(
        scheduled_sync,
        "run_delta",
        lambda engine, **kwargs: ran.append(("delta only", kwargs)) or {"success": True},
    )
    monkeypatch.setattr(
        scheduled_sync,
        "run_retry",
        lambda engine, **kwargs: ran.append(("retry", kwargs)) or {"success": True},
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


def test_the_delta_queue_runs_the_delta_without_the_retry(fake_engine):
    run_from_env(codebases=["WUK"], retry=False, if_busy=IF_BUSY_SKIP)
    assert [kind for kind, _ in fake_engine.ran] == ["delta only"]


def test_the_retry_queue_replays_failures_only(fake_engine):
    retry_from_env(codebases=["WUK"], limit=25, if_busy=IF_BUSY_SKIP)
    assert fake_engine.ran == [
        ("retry", {"codebases": ["WUK"], "dry_run": False, "trigger": scheduled_sync.TRIGGER_TIMER, "limit": 25})
    ]


def test_a_busy_retry_is_skipped(fake_engine):
    fake_engine.dataverse.rows = [{"ts_jobid": "job-0", "ts_lastheartbeat": "x"}]
    result = retry_from_env(codebases=["WUK"], if_busy=IF_BUSY_SKIP)
    assert result == {"success": True, "skipped": True, "busy_job": "job-0"}
    assert fake_engine.ran == []


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
    assert result["delta"] == {"success": True}


def test_run_retry_replays_without_a_delta(monkeypatch):
    def fake_retry(engine, *, codebase, limit, dry_run, trigger):
        return {"success": codebase != "WDE", "selected": 1, "jobs": [], "errors": []}

    monkeypatch.setattr(scheduled_sync, "retry_pending_failures", fake_retry)
    engine = SimpleNamespace(sync_delta=lambda **kwargs: pytest.fail("ran the delta"))
    result = run_retry(engine, codebases=["WUK", "WDE"])
    assert result["success"] is False
    assert result["retry"]["selected"] == 2


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


# -- SYNC_DELTA_FROM -------------------------------------------------------------


@pytest.fixture
def no_delta_setting(monkeypatch):
    monkeypatch.delenv(scheduled_sync.DELTA_FROM_SETTING, raising=False)
    return monkeypatch


def delta_start(fake_engine):
    kind, kwargs = fake_engine.ran[-1]
    assert kind == "delta"
    return kwargs["date_from"]


def test_delta_start_comes_from_the_setting(fake_engine, no_delta_setting):
    no_delta_setting.setenv("SYNC_DELTA_FROM", "2026-08-01T06:30:00Z")
    run_from_env(codebases=["WUK"])
    assert delta_start(fake_engine) == "2026-08-01T06:30:00Z"


def test_a_bare_date_starts_at_midnight_utc(fake_engine, no_delta_setting):
    no_delta_setting.setenv("SYNC_DELTA_FROM", "2026-08-01")
    run_from_env(codebases=["WUK"])
    assert delta_start(fake_engine) == "2026-08-01T00:00:00Z"


def test_without_the_setting_the_window_is_worked_out_as_before(fake_engine, no_delta_setting):
    run_from_env(codebases=["WUK"])
    assert delta_start(fake_engine) is None  # delta_window uses the last covering job


def test_an_explicit_date_from_beats_the_setting(fake_engine, no_delta_setting):
    no_delta_setting.setenv("SYNC_DELTA_FROM", "2026-08-01")
    run_from_env(codebases=["WUK"], date_from="2026-09-15")
    assert delta_start(fake_engine) == "2026-09-15"


@pytest.mark.parametrize("value", ["not a date", "2999-01-01"])
def test_an_unusable_setting_is_ignored(fake_engine, no_delta_setting, value, caplog):
    no_delta_setting.setenv("SYNC_DELTA_FROM", value)
    run_from_env(codebases=["WUK"])
    assert delta_start(fake_engine) is None
    assert "SYNC_DELTA_FROM" in caplog.text


def test_a_setting_after_a_fixed_window_end_is_ignored(fake_engine, no_delta_setting):
    no_delta_setting.setenv("SYNC_DELTA_FROM", "2026-08-10")
    run_from_env(codebases=["WUK"], date_to="2026-08-05")
    assert delta_start(fake_engine) is None


def test_the_full_sync_ignores_the_setting(fake_engine, no_delta_setting):
    no_delta_setting.setenv("SYNC_DELTA_FROM", "2026-08-01")
    run_from_env(codebases=["WUK"], full=True)
    kind, kwargs = fake_engine.ran[-1]
    assert kind == "full" and "date_from" not in kwargs


# -- first run of a codebase -----------------------------------------------------


def promotion_engine(monkeypatch, job_rows):
    calls = []
    monkeypatch.setattr(
        scheduled_sync,
        "retry_pending_failures",
        lambda engine, **kwargs: calls.append(("retry", kwargs["codebase"]))
        or {"success": True, "selected": 0, "jobs": [], "errors": []},
    )
    monkeypatch.setattr(scheduled_sync, "resolve_pending_for_ids", lambda *a, **k: 0)
    engine = SimpleNamespace(
        tracer=SimpleNamespace(trigger=None, retry_of_job_id=None, job_guid=None),
        succeeded_ids=set(),
        dataverse=Jobs(job_rows),
        sync_full=lambda **kwargs: calls.append(("full", kwargs)) or {"success": True, "job_id": "full-1"},
        sync_delta=lambda **kwargs: calls.append(("delta", kwargs)) or {"success": True, "job_id": "delta-1"},
    )
    return engine, calls


def test_a_codebase_without_a_completed_job_gets_a_full_sync(monkeypatch):
    engine, calls = promotion_engine(monkeypatch, [])
    result = run_retry_then_delta(engine, codebases=["WNL"], sync_choices=False)
    assert [name for name, _ in calls] == ["full"]  # no retry, no delta
    assert calls[0][1] == {"codebases": ["WNL"], "dry_run": False, "sync_choices": False}
    assert result["promoted_to_full"] is True
    assert result["full"]["job_id"] == "full-1"


def test_a_codebase_with_a_completed_job_gets_its_delta(monkeypatch):
    engine, calls = promotion_engine(
        monkeypatch,
        [{"ts_jobid": "d-1", "ts_startedon": "2026-10-09T00:00:20Z",
          "ts_watermarkto": "2026-10-09T00:00:10Z", "ts_errorsummary": None}],
    )
    result = run_retry_then_delta(engine, codebases=["WNL"])
    assert [name for name, _ in calls] == ["retry", "delta"]
    assert calls[1][1]["date_from"] == "2026-10-08T23:45:10Z"
    assert "promoted_to_full" not in result
    # One read of the job history, not one for the check and one for the window.
    assert len(engine.dataverse.queries) == 1


def test_an_explicit_start_keeps_the_delta_without_a_job(monkeypatch):
    engine, calls = promotion_engine(monkeypatch, [])
    run_retry_then_delta(engine, codebases=["WNL"], date_from="2026-08-08")
    assert [name for name, _ in calls] == ["retry", "delta"]
    assert calls[1][1]["date_from"] == "2026-08-08T00:00:00Z"
    assert engine.dataverse.queries == []


def test_the_delta_alone_works_out_its_window_without_retrying(monkeypatch):
    engine, calls = promotion_engine(
        monkeypatch,
        [{"ts_jobid": "d-1", "ts_startedon": "2026-10-09T00:00:20Z",
          "ts_watermarkto": "2026-10-09T00:00:10Z", "ts_errorsummary": None}],
    )
    result = run_delta(engine, codebases=["WNL"])
    assert [name for name, _ in calls] == ["delta"]
    assert calls[0][1]["date_from"] == "2026-10-08T23:45:10Z"
    assert set(result) == {"success", "delta", "resolved_by_delta"}


def test_the_delta_alone_runs_a_full_sync_for_a_new_codebase(monkeypatch):
    engine, calls = promotion_engine(monkeypatch, [])
    result = run_delta(engine, codebases=["WNL"])
    assert [name for name, _ in calls] == ["full"]
    assert result["promoted_to_full"] is True
