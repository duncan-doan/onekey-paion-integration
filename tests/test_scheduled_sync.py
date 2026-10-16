"""Offline tests for the scheduled sync window, busy check, and per-codebase retry."""

from contextlib import contextmanager
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from integration import scheduled_sync
from integration.job_trace import JOBS, STATUS_ABANDONED, STATUS_RUNNING
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
    # must start at 00:00 less the overlap, not at 00:42 less it.
    jobs = Jobs([
        {
            "ts_jobid": "delta-7",
            "ts_startedon": "2026-10-07T00:42:00Z",
            "ts_watermarkto": "2026-10-07T00:00:00Z",
            "ts_errorsummary": None,
        }
    ])
    iso_from, _ = delta_window(jobs, ["WUK"], date_to="2026-10-08T00:00:00Z")
    assert iso_from == "2026-10-06T18:00:00Z"
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


def test_active_job_ignores_only_the_jobs_the_delivery_replaces():
    # A delta or full sync replaces its own part's jobs and the run's retry
    # jobs; the next part of the same run still counts as busy.
    jobs = Jobs()
    active_job(jobs, ["WDE"], own_run="delta-20261012T000001Z-7", part=2)
    assert jobs.queries[0][1]["filter"].endswith(
        " and not (contains(ts_name, 'run=delta-20261012T000001Z-7')"
        " and (_ts_retryofjob_value ne null or endswith(ts_name, ' part=2')))"
    )


def test_a_retry_replaces_only_the_runs_retry_jobs():
    jobs = Jobs()
    active_job(jobs, ["WDE"], own_run="delta-20261012T000001Z-7", retry=True)
    assert jobs.queries[0][1]["filter"].endswith(
        " and not (contains(ts_name, 'run=delta-20261012T000001Z-7')"
        " and _ts_retryofjob_value ne null)"
    )


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
    # A message without a run takes nothing over: only the busy check ran.
    assert len(fake_engine.dataverse.queries) == 1


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
    assert calls[1][1]["date_from"] == "2026-10-08T18:00:10Z"
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
    assert calls[0][1]["date_from"] == "2026-10-08T18:00:10Z"
    assert set(result) == {"success", "delta", "resolved_by_delta"}


def test_the_delta_alone_runs_a_full_sync_for_a_new_codebase(monkeypatch):
    engine, calls = promotion_engine(monkeypatch, [])
    result = run_delta(engine, codebases=["WNL"])
    assert [name for name, _ in calls] == ["full"]
    assert result["promoted_to_full"] is True


def test_the_run_id_reaches_the_job_tracer(fake_engine):
    fake_engine.tracer = SimpleNamespace(run_id=None)
    run_from_env(codebases=["WUK"], retry=False, run_id="delta-20261012T000001Z")
    assert fake_engine.tracer.run_id == "delta-20261012T000001Z"
    fake_engine.tracer.run_id = None
    retry_from_env(codebases=["WUK"], run_id="delta-20261013T000001Z")
    assert fake_engine.tracer.run_id == "delta-20261013T000001Z"


def test_a_redelivered_message_is_not_blocked_by_its_own_run(fake_engine):
    fake_engine.tracer = SimpleNamespace(run_id=None)
    retry_from_env(codebases=["WDE"], if_busy=IF_BUSY_SKIP, run_id="delta-20261012T000001Z-7")
    run_from_env(codebases=["WDE"], retry=False, if_busy=IF_BUSY_SKIP, run_id="delta-20261012T000001Z-7")
    filters = [kwargs["filter"] for _, kwargs in fake_engine.dataverse.queries]
    busy = [text for text in filters if "ts_lastheartbeat ge" in text]
    assert len(busy) == 2
    assert all("not (contains(ts_name, 'run=delta-20261012T000001Z-7')" in text for text in busy)


# -- taking over from a stopped delivery of the same run ------------------------------

RUN = "full-20261014T131500Z-7"
STOPPED = {
    "ts_integrationjobid": "guid-1",
    "ts_jobid": "job-killed",
    "ts_startedon": "2026-10-14T14:41:00Z",
    "ts_lastheartbeat": "2026-10-14T15:25:00Z",
    "ts_errorsummary": "WDE: 2 failure(s)",
}


class JobTable(Jobs):
    """Jobs stub: own-run Running rows for the takeover, live rows for the busy check."""

    def __init__(self, own=None, live=None):
        super().__init__()
        self.own = own or []
        self.live = live or []
        self.updates = []

    def retrieve_multiple(self, entity_set, **kwargs):
        self.queries.append((entity_set, kwargs))
        if "ts_lastheartbeat ge" in (kwargs.get("filter") or ""):
            return list(self.live)
        return list(self.own)

    def update(self, entity_set, record_id, payload):
        self.updates.append((entity_set, record_id, payload))


def test_a_redelivered_full_sync_takes_over_its_stopped_job(fake_engine):
    fake_engine.tracer = SimpleNamespace(run_id=None)
    fake_engine.dataverse = JobTable(own=[STOPPED])
    run_from_env(codebases=["WDE"], full=True, if_busy=IF_BUSY_RAISE, run_id=RUN)
    assert [kind for kind, _ in fake_engine.ran] == ["full"]
    takeover = fake_engine.dataverse.queries[0][1]["filter"]
    assert takeover == (
        f"ts_codebase eq 'WDE' and ts_status eq {STATUS_RUNNING}"
        f" and contains(ts_name, 'run={RUN}')"
        " and (_ts_retryofjob_value ne null or endswith(ts_name, ' part=1'))"
    )
    [(entity_set, record_id, payload)] = fake_engine.dataverse.updates
    assert (entity_set, record_id) == (JOBS, "guid-1")
    assert payload["ts_status"] == STATUS_ABANDONED
    # It ends when its heartbeat stopped, 44 minutes after it started.
    assert payload["ts_completedon"] == "2026-10-14T15:25:00Z"
    assert payload["ts_durationseconds"] == 44 * 60
    assert payload["ts_errorsummary"].startswith(
        "Abandoned: the delivery running it was stopped (last heartbeat 2026-10-14T15:25:00Z)"
    )
    assert f"run={RUN} took over" in payload["ts_errorsummary"]
    assert payload["ts_errorsummary"].endswith(" | WDE: 2 failure(s)")


def test_a_live_job_of_another_run_still_blocks_a_full_sync(fake_engine):
    fake_engine.tracer = SimpleNamespace(run_id=None)
    fake_engine.dataverse = JobTable(live=[{"ts_jobid": "job-cli", "ts_lastheartbeat": "x"}])
    with pytest.raises(SyncBusy, match="job job-cli"):
        run_from_env(codebases=["WDE"], full=True, if_busy=IF_BUSY_RAISE, run_id=RUN)
    assert fake_engine.dataverse.updates == []
    assert fake_engine.ran == []


def test_the_takeover_goes_on_when_the_stopped_job_cannot_be_marked(fake_engine):
    fake_engine.tracer = SimpleNamespace(run_id=None)
    table = JobTable(own=[STOPPED])

    def refuse(entity_set, record_id, payload):
        raise RuntimeError("HTTP 503")

    table.update = refuse
    fake_engine.dataverse = table
    assert scheduled_sync.abandon_own_jobs(table, ["WDE"], RUN) == []
    run_from_env(codebases=["WDE"], full=True, if_busy=IF_BUSY_RAISE, run_id=RUN)
    assert [kind for kind, _ in fake_engine.ran] == ["full"]


def test_the_takeover_reports_the_jobs_it_abandoned():
    table = JobTable(own=[STOPPED])
    assert scheduled_sync.abandon_own_jobs(table, ["WDE"], RUN) == ["job-killed"]


def test_run_tries_counts_the_parts_jobs_since_the_message_was_queued():
    jobs = Jobs([{"ts_jobid": "a"}, {"ts_jobid": "b"}])
    since = datetime(2026, 10, 14, 15, 26, 40, 500000, tzinfo=timezone.utc)
    assert scheduled_sync.run_tries(jobs, "wde", RUN, 2, since) == 2
    assert jobs.queries[0][1]["filter"] == (
        f"ts_codebase eq 'WDE' and contains(ts_name, 'run={RUN}')"
        " and endswith(ts_name, ' part=2')"
        " and _ts_retryofjob_value eq null and ts_startedon ge 2026-10-14T15:26:40Z"
    )


QUEUED = datetime(2026, 10, 14, 15, 26, 40, tzinfo=timezone.utc)


def test_a_full_sync_reports_which_try_it_is(fake_engine):
    # An earlier delivery of this part was stopped: its job is taken over and
    # counted, so this is try 2. The count comes after the busy check, so a
    # delivery that found the codebase busy is not a try.
    fake_engine.tracer = SimpleNamespace(run_id=None, part=None)
    fake_engine.dataverse = JobTable(own=[STOPPED])
    result = run_from_env(
        codebases=["WDE"], full=True, if_busy=IF_BUSY_RAISE, run_id=RUN, part=2, queued_at=QUEUED
    )
    assert result["tries"] == 2
    assert [record_id for _, record_id, _ in fake_engine.dataverse.updates] == ["guid-1"]
    assert (fake_engine.tracer.run_id, fake_engine.tracer.part) == (RUN, 2)


def test_a_failing_full_sync_reports_its_try_on_the_exception(fake_engine, monkeypatch):
    fake_engine.tracer = SimpleNamespace(run_id=None, part=None)
    fake_engine.dataverse = JobTable()

    def outage(engine, **kwargs):
        raise RuntimeError("OneKey 503")

    monkeypatch.setattr(scheduled_sync, "run_full", outage)
    with pytest.raises(RuntimeError) as caught:
        run_from_env(codebases=["WDE"], full=True, if_busy=IF_BUSY_RAISE, run_id=RUN, queued_at=QUEUED)
    assert caught.value.tries == 1


def test_a_full_sync_outside_a_timer_run_reports_no_try(fake_engine):
    result = run_from_env(codebases=["WUK"], full=True, if_busy=IF_BUSY_RAISE)
    assert "tries" not in result


def test_a_paused_promoted_full_sync_reports_it_at_the_top(monkeypatch):
    # The queue worker reads paused and records_written from the top level to
    # decide whether to queue the next part.
    engine, calls = promotion_engine(monkeypatch, [])
    engine.sync_full = lambda **kwargs: {
        "success": True, "job_id": "full-1", "paused": True, "records_written": 300,
    }
    result = run_delta(engine, codebases=["WNL"])
    assert result["promoted_to_full"] is True
    assert (result["paused"], result["records_written"]) == (True, 300)


def test_a_finished_delta_reports_no_pause(monkeypatch):
    engine, calls = promotion_engine(
        monkeypatch,
        [{"ts_jobid": "d-1", "ts_startedon": "2026-10-09T00:00:20Z",
          "ts_watermarkto": "2026-10-09T00:00:10Z", "ts_errorsummary": None}],
    )
    assert "paused" not in run_delta(engine, codebases=["WNL"])


def test_a_time_budget_becomes_the_engine_deadline(fake_engine, monkeypatch):
    engine = fake_engine
    monkeypatch.setattr(scheduled_sync.time, "monotonic", lambda: 1000.0)
    run_from_env(codebases=["WUK"], full=True, time_budget=2700)
    assert engine.deadline == 3700.0
    run_from_env(codebases=["WUK"], full=True)  # the CLI: no limit
    assert engine.deadline is None
