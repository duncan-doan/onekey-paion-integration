"""Offline tests for scheduled Azure Storage Queue work."""

import json
from datetime import datetime, timedelta, timezone

import pytest

from integration import queue_sync
from integration.queue_sync import (
    FULL_MAX_DELIVERIES,
    OUTCOME_DONE,
    MAX_PARTS,
    OUTCOME_CONTINUED,
    OUTCOME_POISON,
    OUTCOME_SKIPPED,
    QUEUES,
    RETRY_MAX_DELIVERIES,
    QueueMessageError,
    QueueWorkFailed,
    SyncQueueMessage,
    delta_after,
    dispatch,
    handle_message,
    new_run,
    poison_queue,
    run_progress,
    scheduled_messages,
)
from integration.scheduled_sync import IF_BUSY_RAISE, IF_BUSY_SKIP, SyncBusy


def test_scheduled_messages_fan_out_by_distinct_codebase():
    messages = scheduled_messages("delta", ["wuk", "WDE", " WUK "], retry_limit=25)
    decoded = [json.loads(message) for message in messages]
    assert [item["codebase"] for item in decoded] == ["WUK", "WDE"]
    assert all(item["mode"] == "delta" for item in decoded)
    assert all(item["retry_limit"] == 25 for item in decoded)
    assert all(item["choices"] is True for item in decoded)


def test_scheduled_delta_messages_leave_the_window_end_to_the_worker():
    # A date_to fixed at enqueue time ends before the worker starts; the next
    # window would then start after the gap.
    decoded = json.loads(scheduled_messages("delta", ["WUK"])[0])
    assert decoded["date_to"] is None


def test_scheduled_messages_carry_the_choices_flag():
    decoded = json.loads(scheduled_messages("full", ["WUK"], choices=False)[0])
    assert decoded["choices"] is False


def test_message_round_trip():
    original = SyncQueueMessage(mode="FULL", codebase="wuk", choices=False)
    assert SyncQueueMessage.decode(original.encode()) == original


def test_each_mode_has_its_queue_and_azure_poison_name():
    assert QUEUES == {"full": "onekey-full", "delta": "onekey-delta", "retry": "onekey-retry"}
    assert poison_queue("onekey-retry") == "onekey-retry-poison"


def test_a_message_without_a_mode_takes_the_queue_mode():
    assert SyncQueueMessage.decode('{"codebase":"wde"}', mode="full").mode == "full"
    # A mode in the body is kept, so handle_message can reject the wrong queue.
    assert SyncQueueMessage.decode('{"mode":"delta","codebase":"WDE"}', mode="full").mode == "delta"


def test_message_without_choices_defaults_to_syncing_them():
    item = SyncQueueMessage.decode('{"mode":"delta","codebase":"WUK"}')
    assert item.choices is True
    assert item.date_to is None


@pytest.mark.parametrize(
    "payload",
    [
        "not json",
        b"\xff\xfe",
        "[]",
        '{"mode":"other","codebase":"WUK"}',
        '{"mode":"full","codebase":""}',
        '{"mode":"full","codebase":"WUK","version":2}',
        '{"mode":"full","codebase":"WUK","date_to":"2026-10-07"}',
        '{"mode":"retry","codebase":"WUK","date_to":"2026-10-07"}',
        '{"codebase":"WUK"}',
        '{"mode":"delta","codebase":"WUK","run_id":"has space"}',
        '{"mode":"delta","codebase":"WUK","enqueued_at":"yesterday"}',
        '{"mode":"delta","codebase":"WUK","choices":"no"}',
    ],
)
def test_invalid_message_is_rejected(payload):
    with pytest.raises(QueueMessageError):
        SyncQueueMessage.decode(payload)


# -- dispatch ----------------------------------------------------------------


def fake_runner(monkeypatch, result, name="run_from_env"):
    seen = {}

    def fake_run(**kwargs):
        seen.update(kwargs)
        return result

    monkeypatch.setattr(f"integration.scheduled_sync.{name}", fake_run)
    return seen


def test_dispatch_runs_one_codebase_and_skips_a_busy_delta(monkeypatch):
    seen = fake_runner(monkeypatch, {"success": True, "delta": {"job_id": "job-1"}})
    result = dispatch(
        SyncQueueMessage(
            mode="delta",
            codebase="WDE",
            date_to="2026-10-07T08:00:00Z",
            retry_limit=10,
            choices=False,
        )
    )
    assert result["success"] is True
    # The retry already ran from onekey-retry, so the delta runs alone.
    assert seen == {
        "codebases": ["WDE"],
        "full": False,
        "retry": False,
        "date_to": "2026-10-07T08:00:00Z",
        "if_busy": IF_BUSY_SKIP,
        "sync_choices": False,
        "run_id": None,
        # Stops before Flex Consumption's 60-minute scale-in limit.
        "time_budget": 45 * 60,
        "part": 1,
        "queued_at": None,
    }


def test_dispatch_runs_a_retry_alone_and_skips_a_busy_codebase(monkeypatch):
    seen = fake_runner(
        monkeypatch, {"success": True, "retry": {"jobs": []}}, name="retry_from_env"
    )
    monkeypatch.setattr(
        "integration.scheduled_sync.run_from_env", lambda **kwargs: pytest.fail("ran a sync")
    )
    dispatch(SyncQueueMessage(mode="retry", codebase="WDE", retry_limit=10))
    assert seen == {
        "codebases": ["WDE"], "limit": 10, "if_busy": IF_BUSY_SKIP, "run_id": None, "part": 1,
    }


def test_dispatch_passes_the_run_to_the_job_trace(monkeypatch):
    seen = fake_runner(monkeypatch, {"success": True, "full": {}})
    dispatch(SyncQueueMessage(mode="full", codebase="WUK", run_id="full-20261101T000000Z"))
    assert seen["run_id"] == "full-20261101T000000Z"


def test_a_failed_retry_reports_its_jobs(monkeypatch):
    fake_runner(
        monkeypatch,
        {"success": False, "retry": {"jobs": ["r-1", "r-2"], "errors": ["claim lost"]}},
        name="retry_from_env",
    )
    with pytest.raises(QueueWorkFailed, match="retry sync failed for codebase WUK: job=r-1,r-2 errors=claim lost") as caught:
        dispatch(SyncQueueMessage(mode="retry", codebase="WUK"))
    assert caught.value.full_sync is False


def test_dispatch_defers_a_busy_full_sync(monkeypatch):
    seen = fake_runner(monkeypatch, {"success": True, "full": {"job_id": "job-1"}})
    dispatch(SyncQueueMessage(mode="full", codebase="WUK"))
    assert seen["full"] is True
    assert seen["if_busy"] == IF_BUSY_RAISE


def test_dispatch_returns_a_skipped_run_without_raising(monkeypatch):
    fake_runner(monkeypatch, {"success": True, "skipped": True, "busy_job": "job-0"})
    assert dispatch(SyncQueueMessage(mode="delta", codebase="WUK"))["skipped"] is True


def test_dispatch_raises_with_job_and_errors_so_azure_retries(monkeypatch):
    fake_runner(
        monkeypatch,
        {"success": False, "full": {"job_id": "job-3", "errors": ["WUK: OneKey 503"]}},
    )
    with pytest.raises(QueueWorkFailed, match="job=job-3 errors=WUK: OneKey 503"):
        dispatch(SyncQueueMessage(mode="full", codebase="WUK"))


# -- handle_message ----------------------------------------------------------


def test_malformed_message_goes_straight_to_poison(monkeypatch):
    monkeypatch.setattr(queue_sync, "dispatch", lambda item, **_: pytest.fail("ran"))
    outcome = handle_message(b"not json", 1)
    assert outcome.status == OUTCOME_POISON
    assert outcome.item is None
    assert "Invalid queue message" in outcome.reason


def failing_dispatch(item, **_):
    raise QueueWorkFailed(f"{item.mode} sync failed for codebase {item.codebase}")


def test_failed_full_sync_is_retried_once(monkeypatch):
    monkeypatch.setattr(queue_sync, "dispatch", failing_dispatch)
    body = '{"mode":"full","codebase":"WUK"}'
    with pytest.raises(QueueWorkFailed):
        handle_message(body, 1)
    outcome = handle_message(body, FULL_MAX_DELIVERIES)
    assert outcome.status == OUTCOME_POISON
    assert outcome.item.codebase == "WUK"
    assert f"delivery {FULL_MAX_DELIVERIES}" in outcome.reason


def test_failed_delta_keeps_every_azure_retry(monkeypatch):
    monkeypatch.setattr(queue_sync, "dispatch", failing_dispatch)
    with pytest.raises(QueueWorkFailed):
        handle_message('{"mode":"delta","codebase":"WUK"}', 4)


def test_busy_full_sync_is_deferred_not_poisoned(monkeypatch):
    def busy(item, **_):
        raise SyncBusy("WUK", {"ts_jobid": "job-0", "ts_lastheartbeat": "x"})

    monkeypatch.setattr(queue_sync, "dispatch", busy)
    with pytest.raises(SyncBusy):
        handle_message('{"mode":"full","codebase":"WUK"}', FULL_MAX_DELIVERIES + 1)


RUN_FULL = SyncQueueMessage(
    mode="full", codebase="WDE", run_id="full-20261014T131500Z-7"
).encode()


QUEUED = datetime(2026, 10, 14, 15, 26, 40, tzinfo=timezone.utc)


def failing_try(tries):
    def fail(item, **_):
        raise QueueWorkFailed(f"full sync failed for codebase {item.codebase}", full_sync=True, tries=tries)
    return fail


def test_busy_deliveries_do_not_use_up_a_full_syncs_tries(monkeypatch):
    # Delivery 1 found WDE busy; delivery 2 got past the busy check as the
    # message's first try (run_from_env counts it), so it is retried.
    monkeypatch.setattr(queue_sync, "dispatch", failing_try(1))
    with pytest.raises(QueueWorkFailed):
        handle_message(RUN_FULL, FULL_MAX_DELIVERIES, mode="full", inserted_at=QUEUED)
    # The second try fails too, so it is poisoned.
    monkeypatch.setattr(queue_sync, "dispatch", failing_try(2))
    outcome = handle_message(RUN_FULL, FULL_MAX_DELIVERIES + 1, mode="full", inserted_at=QUEUED)
    assert outcome.status == OUTCOME_POISON


def test_a_delivery_that_fails_before_starting_is_not_a_try(monkeypatch):
    # An outage before the busy check: the exception carries no try, so
    # Azure's maxDequeueCount is what bounds the deliveries.
    def outage(item, **_):
        raise RuntimeError("token endpoint unavailable")

    monkeypatch.setattr(queue_sync, "dispatch", outage)
    with pytest.raises(RuntimeError):
        handle_message(RUN_FULL, FULL_MAX_DELIVERIES, mode="full", inserted_at=QUEUED)


def test_a_full_sync_outside_a_timer_run_counts_its_deliveries(monkeypatch):
    monkeypatch.setattr(queue_sync, "dispatch", failing_dispatch)
    manual = '{"mode":"full","codebase":"WDE"}'
    outcome = handle_message(manual, FULL_MAX_DELIVERIES, mode="full", inserted_at=QUEUED)
    assert outcome.status == OUTCOME_POISON
    # Without a queue time there is nothing to count from either.
    assert handle_message(RUN_FULL, FULL_MAX_DELIVERIES, mode="full").status == OUTCOME_POISON


def test_the_part_and_queue_time_reach_the_sync(monkeypatch):
    seen = fake_runner(monkeypatch, {"success": True, "full": {}})
    body = SyncQueueMessage(
        mode="full", codebase="WUK", run_id="full-20261101T000000Z-7", part=3
    ).encode()
    handle_message(body, 1, mode="full", inserted_at=QUEUED)
    assert (seen["part"], seen["queued_at"]) == (3, QUEUED)


def test_a_failed_full_sync_reports_its_try(monkeypatch):
    fake_runner(monkeypatch, {"success": False, "tries": 2, "full": {"errors": ["HTTP 400"]}})
    with pytest.raises(QueueWorkFailed) as caught:
        dispatch(SyncQueueMessage(mode="full", codebase="WUK", run_id="full-20261101T000000Z-7"))
    assert caught.value.tries == 2


def test_skipped_and_done_outcomes(monkeypatch):
    monkeypatch.setattr(
        queue_sync, "dispatch", lambda item, **_: {"success": True, "skipped": True}
    )
    assert handle_message('{"mode":"delta","codebase":"WUK"}', 1).status == OUTCOME_SKIPPED
    monkeypatch.setattr(queue_sync, "dispatch", lambda item, **_: {"success": True, "delta": {}})
    outcome = handle_message('{"mode":"delta","codebase":"WUK"}', 1)
    assert outcome.status == OUTCOME_DONE
    assert outcome.result == {"success": True, "delta": {}}
    assert outcome.next_delta is None


def test_a_message_from_another_queue_is_poisoned(monkeypatch):
    monkeypatch.setattr(queue_sync, "dispatch", lambda item, **_: pytest.fail("ran"))
    outcome = handle_message('{"mode":"full","codebase":"WUK"}', 1, mode="delta")
    assert outcome.status == OUTCOME_POISON
    assert outcome.item.codebase == "WUK"
    assert outcome.reason == "full message on onekey-delta; put it on onekey-full"


def test_the_queue_mode_runs_a_message_without_one(monkeypatch):
    ran = []
    monkeypatch.setattr(
        queue_sync, "dispatch", lambda item, **_: ran.append(item.mode) or {"success": True}
    )
    assert handle_message('{"codebase":"WUK"}', 1, mode="full").status == OUTCOME_DONE
    assert ran == ["full"]


# -- retry then delta --------------------------------------------------------------


def test_a_finished_retry_queues_its_delta(monkeypatch):
    monkeypatch.setattr(queue_sync, "dispatch", lambda item, **_: {"success": True, "retry": {}})
    outcome = handle_message(
        '{"mode":"retry","codebase":"wnl","choices":false}', 1, mode="retry"
    )
    assert outcome.status == OUTCOME_DONE
    assert SyncQueueMessage.decode(outcome.next_delta) == SyncQueueMessage(
        mode="delta", codebase="WNL", choices=False
    )


def test_a_busy_retry_queues_no_delta(monkeypatch):
    monkeypatch.setattr(
        queue_sync, "dispatch", lambda item, **_: {"success": True, "skipped": True}
    )
    outcome = handle_message('{"mode":"retry","codebase":"WNL"}', 1, mode="retry")
    assert outcome.status == OUTCOME_SKIPPED
    assert outcome.next_delta is None


def test_a_failed_retry_is_retried_once_then_poisoned_with_its_delta(monkeypatch):
    monkeypatch.setattr(queue_sync, "dispatch", failing_dispatch)
    body = '{"mode":"retry","codebase":"WNL"}'
    # The first failure raises, so the delta waits for the retry's next delivery.
    with pytest.raises(QueueWorkFailed):
        handle_message(body, 1, mode="retry")
    outcome = handle_message(body, RETRY_MAX_DELIVERIES, mode="retry")
    assert outcome.status == OUTCOME_POISON
    assert outcome.reason.startswith(f"retry failed on delivery {RETRY_MAX_DELIVERIES}")
    assert SyncQueueMessage.decode(outcome.next_delta).mode == "delta"


def test_a_poisoned_full_sync_queues_no_delta(monkeypatch):
    monkeypatch.setattr(queue_sync, "dispatch", failing_dispatch)
    outcome = handle_message('{"mode":"full","codebase":"WUK"}', FULL_MAX_DELIVERIES, mode="full")
    assert outcome.status == OUTCOME_POISON
    assert outcome.next_delta is None


# -- delta messages that ran as a full sync --------------------------------------


def test_a_promoted_full_sync_that_fails_is_reported_as_full(monkeypatch):
    fake_runner(
        monkeypatch,
        {"success": False, "promoted_to_full": True,
         "full": {"job_id": "job-9", "errors": ["WNL: HTTP 400"]}},
    )
    with pytest.raises(QueueWorkFailed, match="full sync failed for codebase WNL: job=job-9") as caught:
        dispatch(SyncQueueMessage(mode="delta", codebase="WNL"))
    assert caught.value.full_sync is True


def test_a_promoted_full_sync_gets_the_full_sync_delivery_limit(monkeypatch):
    def promoted_failure(item, **_):
        raise QueueWorkFailed("full sync failed for codebase WNL", full_sync=True)

    monkeypatch.setattr(queue_sync, "dispatch", promoted_failure)
    body = '{"mode":"delta","codebase":"WNL"}'
    with pytest.raises(QueueWorkFailed):
        handle_message(body, 1)
    assert handle_message(body, FULL_MAX_DELIVERIES).status == OUTCOME_POISON


def test_ran_mode():
    delta = SyncQueueMessage(mode="delta", codebase="WNL")
    assert queue_sync.ran_mode(delta, {"promoted_to_full": True}) == "full"
    assert queue_sync.ran_mode(delta, {"success": True}) == "delta"
    assert queue_sync.ran_mode(delta, None) == "delta"


# -- run timing --------------------------------------------------------------------

QUEUED = datetime(2026, 10, 12, 0, 0, 1, tzinfo=timezone.utc)


def test_a_run_is_named_after_its_job_queue_time_and_country_count():
    assert new_run("delta", 7, QUEUED) == ("delta-20261012T000001Z-7", "2026-10-12T00:00:01Z")


def test_unknown_fields_are_ignored_so_a_rollback_keeps_newer_messages(caplog):
    item = SyncQueueMessage.decode('{"mode":"full","codebase":"WUK","added_later":true}')
    assert item == SyncQueueMessage(mode="full", codebase="WUK")
    assert "Ignoring unknown queue message fields: added_later" in caplog.text


def test_scheduled_messages_share_the_run():
    run_id, enqueued_at = new_run("full", 2, QUEUED)
    decoded = [
        SyncQueueMessage.decode(message)
        for message in scheduled_messages("full", ["WUK", "WDE"], run_id=run_id, enqueued_at=enqueued_at)
    ]
    assert {(item.run_id, item.enqueued_at) for item in decoded} == {(run_id, enqueued_at)}


def test_the_delta_stays_in_its_retrys_run():
    retry = SyncQueueMessage(
        mode="retry", codebase="WNL", run_id="delta-20261012T000001Z", enqueued_at="2026-10-12T00:00:01Z"
    )
    delta = SyncQueueMessage.decode(delta_after(retry))
    assert (delta.mode, delta.run_id, delta.enqueued_at) == (
        "delta", "delta-20261012T000001Z", "2026-10-12T00:00:01Z"
    )


def test_run_progress_counts_the_message_and_the_run_so_far():
    item = SyncQueueMessage(
        mode="delta", codebase="WNL", run_id="delta-20261012T000001Z", enqueued_at="2026-10-12T00:00:01Z"
    )
    started = QUEUED + timedelta(minutes=3)
    progress = run_progress(item, started, started + timedelta(seconds=95))
    assert progress == {"run_id": "delta-20261012T000001Z", "duration_s": 95, "elapsed_s": 275}


def test_a_manual_message_has_no_run_progress():
    now = datetime.now(timezone.utc)
    assert run_progress(SyncQueueMessage(mode="delta", codebase="WNL"), now, now) is None


# -- time budget: a paused part queues the next one ----------------------------


def _paused(written, mode="full"):
    return {"success": True, "paused": True, "records_written": written, mode: {"job_id": "job-1"}}


def test_a_paused_full_sync_queues_its_next_part(monkeypatch):
    fake_runner(monkeypatch, _paused(800))
    body = SyncQueueMessage(
        mode="full", codebase="WDE", run_id="full-20261101T000000Z-7",
        enqueued_at="2026-11-01T00:00:00Z",
    ).encode()
    outcome = handle_message(body, 1, mode="full")
    assert outcome.status == OUTCOME_CONTINUED
    following = SyncQueueMessage.decode(outcome.next_part)
    assert (following.mode, following.codebase, following.part) == ("full", "WDE", 2)
    # Still the same run, so its total time covers every part.
    assert following.run_id == "full-20261101T000000Z-7"
    assert following.enqueued_at == "2026-11-01T00:00:00Z"


def test_a_part_that_wrote_nothing_is_not_continued(monkeypatch):
    # The OneKey walk alone outlasted the budget: another part would repeat it.
    fake_runner(monkeypatch, _paused(0))
    outcome = handle_message(SyncQueueMessage(mode="full", codebase="WDE").encode(), 1, mode="full")
    assert outcome.status == OUTCOME_POISON
    assert outcome.next_part is None
    assert "without writing a record" in outcome.reason


def test_continuations_stop_after_max_parts(monkeypatch):
    fake_runner(monkeypatch, _paused(800, mode="delta"))
    body = SyncQueueMessage(mode="delta", codebase="WDE", part=MAX_PARTS).encode()
    outcome = handle_message(body, 1, mode="delta")
    assert outcome.status == OUTCOME_POISON
    assert f"after {MAX_PARTS} parts" in outcome.reason


def test_a_first_part_is_encoded_as_before():
    # Bodies without "part" stay readable by the version before this one.
    assert "part" not in json.loads(SyncQueueMessage(mode="full", codebase="WDE").encode())
    second = SyncQueueMessage(mode="full", codebase="WDE", part=2)
    assert SyncQueueMessage.decode(second.encode()) == second


@pytest.mark.parametrize("part", [0, -1, True, "2"])
def test_an_invalid_part_is_rejected(part):
    with pytest.raises(QueueMessageError):
        SyncQueueMessage(mode="full", codebase="WDE", part=part)


@pytest.mark.parametrize(
    "setting, seconds",
    [(None, 45 * 60), ("30", 30 * 60), ("0", None), ("off", None), ("junk", 45 * 60)],
)
def test_time_budget_setting(monkeypatch, setting, seconds):
    from integration.env import time_budget_seconds

    if setting is None:
        monkeypatch.delenv("SYNC_TIME_BUDGET_MINUTES", raising=False)
    else:
        monkeypatch.setenv("SYNC_TIME_BUDGET_MINUTES", setting)
    assert time_budget_seconds() == seconds


def test_paused_work_is_not_continued_without_skip_unchanged(monkeypatch):
    # Each part would rewrite what the first part wrote and never get further.
    monkeypatch.setenv("SYNC_SKIP_UNCHANGED", "false")
    fake_runner(monkeypatch, _paused(800))
    outcome = handle_message(SyncQueueMessage(mode="full", codebase="WDE").encode(), 1, mode="full")
    assert outcome.status == OUTCOME_POISON
    assert "SYNC_SKIP_UNCHANGED is off" in outcome.reason
