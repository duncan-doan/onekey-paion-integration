"""Offline tests for scheduled Azure Storage Queue work."""

import json

import pytest

from integration import queue_sync
from integration.queue_sync import (
    FULL_MAX_DELIVERIES,
    OUTCOME_DONE,
    OUTCOME_POISON,
    OUTCOME_SKIPPED,
    QueueMessageError,
    QueueWorkFailed,
    SyncQueueMessage,
    dispatch,
    handle_message,
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
        '{"mode":"full","codebase":"WUK","unexpected":true}',
        '{"mode":"full","codebase":"WUK","date_to":"2026-10-07"}',
        '{"mode":"delta","codebase":"WUK","choices":"no"}',
    ],
)
def test_invalid_message_is_rejected(payload):
    with pytest.raises(QueueMessageError):
        SyncQueueMessage.decode(payload)


# -- dispatch ----------------------------------------------------------------


def fake_runner(monkeypatch, result):
    seen = {}

    def fake_run_from_env(**kwargs):
        seen.update(kwargs)
        return result

    monkeypatch.setattr("integration.scheduled_sync.run_from_env", fake_run_from_env)
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
    assert seen == {
        "codebases": ["WDE"],
        "full": False,
        "date_to": "2026-10-07T08:00:00Z",
        "retry_limit": 10,
        "if_busy": IF_BUSY_SKIP,
        "sync_choices": False,
    }


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
    monkeypatch.setattr(queue_sync, "dispatch", lambda item: pytest.fail("ran"))
    outcome = handle_message(b"not json", 1)
    assert outcome.status == OUTCOME_POISON
    assert outcome.item is None
    assert "Invalid queue message" in outcome.reason


def failing_dispatch(item):
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
    def busy(item):
        raise SyncBusy("WUK", {"ts_jobid": "job-0", "ts_lastheartbeat": "x"})

    monkeypatch.setattr(queue_sync, "dispatch", busy)
    with pytest.raises(SyncBusy):
        handle_message('{"mode":"full","codebase":"WUK"}', FULL_MAX_DELIVERIES + 1)


def test_skipped_and_done_outcomes(monkeypatch):
    monkeypatch.setattr(
        queue_sync, "dispatch", lambda item: {"success": True, "skipped": True}
    )
    assert handle_message('{"mode":"delta","codebase":"WUK"}', 1).status == OUTCOME_SKIPPED
    monkeypatch.setattr(queue_sync, "dispatch", lambda item: {"success": True, "delta": {}})
    outcome = handle_message('{"mode":"delta","codebase":"WUK"}', 1)
    assert outcome.status == OUTCOME_DONE
    assert outcome.result == {"success": True, "delta": {}}
