"""Offline tests for scheduled Azure Storage Queue work."""

import json

import pytest

from integration.queue_sync import (
    QueueMessageError,
    QueueWorkFailed,
    SyncQueueMessage,
    dispatch,
    scheduled_messages,
)


def test_scheduled_messages_fan_out_by_codebase():
    messages = scheduled_messages(
        "delta",
        ["wuk", "WDE"],
        date_to="2026-10-07T08:00:00Z",
        retry_limit=25,
    )
    decoded = [json.loads(message) for message in messages]
    assert [item["codebase"] for item in decoded] == ["WUK", "WDE"]
    assert all(item["mode"] == "delta" for item in decoded)
    assert all(item["date_to"] == "2026-10-07T08:00:00Z" for item in decoded)
    assert all(item["retry_limit"] == 25 for item in decoded)


def test_message_round_trip():
    original = SyncQueueMessage(mode="FULL", codebase="wuk")
    assert SyncQueueMessage.decode(original.encode()) == original


@pytest.mark.parametrize(
    "payload",
    [
        "not json",
        "[]",
        '{"mode":"other","codebase":"WUK"}',
        '{"mode":"full","codebase":""}',
        '{"mode":"full","codebase":"WUK","version":2}',
        '{"mode":"full","codebase":"WUK","unexpected":true}',
    ],
)
def test_invalid_message_is_rejected(payload):
    with pytest.raises(QueueMessageError):
        SyncQueueMessage.decode(payload)


def test_dispatch_passes_one_codebase_to_scheduled_runner(monkeypatch):
    seen = {}

    def fake_run_from_env(**kwargs):
        seen.update(kwargs)
        return {"success": True, "delta": {"job_id": "job-1"}}

    monkeypatch.setattr("integration.scheduled_sync.run_from_env", fake_run_from_env)
    result = dispatch(
        SyncQueueMessage(
            mode="delta",
            codebase="WDE",
            date_to="2026-10-07T08:00:00Z",
            retry_limit=10,
        )
    )
    assert result["success"] is True
    assert seen == {
        "codebases": ["WDE"],
        "full": False,
        "date_to": "2026-10-07T08:00:00Z",
        "retry_limit": 10,
    }


def test_dispatch_raises_so_azure_retries_failed_work(monkeypatch):
    monkeypatch.setattr(
        "integration.scheduled_sync.run_from_env",
        lambda **_kwargs: {"success": False},
    )
    with pytest.raises(QueueWorkFailed):
        dispatch(SyncQueueMessage(mode="full", codebase="WUK"))
