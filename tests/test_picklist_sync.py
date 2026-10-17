"""Offline tests for the choice option sync's error count."""

import pytest

import integration.picklist_sync as picklist_sync
from dataverse.exceptions import DataverseHTTPError

TARGET = {"kind": "global", "option_set": "ts_example"}
DESIRED = {100000000 + i: f"Label {i}" for i in range(10)}


def _sync(monkeypatch, rejected_values, batch_errors):
    monkeypatch.setattr(picklist_sync, "load_global_options", lambda dv, name: {})
    monkeypatch.setattr(picklist_sync, "run_batch", lambda dv, ops: batch_errors)

    def post(dv, action, body):
        if body["Value"] in rejected_values:
            raise DataverseHTTPError("rejected", status_code=400)

    monkeypatch.setattr(picklist_sync, "post_action", post)
    return picklist_sync.sync_type(object(), "example", TARGET, DESIRED, dry_run=False)


@pytest.mark.parametrize("rejected", [100000000, 100000009])
def test_an_option_rejected_on_its_own_is_counted(monkeypatch, rejected):
    # The changeset failed, so each option was sent again on its own. The
    # count used to subtract the successes, so a failure that came first
    # ended at 0 and the timer told the workers the choices were synced.
    summary = _sync(monkeypatch, {rejected}, ["changeset failed"])
    assert summary["errors"] == 1


def test_a_batch_that_only_failed_as_a_whole_has_no_errors(monkeypatch):
    summary = _sync(monkeypatch, set(), ["changeset failed"])
    assert summary["errors"] == 0
