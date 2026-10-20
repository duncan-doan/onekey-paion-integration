"""Offline tests for the choice option sync's error count."""

import pytest

import integration.picklist_sync as picklist_sync
from dataverse.exceptions import DataverseHTTPError
from integration.dynamic_mapping import load_mapping_config

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


def test_substatus_targets_the_shared_global_choice():
    config = load_mapping_config()
    target = config.get_picklist_target("substatus")

    assert target is not None
    assert target.kind == "global"
    assert target.option_set == "ts_substatus"
    assert picklist_sync.csv_options(config, "substatus") == {
        126870001: "Active",
        126870002: "Closed",
        126870003: "Deceased",
        126870004: "Deregistered",
        126870005: "Inactive",
        126870006: "Maternity/Paternity Leave",
        126870007: "Open",
        126870008: "Is opening",
        126870009: "Reduced Activity",
        126870010: "Retired",
        126870011: "Temporarily Closed",
        126870012: "Temporarily Inactive",
    }


def test_addresstype_targets_the_customeraddress_local_choice():
    config = load_mapping_config()
    target = config.get_picklist_target("addresstype")

    assert target is not None
    assert target.kind == "local"
    assert target.entity == "customeraddress"
    assert target.attribute == "addresstypecode"
    assert picklist_sync.csv_options(config, "addresstype") == {
        1: "Bill To",
        2: "Ship To",
        3: "Primary",
        4: "Other",
        5: "Mail To",
    }


def test_workingstatus_targets_the_affiliation_global_choice():
    config = load_mapping_config()
    target = config.get_picklist_target("workingstatus")

    assert target is not None
    assert target.kind == "global"
    assert target.option_set == "ts_affiliationworkingstatus"
    assert picklist_sync.csv_options(config, "workingstatus") == {
        126870001: "Full Time",
        126870002: "Home",
        126870003: "Part Time",
        126870004: "Regular Substitute",
        126870005: "Market Access",
        126870006: "Unknown Activity",
    }
