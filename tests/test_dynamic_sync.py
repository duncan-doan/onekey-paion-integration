"""Offline tests for the sync engine's phases on a fake OneKey and Dataverse."""

import pytest

from dataverse.exceptions import DataverseHTTPError
from integration.dynamic_sync import DynamicSyncEngine, main_activity_by_individual
from integration.job_trace import (
    ENTITY_ACCOUNT,
    ENTITY_ADDRESS,
    ENTITY_AFFILIATION,
    ENTITY_CONTACT,
    FAILURES,
    PHASE_ADDRESSES,
    PHASE_HCO,
    PHASE_HCP,
    PHASE_HCP_ACTIVITY,
    PHASE_HIERARCHY,
)
from integration.sync_logger import SyncLogger
from onekey.parse import flatten_hco


class PopulationOneKey:
    """Replays the same population records on every walk, and counts the walks."""

    def __init__(self, records):
        self.records = records
        self.walks = 0

    def iter_population(self, **kwargs):
        self.walks += 1
        return iter(self.records)

    def close(self):
        pass


class BulkDataverse:
    """Upserts every record, except those whose key is in ``reject`` (a 400)."""

    def __init__(self, reject=(), lookup_error=None):
        self.reject = set(reject)
        self.lookup_error = lookup_error
        self.created = []
        self.updated = []
        self.upserts = []

    def create(self, entity_set, data, return_record=False):
        self.created.append((entity_set, data))
        return f"guid-{len(self.created)}"

    def update(self, entity_set, record_id, data):
        self.updated.append((entity_set, record_id, data))
        return record_id

    def create_multiple(self, entity_set, records, logical_name=None):
        self.created.extend((entity_set, record) for record in records)
        return [f"f-{i}" for i in range(len(records))]

    def retrieve_multiple(self, *args, **kwargs):
        return []

    def find_one(self, *args, **kwargs):
        return None

    def find_many_by_field(self, *args, **kwargs):
        if self.lookup_error:
            raise self.lookup_error
        return {}

    def bulk_upsert_by_field(self, entity_set, match_field, records, batch_size=None,
                             record_errors=None, skip_unchanged=False, skipped=None):
        self.upserts.append((entity_set, [r.get(match_field) for r in records]))
        results = []
        for record in records:
            key = record.get(match_field)
            if key in self.reject:
                record_errors[key] = DataverseHTTPError("bad value", status_code=400)
                results.append((key, None, False))
            else:
                results.append((key, f"{entity_set}-{key}", True))
        return results


POPULATION = [
    {"entityType": "WORKPLACE", "workplace": {"workplaceEid": "WUKH1", "officialName": "One"}},
    {"entityType": "WORKPLACE", "workplace": {"workplaceEid": "WUKH2", "officialName": "Two"}},
    {"entityType": "INDIVIDUAL", "individual": {"individualEid": "WUKI1", "lastName": "Doe"}},
    {
        "entityType": "ACTIVITY",
        "activity": {
            "activityEid": "WUKA1",
            "individualEid": "WUKI1",
            "workplaceEid": "WUKH1",
            "isMainActivity": True,
        },
    },
]


def _engine(tmp_path, dataverse, records=POPULATION):
    onekey = PopulationOneKey(records)
    engine = DynamicSyncEngine(onekey, dataverse, sync_logger=SyncLogger(log_dir=tmp_path))
    return engine, onekey


def test_full_sync_walks_the_population_once(tmp_path):
    engine, onekey = _engine(tmp_path, BulkDataverse())
    result = engine.sync_full(["WUK"], sync_picklists=False)
    assert onekey.walks == 1
    assert result["accounts"]["processed"] == 2
    assert result["contacts"]["processed"] == 1
    assert result["activity_affiliations"]["processed"] == 1
    assert engine.succeeded_records == {
        (PHASE_HCO, ENTITY_ACCOUNT, "WUKH1", "WUKH1"),
        (PHASE_HCO, ENTITY_ACCOUNT, "WUKH2", "WUKH2"),
        (PHASE_HCP, ENTITY_CONTACT, "WUKI1", "WUKI1"),
        (PHASE_HCP_ACTIVITY, ENTITY_AFFILIATION, "WUKA1", "WUKA1"),
    }


@pytest.mark.parametrize("mode", ["full", "affiliations_only"])
def test_dry_run_counts_affiliations_without_existing_guids(tmp_path, mode):
    records = [
        {
            "entityType": "WORKPLACE",
            "workplace": {
                "workplaceEid": "WUKH1",
                "officialName": "One",
                "relations": [{
                    "typeCode": "workplaceToWorkplaceLink",
                    "onekeyEid": "WUKH2",
                    "linkCode": "HIE",
                    "isParent": True,
                }],
            },
        },
        *POPULATION[1:],
    ]
    dataverse = BulkDataverse()
    engine, onekey = _engine(tmp_path, dataverse, records)
    if mode == "full":
        result = engine.sync_full(["WUK"], dry_run=True, sync_picklists=False)
    else:
        result = engine.sync_affiliations_only(["WUK"], dry_run=True)

    assert result["success"] is True
    assert result["affiliations"]["processed"] == 1
    assert result["affiliations"]["failed"] == 0
    assert result["activity_affiliations"]["processed"] == 1
    assert result["activity_affiliations"]["failed"] == 0
    assert onekey.walks == 1
    assert dataverse.created == dataverse.updated == dataverse.upserts == []
    assert engine.succeeded_records == set()


def test_address_success_keeps_its_phase_entity_and_composite_key(tmp_path):
    engine, _ = _engine(tmp_path, BulkDataverse())
    engine._account_cache["WUKH1"] = "account-guid"
    stats = engine._sync_addresses([
        {"workplaceEid": "WUKH1", "address_addressEid": "WUKADDR1"}
    ])

    assert stats["processed"] == 1
    assert (PHASE_ADDRESSES, ENTITY_ADDRESS, "WUKH1", "WUKH1 - WUKADDR1") in engine.succeeded_records
    assert all(entity == ENTITY_ADDRESS for _, entity, _, _ in engine.succeeded_records)


@pytest.mark.parametrize("already_linked", [False, True])
def test_hierarchy_records_success_for_updated_or_already_linked_children(tmp_path, already_linked):
    class HierarchyDataverse(BulkDataverse):
        def find_many_by_field(self, *args, **kwargs):
            return {
                "WUKH1": {
                    "accountid": "child-guid",
                    "_parentaccountid_value": "parent-guid" if already_linked else None,
                },
                "WUKH2": {"accountid": "parent-guid"},
            }

        def update_multiple(self, entity_set, records):
            self.updated.extend((entity_set, row["accountid"], row) for row in records)

    dataverse = HierarchyDataverse()
    engine, _ = _engine(tmp_path, dataverse)
    result = engine._sync_hierarchy([
        {"workplaceEid": "WUKH1", "parentWorkplaceEid": "WUKH2"},
    ], allowed_workplace_eids={"WUKH1", "WUKH2"})

    assert result["already_linked" if already_linked else "linked"] == 1
    assert engine.succeeded_records == {(PHASE_HIERARCHY, ENTITY_ACCOUNT, "WUKH1", "")}
    assert bool(dataverse.updated) is (not already_linked)


def test_a_rejected_record_fails_alone_and_the_codebase_carries_on(tmp_path):
    dataverse = BulkDataverse(reject={"WUKH2"})
    engine, _ = _engine(tmp_path, dataverse)
    result = engine.sync_full(["WUK"], sync_picklists=False)

    assert result["success"] is True
    # No codebase error, so the job still covers its window.
    assert result["errors"] == []
    assert result["accounts"] == {"processed": 1, "created": 1, "updated": 0, "skipped": 0, "failed": 1}
    assert result["contacts"]["processed"] == 1
    assert result["trace_status"] == "Partially Succeeded"
    failures = [data for entity_set, data in dataverse.created if entity_set == FAILURES]
    assert [row["ts_sourceid"] for row in failures] == ["WUKH2"]
    assert failures[0]["ts_httpstatus"] == 400
    assert failures[0]["ts_isretryable"] is False


def test_activity_affiliation_success_notes_only_the_activity(tmp_path):
    engine, _ = _engine(tmp_path, BulkDataverse())
    engine._contact_cache["WUKI1"] = "contact-guid"
    engine._account_cache["WUKH1"] = "account-guid"
    engine._sync_activity_affiliations([POPULATION[3]])
    # WUKI1 / WUKH1 would resolve the contact and account failure rows,
    # though neither record was written.
    assert engine.succeeded_ids == {"WUKA1"}


def test_a_failed_account_lookup_stops_the_phase(tmp_path):
    error = DataverseHTTPError("Service unavailable", status_code=503)
    engine, _ = _engine(tmp_path, BulkDataverse(lookup_error=error))
    flats = [{"workplaceEid": "WUKH1", "address_addressEid": "WUKA9"}]
    with pytest.raises(DataverseHTTPError):
        engine._sync_addresses(flats)


class UnchangedDataverse(BulkDataverse):
    """Reports the keys in ``same`` as already up to date."""

    def __init__(self, same=(), **kwargs):
        super().__init__(**kwargs)
        self.same = set(same)
        self.skip_flags = []

    def bulk_upsert_by_field(self, entity_set, match_field, records, batch_size=None,
                             record_errors=None, skip_unchanged=False, skipped=None):
        self.skip_flags.append(skip_unchanged)
        results = []
        for record in records:
            key = record.get(match_field)
            if skip_unchanged and key in self.same:
                skipped.add(key)
                results.append((key, f"{entity_set}-{key}", False))
            else:
                results.append((key, f"{entity_set}-{key}", True))
        return results


def test_unchanged_records_are_counted_as_skipped(tmp_path, monkeypatch):
    monkeypatch.delenv("SYNC_SKIP_UNCHANGED", raising=False)
    dataverse = UnchangedDataverse(same={"WUKH1"})
    engine, _ = _engine(tmp_path, dataverse)
    result = engine.sync_full(["WUK"], sync_picklists=False)

    assert result["accounts"] == {"processed": 2, "created": 1, "updated": 0, "skipped": 1, "failed": 0}
    assert set(dataverse.skip_flags) == {True}
    # A skipped record is in the right state, so it still resolves pending failures.
    assert "WUKH1" in engine.succeeded_ids
    job_updates = [data for _, _, data in dataverse.updated if "ts_skipped" in data]
    assert job_updates and job_updates[-1]["ts_skipped"] >= 1


def test_skip_unchanged_can_be_switched_off(tmp_path, monkeypatch):
    monkeypatch.setenv("SYNC_SKIP_UNCHANGED", "false")
    dataverse = UnchangedDataverse(same={"WUKH1"})
    engine, _ = _engine(tmp_path, dataverse)
    result = engine.sync_full(["WUK"], sync_picklists=False)

    assert set(dataverse.skip_flags) == {False}
    assert result["accounts"]["skipped"] == 0


def test_the_cli_dry_run_mock_passes_the_schema_check(tmp_path):
    # scripts/run_dynamic_sync.py --dry-run uses MockDataverse. Without
    # retrieve_multiple every entity looked missing and the run stopped at
    # validation, before any phase.
    import importlib.util
    from pathlib import Path

    script = Path(__file__).resolve().parent.parent / "scripts" / "run_dynamic_sync.py"
    spec = importlib.util.spec_from_file_location("run_dynamic_sync", script)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    engine, onekey = _engine(tmp_path, module.MockDataverse())
    result = engine.sync_full(["WUK"], dry_run=True, sync_picklists=False)
    assert result["success"] is True
    assert "validation" not in result
    assert result["accounts"]["processed"] == 2


def test_a_delta_counts_contact_changes_it_cannot_apply(tmp_path):
    # An activity whose individual did not change (WUKI2) and a new individual
    # with no activity (WUKI9) are not written; both are counted on the job
    # instead of disappearing into an INFO line.
    records = [
        POPULATION[0],  # WUKH1
        {"entityType": "INDIVIDUAL", "individual": {"individualEid": "WUKI9", "lastName": "Alone"}},
        {
            "entityType": "ACTIVITY",
            "activity": {"activityEid": "WUKA2", "individualEid": "WUKI2",
                         "workplaceEid": "WUKH1", "isMainActivity": True},
        },
    ]
    dataverse = BulkDataverse()
    engine, _ = _engine(tmp_path, dataverse, records)
    result = engine.sync_delta(
        ["WUK"], "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z", sync_picklists=False
    )
    assert result["unjoined"] == {"activities": 1, "individuals": 1}
    assert result["contacts"]["processed"] == 0
    final = dataverse.updated[-1][2]
    assert '"unjoined":{"activities":1,"individuals":1}' in final["ts_phasecounts"]


# -- delta contacts from what changed ----------------------------------------------

DELTA_WINDOW = ("2026-10-15T00:46:00Z", "2026-10-16T03:24:00Z")
PARENT = "parentcustomerid_account@odata.bind"


class KnownDataverse(BulkDataverse):
    """Already holds some contacts and accounts; keeps each upserted record."""

    def __init__(self, contacts=(), accounts=(), **kwargs):
        super().__init__(**kwargs)
        self.rows = {
            "contacts": {eid: {"contactid": f"contact-{eid}"} for eid in contacts},
            "accounts": {eid: {"accountid": f"account-{eid}"} for eid in accounts},
        }
        self.records = {}
        self.lookups = []

    def find_many_by_field(self, entity_set, match_field, values, select=None, **kwargs):
        if self.lookup_error:
            raise self.lookup_error
        self.lookups.append((entity_set, list(values)))
        table = self.rows.get(entity_set, {})
        return {value: table[value] for value in values if value in table}

    def bulk_upsert_by_field(self, entity_set, match_field, records, **kwargs):
        for record in records:
            self.records[(entity_set, record.get(match_field))] = record
        return super().bulk_upsert_by_field(entity_set, match_field, records, **kwargs)

    def contacts(self):
        return {key: record for (entity_set, key), record in self.records.items()
                if entity_set == "contacts"}


def _individual(eid, last_name):
    return {"entityType": "INDIVIDUAL", "codBase": eid[:3],
            "individual": {"individualEid": eid, "lastName": last_name}}


def _activity(activity_eid, individual_eid, workplace_eid, main=True):
    # As on UAT: the ACTIVITY's own individual and workplace are empty.
    return {"entityType": "ACTIVITY", "codBase": individual_eid[:3],
            "individual": {}, "workplace": {},
            "activity": {"activityEid": activity_eid, "individualEid": individual_eid,
                         "workplaceEid": workplace_eid, "isMainActivity": main}}


def _delta(tmp_path, dataverse, records, codebase="WNL"):
    engine, _ = _engine(tmp_path, dataverse, records)
    return engine.sync_delta(
        [codebase], *DELTA_WINDOW, sync_picklists=False, sync_activity_affiliations=False
    )


def test_a_delta_updates_a_contact_whose_workplace_did_not_change(tmp_path):
    # The UAT test of 16 Oct: a last name changed, so OneKey sent the
    # individual and its main activity but not the activity's workplace.
    dataverse = KnownDataverse(contacts={"WNLN00001683"}, accounts={"WNLH05419505"})
    result = _delta(tmp_path, dataverse, [
        _individual("WNLN00001683", "Test"),
        _activity("WNLN0000168301", "WNLN00001683", "WNLH05419505"),
    ])
    assert result["contacts"]["processed"] == 1
    assert result["unjoined"] == {"activities": 0, "individuals": 0}
    contact = dataverse.contacts()["WNLN00001683"]
    assert contact["lastname"] == "Test"
    assert contact[PARENT] == "/accounts(account-WNLH05419505)"
    # The workplace was not in the window, so the department is left as it is.
    assert "department" not in contact


def test_a_delta_moves_the_parent_only_through_the_main_activity(tmp_path):
    # WDKM00001098 (status Inactive) came with a secondary activity only, and
    # WDKM00000526 with no activity: both are updated, their parents kept.
    dataverse = KnownDataverse(
        contacts={"WDKM00001098", "WDKM00000526"}, accounts={"WDKH00017249"}
    )
    result = _delta(tmp_path, dataverse, [
        _individual("WDKM00001098", "Inactive"),
        _activity("WDKM0000109801", "WDKM00001098", "WDKH00017249", main=False),
        _individual("WDKM00000526", "Alone"),
    ], codebase="WDK")
    assert result["contacts"]["processed"] == 2
    contacts = dataverse.contacts()
    assert set(contacts) == {"WDKM00001098", "WDKM00000526"}
    assert all(PARENT not in contact for contact in contacts.values())


def test_a_delta_creates_a_new_contact_only_at_a_known_workplace(tmp_path):
    # As the full sync: a new individual needs an activity at a workplace in
    # the window or already an account.
    dataverse = KnownDataverse(accounts={"WNLH1"})
    result = _delta(tmp_path, dataverse, [
        _individual("WNLN1", "Known"), _activity("WNLN101", "WNLN1", "WNLH1"),
        _individual("WNLN2", "Outside"), _activity("WNLN201", "WNLN2", "WNLH9"),
        _individual("WNLN3", "NoActivity"),
    ])
    contacts = dataverse.contacts()
    assert set(contacts) == {"WNLN1"}
    assert contacts["WNLN1"][PARENT] == "/accounts(account-WNLH1)"
    assert result["unjoined"] == {"activities": 0, "individuals": 2}


def test_a_delta_with_the_workplace_still_sets_the_department(tmp_path):
    dataverse = KnownDataverse(contacts={"WNLN1"})
    _delta(tmp_path, dataverse, [
        {"entityType": "WORKPLACE", "codBase": "WNL",
         "workplace": {"workplaceEid": "WNLH1", "officialName": "Ziekenhuis",
                       "usualName": "Cardiologie"}},
        _individual("WNLN1", "Doe"), _activity("WNLN101", "WNLN1", "WNLH1"),
    ])
    contact = dataverse.contacts()["WNLN1"]
    assert contact["department"] == "Cardiologie"
    assert PARENT in contact


def test_a_failed_contact_lookup_fails_the_delta(tmp_path):
    # Treating every individual as new would leave existing contacts out, so
    # the codebase fails and the next delta reads its window again.
    error = DataverseHTTPError("Service unavailable", status_code=503)
    dataverse = KnownDataverse(lookup_error=error)
    result = _delta(tmp_path, dataverse, [_individual("WNLN1", "Doe")])
    assert result["success"] is False
    assert dataverse.contacts() == {}


# -- time budget -----------------------------------------------------------


def _workplaces(count):
    return [
        {"entityType": "WORKPLACE", "workplace": {"workplaceEid": f"WUKH{n:04d}", "officialName": f"H{n}"}}
        for n in range(count)
    ]


def test_a_run_past_its_time_budget_stops_between_batches(tmp_path, monkeypatch):
    # Flex Consumption ends an execution 60 minutes into a scale-in; a queued
    # run stops cleanly first, keeps what it wrote, and does not cover its
    # window, so the continuation (or the next delta) reads it again.
    from integration.scheduled_sync import covered_window

    clock = [0.0]
    monkeypatch.setattr("integration.dynamic_sync.time.monotonic", lambda: clock[0])
    dataverse = BulkDataverse()
    original = dataverse.bulk_upsert_by_field

    def slow_upsert(*args, **kwargs):
        clock[0] += 100  # each batch takes 100 s
        return original(*args, **kwargs)

    dataverse.bulk_upsert_by_field = slow_upsert
    engine, _ = _engine(tmp_path, dataverse, _workplaces(250))
    engine.bulk_batch_size = 100
    engine.deadline = 150  # spent during the second batch

    result = engine.sync_full(["WUK"], sync_picklists=False)

    assert result["paused"] is True
    assert result["accounts"]["processed"] == 200  # two batches, the third never sent
    assert result["records_written"] == 200
    assert len([u for u in dataverse.upserts if u[0] == "accounts"]) == 2
    assert result["contacts"]["processed"] == 0  # later phases did not start
    final = dataverse.updated[-1][2]
    assert "WUK: paused" in final["ts_errorsummary"]
    assert not covered_window(final)


def test_without_a_deadline_the_run_finishes(tmp_path):
    engine, _ = _engine(tmp_path, BulkDataverse(), _workplaces(250))
    engine.bulk_batch_size = 100
    result = engine.sync_full(["WUK"], sync_picklists=False)
    assert "paused" not in result
    assert result["accounts"]["processed"] == 250


def test_a_paused_delta_keeps_the_counts_of_the_phases_it_finished(tmp_path, monkeypatch):
    clock = [0.0]
    monkeypatch.setattr("integration.dynamic_sync.time.monotonic", lambda: clock[0])
    dataverse = BulkDataverse()
    original = dataverse.bulk_upsert_by_field

    def slow_upsert(*args, **kwargs):
        clock[0] += 100
        return original(*args, **kwargs)

    dataverse.bulk_upsert_by_field = slow_upsert
    engine, _ = _engine(tmp_path, dataverse)
    engine.deadline = 50  # spent by the accounts batch; contacts never start
    result = engine.sync_delta(
        ["WUK"], "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z", sync_picklists=False
    )
    assert result["paused"] is True
    assert result["accounts"]["processed"] == 2
    assert result["contacts"]["processed"] == 0
    assert result["records_written"] == 2


class NamedAccountsDataverse(BulkDataverse):
    """Holds account WUKH9 already; records every account payload it is sent."""

    def __init__(self):
        super().__init__()
        self.account_payloads = {}

    def find_many_by_field(self, entity_set, match_field, keys, **kwargs):
        if entity_set == "accounts":
            return {key: {"accountid": "guid-9"} for key in keys if key == "WUKH9"}
        return {}

    def bulk_upsert_by_field(self, entity_set, match_field, records, **kwargs):
        if entity_set == "accounts":
            self.account_payloads.update({r.get(match_field): dict(r) for r in records})
        return super().bulk_upsert_by_field(entity_set, match_field, records, **kwargs)


NAMELESS = [
    {"entityType": "WORKPLACE", "workplace": {"workplaceEid": "WUKH9", "typeLabel": "Home address"}},
    {"entityType": "WORKPLACE", "workplace": {"workplaceEid": "WUKH8", "typeLabel": "Home address"}},
]


def test_a_delta_keeps_the_name_of_an_existing_nameless_workplace(tmp_path):
    # The delta window holds none of their activities, so it cannot build
    # the HCP part of the name the full sync gave them.
    dataverse = NamedAccountsDataverse()
    engine, _ = _engine(tmp_path, dataverse, records=NAMELESS)
    engine.sync_delta(
        ["WUK"], "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z", sync_picklists=False
    )
    key = engine.config.require_key_field("account")
    payloads = {payload[key]: payload for payload in dataverse.account_payloads.values()}
    assert "name" not in payloads["WUKH9"]  # exists: keeps its name
    assert payloads["WUKH8"]["name"].startswith("Home address")  # new: gets one


def test_a_full_sync_names_every_nameless_workplace(tmp_path):
    dataverse = NamedAccountsDataverse()
    engine, _ = _engine(tmp_path, dataverse, records=NAMELESS)
    engine.sync_full(["WUK"], sync_picklists=False)
    key = engine.config.require_key_field("account")
    names = {payload[key]: payload.get("name") for payload in dataverse.account_payloads.values()}
    assert names["WUKH9"].startswith("Home address")
    assert names["WUKH8"].startswith("Home address")


def test_a_new_contact_takes_its_parent_only_from_the_main_activity(tmp_path):
    # Its main activity is outside the window. The secondary one, at an
    # account, shows the individual is in the contract but is not its parent.
    dataverse = KnownDataverse(accounts={"WNLH2"})
    result = _delta(tmp_path, dataverse, [
        _individual("WNLN7", "New"), _activity("WNLN701", "WNLN7", "WNLH2", main=False),
    ])
    assert PARENT not in dataverse.contacts()["WNLN7"]
    assert result["unjoined"]["individuals"] == 0


def test_a_partial_contact_leaves_its_pending_failures_open(tmp_path):
    # WNLN1 comes whole. WNLN2 leaves out its department and WNLN3 its
    # parent: a pending failure of theirs may be about that part.
    dataverse = KnownDataverse(contacts={"WNLN1", "WNLN2", "WNLN3"}, accounts={"WNLH5"})
    engine, _ = _engine(tmp_path, dataverse, [
        {"entityType": "WORKPLACE", "codBase": "WNL",
         "workplace": {"workplaceEid": "WNLH1", "officialName": "Ziekenhuis"}},
        _individual("WNLN1", "Whole"), _activity("WNLN101", "WNLN1", "WNLH1"),
        _individual("WNLN2", "NoDepartment"), _activity("WNLN201", "WNLN2", "WNLH5"),
        _individual("WNLN3", "NoParent"),
    ])
    engine.sync_delta(
        ["WNL"], *DELTA_WINDOW, sync_picklists=False, sync_activity_affiliations=False
    )
    assert set(dataverse.contacts()) == {"WNLN1", "WNLN2", "WNLN3"}
    assert "WNLN1" in engine.succeeded_ids
    assert not {"WNLN2", "WNLN3"} & engine.succeeded_ids


def test_a_delta_looks_up_only_the_accounts_it_can_use(tmp_path):
    # An existing contact's secondary activity links nothing, so its
    # workplace is not looked up.
    dataverse = KnownDataverse(contacts={"WDKM1", "WDKM2"}, accounts={"WDKH1", "WDKH2"})
    _delta(tmp_path, dataverse, [
        _individual("WDKM1", "Main"), _activity("WDKM101", "WDKM1", "WDKH1"),
        _individual("WDKM2", "Secondary"), _activity("WDKM201", "WDKM2", "WDKH2", main=False),
    ], codebase="WDK")
    looked_up = {
        value for entity_set, values in dataverse.lookups if entity_set == "accounts"
        for value in values
    }
    assert looked_up == {"WDKH1"}


def test_the_main_activity_wins_whatever_the_order():
    picked = main_activity_by_individual([
        _activity("A1", "WNLN1", "WNLH1", main=False),
        _activity("A2", "WNLN1", "WNLH2"),
        _activity("A3", "WNLN1", "WNLH3"),
        _activity("B1", "WNLN2", "WNLH4", main=False),
    ])
    assert picked["WNLN1"]["activity"]["activityEid"] == "A2"
    assert picked["WNLN2"]["activity"]["activityEid"] == "B1"


def test_a_retry_keeps_the_name_of_an_existing_nameless_workplace(tmp_path):
    # The retry replays accounts without a population walk, so it has no HCP
    # names: the existing account keeps the name the full sync gave it.
    dataverse = NamedAccountsDataverse()
    engine, _ = _engine(tmp_path, dataverse, records=NAMELESS)
    engine._sync_hcos("WUK", flattened=[flatten_hco(record) for record in NAMELESS])
    key = engine.config.require_key_field("account")
    payloads = {payload[key]: payload for payload in dataverse.account_payloads.values()}
    assert "name" not in payloads["WUKH9"]
    assert payloads["WUKH8"]["name"].startswith("Home address")


def test_a_full_sync_capped_by_max_records_keeps_existing_names(tmp_path):
    dataverse = NamedAccountsDataverse()
    engine, _ = _engine(tmp_path, dataverse, records=NAMELESS)
    engine.sync_full(["WUK"], max_records=10, sync_picklists=False)
    key = engine.config.require_key_field("account")
    payloads = {payload[key]: payload for payload in dataverse.account_payloads.values()}
    assert "name" not in payloads["WUKH9"]


def test_role_rows_stop_between_batches_when_the_time_budget_is_spent(tmp_path, monkeypatch):
    # The roles are written after the last affiliation batch. Without a check
    # there, a phase that ended near the budget wrote every role and ran into
    # the 60-minute scale-in stop.
    from integration.dynamic_sync import SyncPaused

    clock = [0.0]
    monkeypatch.setattr("integration.dynamic_sync.time.monotonic", lambda: clock[0])
    dataverse = BulkDataverse()
    original = dataverse.bulk_upsert_by_field

    def slow_upsert(*args, **kwargs):
        clock[0] += 100
        return original(*args, **kwargs)

    dataverse.bulk_upsert_by_field = slow_upsert
    engine, _ = _engine(tmp_path, dataverse)
    engine.bulk_batch_size = 2
    engine.deadline = 150
    rows = [{"ts_name": f"WUKA{n} - 28"} for n in range(6)]
    affiliations = {"processed": 3}

    with pytest.raises(SyncPaused) as paused:
        engine._upsert_affiliation_roles(rows, affiliations)

    assert len([u for u in dataverse.upserts if u[0] == "ts_affiliationroles"]) == 2
    assert paused.value.phase == "activity_affiliations"
    assert paused.value.stats is affiliations
