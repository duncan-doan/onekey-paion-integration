"""Offline tests for the sync engine's phases on a fake OneKey and Dataverse."""

import pytest

from dataverse.exceptions import DataverseHTTPError
from integration.dynamic_sync import DynamicSyncEngine
from integration.job_trace import FAILURES
from integration.sync_logger import SyncLogger


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
