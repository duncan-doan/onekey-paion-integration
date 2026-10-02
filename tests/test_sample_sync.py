"""Offline tests for the OneKey to Dataverse sample."""

import pytest

from integration.dynamic_sync import DynamicSyncEngine
from integration.job_trace import (
    JOB_HCO,
    JOBS,
    STATUS_SUCCEEDED,
    TRIGGER_TIMER,
    JobTracer,
)
from integration.sample_sync import sync_sample


class FakeDataverse:
    def __init__(self):
        self.created = []
        self.updated = []

    def create(self, entity_set, data, return_record=False):
        self.created.append((entity_set, data))
        return f"guid-{len(self.created)}"

    def update(self, entity_set, record_id, data):
        self.updated.append((entity_set, record_id, dict(data)))
        return record_id

    def find_one(self, entity_set, filter=None, select=None):
        return {"ts_integrationjobid": "parent-guid"}


class FakeOneKey:
    def __init__(self, hcos, activities):
        self.hcos = hcos
        self.activities = activities
        self.searches = []

    def search_hco(self, **kwargs):
        self.searches.append(("hco", kwargs))
        return self.hcos

    def search_hcp(self, **kwargs):
        self.searches.append(("hcp", kwargs))
        return self.activities


class Engine:
    _begin_job = DynamicSyncEngine._begin_job
    _checkpoint = DynamicSyncEngine._checkpoint
    _finish_job = DynamicSyncEngine._finish_job

    def __init__(self, onekey, in_dataverse=()):
        self.onekey = onekey
        self.dataverse = FakeDataverse()
        self.tracer = JobTracer(self.dataverse)
        self._account_cache = {}
        self._in_dataverse = set(in_dataverse)
        self.calls = []

    def _sync_hcos(self, codebase, flattened=None, **kwargs):
        eids = [flat["workplaceEid"] for flat in flattened]
        self.calls.append(("accounts", eids))
        self._account_cache.update({eid: f"acc-{eid}" for eid in eids})
        stats = {"processed": len(eids), "created": len(eids), "updated": 0, "failed": 0}
        return stats, flattened, []

    def _sync_addresses(self, flats, **kwargs):
        self.calls.append(("addresses", len(flats)))
        return {"processed": 0, "created": 0, "updated": 0, "failed": 0}

    def _lookup_accounts_bulk(self, eids):
        found = {eid: f"acc-{eid}" for eid in eids if eid in self._in_dataverse}
        self._account_cache.update(found)
        return found

    def _sync_hierarchy(self, flats, allowed_workplace_eids=None, **kwargs):
        self.calls.append(("hierarchy", set(allowed_workplace_eids)))
        return {"edges": 1, "linked": 1}

    def _sync_hco_relations(self, raws, allowed_workplace_eids=None, **kwargs):
        self.calls.append(("affiliations", set(allowed_workplace_eids)))
        return {"processed": 0, "created": 0, "updated": 0, "failed": 0}

    def _sync_hcps(self, codebase, composed=None, **kwargs):
        ids = [record["individual"]["individualEid"] for record in composed]
        self.calls.append(("contacts", ids))
        return {"processed": len(ids), "created": len(ids), "updated": 0, "failed": 0}

    def _sync_activity_affiliations(self, records, **kwargs):
        self.calls.append(("activity_affiliations", len(records)))
        return {"processed": 0, "created": 0, "updated": 0, "failed": 0}


def _workplace(eid, parent=None, related=()):
    return {
        "entityType": "WORKPLACE",
        "workplace": {
            "workplaceEid": eid,
            "managerWorkplaceEid": parent,
            "relations": [{"onekeyEid": other} for other in related],
        },
    }


def _activity(individual, workplace, main):
    return {
        "entityType": "ACTIVITY",
        "activity": {"individualEid": individual, "workplaceEid": workplace, "isMainActivity": main},
        "individual": {"individualEid": individual},
        "workplace": {"workplaceEid": workplace},
    }


HCOS = [
    _workplace("WUKH1", parent="WUKH9", related=["WUKH8", "WUKH2"]),
    _workplace("WUKH2"),
]
ACTIVITIES = [
    _activity("WUKM1", "WUKH1", True),
    _activity("WUKM1", "WUKH2", False),
    _activity("WUKM2", "WUKH2", False),
    _activity("WUKM3", "WUKH2", True),
]


def test_sample_upserts_hcos_and_related_data():
    onekey = FakeOneKey(HCOS, ACTIVITIES)
    # The parent is already in Dataverse; the other related HCO is not.
    engine = Engine(onekey, in_dataverse={"WUKH9"})
    engine.tracer.retry_of_job_id = "parent-job"

    result = sync_sample(engine, codebase="WUK")

    assert result["success"] is True
    hco_search, hcp_search = onekey.searches
    assert hco_search[1]["is_in_contract"] is True
    assert hco_search[1]["result_size"] == 5
    criterion = hcp_search[1]["fields"][0]
    assert criterion["name"] == "workplace.workplaceEid"
    assert criterion["values"] == ["WUKH1", "WUKH2"]
    assert hcp_search[1]["is_in_contract"] is True

    allow = {"WUKH1", "WUKH2", "WUKH9"}
    assert engine.calls == [
        ("accounts", ["WUKH1", "WUKH2"]),
        ("addresses", 2),
        ("hierarchy", allow),
        ("affiliations", allow),
        ("contacts", ["WUKM1", "WUKM3"]),
        ("activity_affiliations", 2),
    ]

    entity_set, job = engine.dataverse.created[0]
    assert entity_set == JOBS
    assert job["ts_jobtype"] == JOB_HCO
    assert job["ts_trigger"] == TRIGGER_TIMER
    assert "ts_Retryofjob@odata.bind" not in job
    assert engine.dataverse.updated[-1][2]["ts_status"] == STATUS_SUCCEEDED
    assert result["job_id"] == job["ts_jobid"]


def test_sample_without_hcos_writes_nothing():
    onekey = FakeOneKey([], ACTIVITIES)
    engine = Engine(onekey)
    result = sync_sample(engine, codebase="WUK")
    assert result["success"] is False
    assert result["errors"] == ["OneKey returned no contracted HCOs for WUK"]
    assert [kind for kind, _ in onekey.searches] == ["hco"]
    assert engine.dataverse.created == []


def test_sample_finishes_the_job_when_a_phase_raises():
    engine = Engine(FakeOneKey(HCOS, ACTIVITIES))

    def boom(*args, **kwargs):
        raise RuntimeError("Dataverse down")

    engine._sync_hcps = boom
    with pytest.raises(RuntimeError):
        sync_sample(engine, codebase="WUK")
    final = engine.dataverse.updated[-1][2]
    assert "ts_completedon" in final
    assert "Dataverse down" in final["ts_errorsummary"]
