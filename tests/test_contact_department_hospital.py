"""contact.ts_department (HCP-010, the HCP's main workplace), contact.ts_hospital
(HCP-011, that workplace's parent) and contact.ts_hospitalgroup (HCP-012, the
hospital's parent). Each parent is OneKey's managerWorkplaceEid first, then the
HIE parent when that is a group (GOR/POR). They replace parentcustomerid and the
department text, which the sync no longer writes."""

import pytest

from integration import create_transform_engine, load_mapping_config
from integration.dynamic_sync import DynamicSyncEngine
from integration.sync_logger import SyncLogger
from onekey.parse import compose_hcp_result, flatten_hcp
from tests.test_dynamic_sync import BulkDataverse, PopulationOneKey

DEPT = "aaaaaaaa-0000-0000-0000-00000000000d"
HOSP = "aaaaaaaa-0000-0000-0000-00000000000b"
GROUP = "aaaaaaaa-0000-0000-0000-00000000000c"
HCP = {"individualEid": "WUKI1", "lastName": "Doe", "workplaceEid": "WUKH10", "codBase": "WUK", "usualName": "Cardiology"}


@pytest.fixture(scope="module")
def engine():
    return create_transform_engine(load_mapping_config())


def test_department_and_hospital_are_bound_to_their_accounts(engine):
    payload = engine.transform_hcp(HCP, account_id=DEPT, hospital_id=HOSP, hospital_known=True)
    assert payload["ts_Department@odata.bind"] == f"/accounts({DEPT})"
    assert payload["ts_Hospital@odata.bind"] == f"/accounts({HOSP})"
    assert "ts_department" not in payload and "ts_hospital" not in payload


def test_parentcustomerid_and_the_department_text_are_no_longer_written(engine):
    payload = engine.transform_hcp(HCP, account_id=DEPT, hospital_id=HOSP, hospital_known=True)
    assert not any(key.startswith("parentcustomerid") for key in payload)
    assert "department" not in payload


def test_a_workplace_without_a_parent_account_clears_the_hospital(engine):
    payload = engine.transform_hcp(HCP, account_id=DEPT, hospital_known=True)
    assert "ts_Hospital@odata.bind" in payload and payload["ts_Hospital@odata.bind"] is None


def test_an_unknown_hospital_is_left_as_it_is(engine):
    payload = engine.transform_hcp(HCP, account_id=DEPT)
    assert "ts_Hospital@odata.bind" not in payload
    assert payload["ts_Department@odata.bind"] == f"/accounts({DEPT})"


def test_no_workplace_account_leaves_the_department_out(engine):
    payload = engine.transform_hcp(HCP, hospital_known=True)
    assert "ts_Department@odata.bind" not in payload


def test_a_mapped_parentcustomerid_is_bound_to_the_workplace_too():
    """Adding the parentcustomerid row back to entity_mapping.csv writes it again."""
    config = load_mapping_config()
    department = next(fm for fm in config._field_by_target["contact"] if fm.target_field == "ts_department")
    config._field_by_target["contact"].append(type(department)(**{**department.__dict__, "mapping_id": "HCP-X", "target_field": "parentcustomerid"}))
    payload = create_transform_engine(config).transform_hcp(HCP, account_id=DEPT)
    assert payload["ts_Department@odata.bind"] == f"/accounts({DEPT})"


def test_an_unmapped_hospital_is_not_written():
    config = load_mapping_config()
    contact = config._field_by_target["contact"]
    config._field_by_target["contact"] = [fm for fm in contact if fm.target_field != "ts_hospital"]
    payload = create_transform_engine(config).transform_hcp(HCP, account_id=DEPT, hospital_id=HOSP, hospital_known=True)
    assert "ts_Hospital@odata.bind" not in payload


def test_flatten_hcp_carries_the_workplace_parent():
    row = flatten_hcp(compose_hcp_result(
        {"individual": {"individualEid": "WUKI1"}},
        {"activity": {"activityEid": "WUKA1", "individualEid": "WUKI1", "workplaceEid": "WUKH10"}},
        {"workplace": {"workplaceEid": "WUKH10", "managerWorkplaceEid": "WUKH20"}},
    ))
    assert row["hospitalWorkplaceEid"] == "WUKH20" and row["workplaceLoaded"] is True
    without = flatten_hcp(compose_hcp_result(
        {"individual": {"individualEid": "WUKI1"}},
        {"activity": {"activityEid": "WUKA1", "individualEid": "WUKI1", "workplaceEid": "WUKH10"}},
        None,
    ))
    assert without["hospitalWorkplaceEid"] is None and without["workplaceLoaded"] is False


class PayloadDataverse(BulkDataverse):
    """BulkDataverse that keeps each upserted payload by key, and knows the
    parent of the accounts in ``parents`` ({workplaceEid: parent id or None})."""

    def __init__(self, parents=None, **kwargs):
        super().__init__(**kwargs)
        self.payloads = {}
        self.parents = parents or {}

    def find_many_by_field(self, entity_set, match_field, values, select=None, **kwargs):
        if entity_set == "accounts" and "_parentaccountid_value" in (select or []):
            return {v: {"accountid": f"acc-{v}", "_parentaccountid_value": self.parents[v]} for v in values if v in self.parents}
        return super().find_many_by_field(entity_set, match_field, values, select=select, **kwargs)

    def bulk_upsert_by_field(self, entity_set, match_field, records, **kwargs):
        for record in records:
            self.payloads[(entity_set, record.get(match_field))] = record
        return super().bulk_upsert_by_field(entity_set, match_field, records, **kwargs)


def _row(individual, workplace, manager=None, loaded=True):
    return compose_hcp_result(
        {"individual": {"individualEid": individual, "lastName": "Doe"}, "codBase": "WUK"},
        {"activity": {"activityEid": f"A-{individual}", "individualEid": individual, "workplaceEid": workplace, "isMainActivity": True}},
        {"workplace": {"workplaceEid": workplace, "managerWorkplaceEid": manager}} if loaded else None,
    )


def test_contact_sync_binds_clears_or_leaves_the_hospital(tmp_path):
    dataverse = PayloadDataverse(parents={"WUKH11": HOSP, "WUKH12": None})
    sync = DynamicSyncEngine(PopulationOneKey([]), dataverse, sync_logger=SyncLogger(log_dir=tmp_path))
    sync._account_cache.update({"WUKH10": DEPT, "WUKH11": "dept-11", "WUKH12": "dept-12", "WUKH20": HOSP})
    sync._sync_hcps("WUK", composed=[
        _row("WUKI1", "WUKH10", manager="WUKH20"),   # hospital is an account
        _row("WUKI2", "WUKH11", manager="WUKH99"),   # hospital outside Dataverse
        _row("WUKI3", "WUKH12"),                     # workplace has no parent
        _row("WUKI4", "WUKH10", loaded=False),       # delta without the workplace, parent unknown
        _row("WUKI5", "WUKH11", loaded=False),       # delta: the workplace account has a parent
        _row("WUKI6", "WUKH12", loaded=False),       # delta: the workplace account has none
    ], partial={"WUKI4", "WUKI5", "WUKI6"})
    contact = lambda eid: dataverse.payloads[("contacts", eid)]

    assert contact("WUKI1")["ts_Department@odata.bind"] == f"/accounts({DEPT})"
    assert contact("WUKI1")["ts_Hospital@odata.bind"] == f"/accounts({HOSP})"
    for eid in ("WUKI2", "WUKI3"):
        assert contact(eid)["ts_Hospital@odata.bind"] is None
    assert "ts_Hospital@odata.bind" not in contact("WUKI4")
    assert contact("WUKI4")["ts_Department@odata.bind"] == f"/accounts({DEPT})"
    assert contact("WUKI5")["ts_Hospital@odata.bind"] == f"/accounts({HOSP})"
    assert contact("WUKI5")["ts_Department@odata.bind"] == "/accounts(dept-11)"
    assert contact("WUKI6")["ts_Hospital@odata.bind"] is None


def test_the_hospital_group_follows_the_hospital(engine):
    bound = engine.transform_hcp(HCP, account_id=DEPT, hospital_id=HOSP, hospital_known=True, group_id=GROUP)
    assert bound["ts_HospitalGroup@odata.bind"] == f"/accounts({GROUP})"
    cleared = engine.transform_hcp(HCP, account_id=DEPT, hospital_id=HOSP, hospital_known=True)
    assert "ts_HospitalGroup@odata.bind" in cleared and cleared["ts_HospitalGroup@odata.bind"] is None
    unknown = engine.transform_hcp(HCP, account_id=DEPT)
    assert "ts_HospitalGroup@odata.bind" not in unknown


GOR = 126870001   # structure type "Primary Organisation" (OneKey GOR / POR)
ETA = 126870004   # an organisation, not a group


class HierarchyDataverse(PayloadDataverse):
    """Answers the HIE relation and account reads of _parent_or_group."""

    def __init__(self, hie=None, accounts=None, **kwargs):
        super().__init__(**kwargs)
        self.hie = hie or {}            # hospital -> HIE parent
        self.accounts = accounts or {}  # account id -> row
        self.filters = []

    def retrieve_multiple(self, entity_set, select=None, filter=None, **kwargs):
        self.filters.append((entity_set, filter))
        ids = set(__import__("re").findall(r"[0-9a-f-]{36}|[a-z]+-\w+", filter or ""))
        if entity_set == "ts_affiliations":
            return [{"_ts_fromid_value": h, "_ts_toid_value": p} for h, p in self.hie.items() if h in ids]
        if entity_set == "accounts":
            return [{"accountid": a, **row} for a, row in self.accounts.items() if a in ids]
        return []


def test_hospital_group_is_the_parent_account_else_the_hie_parent_group(tmp_path):
    dataverse = HierarchyDataverse(
        hie={"hosp-a": "grp-a", "hosp-b": "org-b", "hosp-e": "grp-e"},
        accounts={
            "hosp-a": {}, "grp-a": {"ts_structuretype": GOR},
            "hosp-b": {}, "org-b": {"ts_structuretype": ETA},
            "hosp-c": {"_parentaccountid_value": "mgr-c"},
            "hosp-d": {},
            "hosp-e": {"_parentaccountid_value": "mgr-e"}, "grp-e": {"ts_structuretype": GOR},
        },
    )
    sync = DynamicSyncEngine(PopulationOneKey([]), dataverse, sync_logger=SyncLogger(log_dir=tmp_path))
    groups = sync._parent_or_group({"hosp-a", "hosp-b", "hosp-c", "hosp-d", "hosp-e"})
    assert groups == {
        "hosp-a": "grp-a",   # no parent account: the HIE parent, a group
        "hosp-b": None,      # no parent account, and the HIE parent is not a group
        "hosp-c": "mgr-c",   # the parent account
        "hosp-d": None,      # neither
        "hosp-e": "mgr-e",   # the parent account comes first, before an HIE group
    }
    relation_filter = next(f for es, f in dataverse.filters if es == "ts_affiliations")
    assert "ts_linktype eq 126870004" in relation_filter and "ts_isparent eq true" in relation_filter


def test_contact_sync_writes_the_hospital_group(tmp_path):
    dataverse = HierarchyDataverse(hie={HOSP: GROUP}, accounts={HOSP: {}, GROUP: {"ts_structuretype": GOR}})
    sync = DynamicSyncEngine(PopulationOneKey([]), dataverse, sync_logger=SyncLogger(log_dir=tmp_path))
    sync._account_cache.update({"WUKH10": DEPT, "WUKH20": HOSP, "WUKH12": "dept-12"})
    sync._sync_hcps("WUK", composed=[_row("WUKI1", "WUKH10", manager="WUKH20"), _row("WUKI3", "WUKH12")], partial=set())
    assert dataverse.payloads[("contacts", "WUKI1")]["ts_HospitalGroup@odata.bind"] == f"/accounts({GROUP})"
    assert dataverse.payloads[("contacts", "WUKI3")]["ts_HospitalGroup@odata.bind"] is None
    levels = {
        account_id: payload["ts_hierarchylevel"]
        for entity_set, account_id, payload in dataverse.updated
        if entity_set == "accounts" and "ts_hierarchylevel" in payload
    }
    assert levels == {DEPT: 3, "dept-12": 3, HOSP: 2, GROUP: 1}


def test_hierarchy_level_uses_the_most_direct_hcp_role_and_never_downgrades(tmp_path):
    dataverse = HierarchyDataverse(
        accounts={
            "account-shared": {"ts_hierarchylevel": 1},
            "account-existing": {"ts_hierarchylevel": 3},
        }
    )
    sync = DynamicSyncEngine(
        PopulationOneKey([]), dataverse, sync_logger=SyncLogger(log_dir=tmp_path)
    )

    updated = sync._set_account_hierarchy_levels(
        departments={"account-shared"},
        hospitals={"account-shared"},
        groups={"account-shared", "account-existing"},
    )

    levels = {
        account_id: payload["ts_hierarchylevel"]
        for entity_set, account_id, payload in dataverse.updated
        if entity_set == "accounts"
    }
    assert updated == 1
    assert levels == {"account-shared": 3}


def test_hierarchy_levels_use_update_multiple_batches_and_skip_matching_rows(tmp_path):
    class BatchedHierarchyDataverse(HierarchyDataverse):
        def __init__(self):
            super().__init__(
                accounts={
                    "account-0": {"ts_hierarchylevel": 3},
                    **{f"account-{index}": {} for index in range(1, 5)},
                }
            )
            self.level_batches = []

        def update_multiple(self, entity_set, records):
            assert entity_set == "accounts"
            self.level_batches.append(list(records))

    dataverse = BatchedHierarchyDataverse()
    sync = DynamicSyncEngine(
        PopulationOneKey([]),
        dataverse,
        sync_logger=SyncLogger(log_dir=tmp_path),
        bulk_batch_size=2,
    )

    updated = sync._set_account_hierarchy_levels(
        departments={f"account-{index}" for index in range(5)}
    )

    assert updated == 4
    assert [len(batch) for batch in dataverse.level_batches] == [2, 2]
    assert {
        row["accountid"]
        for batch in dataverse.level_batches
        for row in batch
    } == {"account-1", "account-2", "account-3", "account-4"}
    assert dataverse.updated == []  # no one-by-one fallback on the normal path

    account_reads = sum(es == "accounts" for es, _ in dataverse.filters)
    assert sync._set_account_hierarchy_levels(
        departments={f"account-{index}" for index in range(5)}
    ) == 0
    assert sum(es == "accounts" for es, _ in dataverse.filters) == account_reads


def test_full_sync_puts_hierarchy_levels_in_the_account_upsert(tmp_path):
    records = [
        {
            "entityType": "WORKPLACE",
            "workplace": {
                "workplaceEid": "WUKD1",
                "officialName": "Department",
                "managerWorkplaceEid": "WUKH1",
            },
        },
        {
            "entityType": "WORKPLACE",
            "workplace": {
                "workplaceEid": "WUKH1",
                "officialName": "Hospital",
                "relations": [
                    {
                        "typeCode": "workplaceToWorkplaceLink",
                        "onekeyEid": "WUKG1",
                        "linkCode": "HIE",
                        "isParent": True,
                    }
                ],
            },
        },
        {
            "entityType": "WORKPLACE",
            "workplace": {
                "workplaceEid": "WUKG1",
                "officialName": "Group",
                "structureTypeCode": "GOR",
            },
        },
        {
            "entityType": "INDIVIDUAL",
            "individual": {"individualEid": "WUKI1", "lastName": "Doe"},
        },
        {
            "entityType": "ACTIVITY",
            "activity": {
                "activityEid": "WUKA1",
                "individualEid": "WUKI1",
                "workplaceEid": "WUKD1",
                "isMainActivity": True,
            },
        },
    ]
    dataverse = PayloadDataverse()
    sync = DynamicSyncEngine(
        PopulationOneKey(records),
        dataverse,
        sync_logger=SyncLogger(log_dir=tmp_path),
    )

    result = sync.sync_full(
        ["WUK"],
        sync_picklists=False,
        sync_addresses=False,
        sync_hierarchy=False,
        sync_hco_relations=False,
        sync_activity_affiliations=False,
    )

    assert result["success"] is True
    assert {
        eid: dataverse.payloads[("accounts", eid)].get("ts_hierarchylevel")
        for eid in ("WUKD1", "WUKH1", "WUKG1")
    } == {"WUKD1": 3, "WUKH1": 2, "WUKG1": 1}
    assert not any(
        entity_set == "accounts" and "ts_hierarchylevel" in payload
        for entity_set, _account_id, payload in dataverse.updated
    )


def test_a_workplace_without_a_parent_account_takes_its_hie_group_as_hospital(tmp_path):
    dataverse = HierarchyDataverse(
        hie={"dept-30": "grp-30", "dept-31": "org-31", "dept-32": "grp-32"},
        accounts={"grp-30": {"ts_structuretype": GOR}, "org-31": {"ts_structuretype": ETA}, "grp-32": {"ts_structuretype": GOR}},
    )
    sync = DynamicSyncEngine(PopulationOneKey([]), dataverse, sync_logger=SyncLogger(log_dir=tmp_path))
    sync._account_cache.update({"WUKH30": "dept-30", "WUKH31": "dept-31", "WUKH32": "dept-32", "WUKH10": DEPT, "WUKH20": HOSP})
    sync._sync_hcps("WUK", composed=[
        _row("WUKI7", "WUKH30"),                     # no manager; HIE parent is a group
        _row("WUKI8", "WUKH31"),                     # no manager; HIE parent is an organisation
        _row("WUKI1", "WUKH10", manager="WUKH20"),   # the manager
        _row("WUKI9", "WUKH32", manager="WUKH20"),   # the manager comes before an HIE group
    ], partial=set())
    contact = lambda eid: dataverse.payloads[("contacts", eid)]
    assert contact("WUKI7")["ts_Hospital@odata.bind"] == "/accounts(grp-30)"
    assert contact("WUKI8")["ts_Hospital@odata.bind"] is None
    assert contact("WUKI1")["ts_Hospital@odata.bind"] == f"/accounts({HOSP})"
    assert contact("WUKI9")["ts_Hospital@odata.bind"] == f"/accounts({HOSP})"


class PlacementDataverse(HierarchyDataverse):
    """Also knows contacts, accounts by workplaceEid, and contacts by their lookups."""

    def __init__(self, contacts=(), workplaces=None, by_lookup=None, **kwargs):
        super().__init__(**kwargs)
        self.known_contacts = set(contacts)
        self.workplaces = workplaces or {}   # workplaceEid -> account id
        self.by_lookup = by_lookup or []     # contact rows returned for a lookup query
        self.upsert_kwargs = []

    def find_many_by_field(self, entity_set, match_field, values, select=None, **kwargs):
        if entity_set == "contacts":
            return {v: {"contactid": f"c-{v}"} for v in values if v in self.known_contacts}
        if entity_set == "accounts":
            return {v: {"accountid": self.workplaces[v]} for v in values if v in self.workplaces}
        return {}

    def retrieve_multiple(self, entity_set, select=None, filter=None, **kwargs):
        if entity_set == "contacts":
            self.filters.append((entity_set, filter))
            return list(self.by_lookup)
        return super().retrieve_multiple(entity_set, select=select, filter=filter, **kwargs)

    def bulk_upsert_by_field(self, entity_set, match_field, records, **kwargs):
        self.upsert_kwargs.append(dict(kwargs))
        kwargs.pop("update_only", None)
        return super().bulk_upsert_by_field(entity_set, match_field, records, **kwargs)


def test_refresh_moves_a_contact_and_recomputes_those_under_a_changed_workplace(tmp_path):
    dataverse = PlacementDataverse(
        contacts={"I1", "I3"},
        workplaces={"WNLH10": "dept-10", "WNLH20": "hosp-20", "WNLH30": "dept-30"},
        by_lookup=[{"ts_externalid": "I4", "_ts_department_value": "dept-20"},
                   {"ts_externalid": "I3", "_ts_department_value": "dept-20"}],
        hie={"hosp-10": "grp-10"},
        accounts={
            "dept-10": {"_parentaccountid_value": "hosp-10"}, "hosp-10": {}, "grp-10": {"ts_structuretype": GOR},
            "dept-20": {"_parentaccountid_value": "hosp-20"}, "hosp-20": {},
        },
    )
    sync = DynamicSyncEngine(PopulationOneKey([]), dataverse, sync_logger=SyncLogger(log_dir=tmp_path))
    stats, applied = sync._refresh_contact_placement(
        {"I1": "WNLH10", "I2": "WNLH10", "I3": "WNLH30"},   # I2 has no contact; I3 was written already
        {"WNLH20"},
        skip={"I3"},
    )
    assert applied == {"I1"}
    one, four = dataverse.payloads[("contacts", "I1")], dataverse.payloads[("contacts", "I4")]
    assert one["ts_Department@odata.bind"] == "/accounts(dept-10)"
    assert one["ts_Hospital@odata.bind"] == "/accounts(hosp-10)"
    assert one["ts_HospitalGroup@odata.bind"] == "/accounts(grp-10)"
    assert four["ts_Department@odata.bind"] == "/accounts(dept-20)"
    assert four["ts_Hospital@odata.bind"] == "/accounts(hosp-20)"
    assert four["ts_HospitalGroup@odata.bind"] is None
    levels = {
        account_id: payload["ts_hierarchylevel"]
        for entity_set, account_id, payload in dataverse.updated
        if entity_set == "accounts" and "ts_hierarchylevel" in payload
    }
    assert levels == {
        "dept-10": 3,
        "dept-20": 3,
        "hosp-10": 2,
        "hosp-20": 2,
        "grp-10": 1,
    }
    assert ("contacts", "I3") not in dataverse.payloads and ("contacts", "I2") not in dataverse.payloads
    assert all(kw.get("update_only") is True for kw in dataverse.upsert_kwargs)
    lookup_filter = next(f for es, f in dataverse.filters if es == "contacts")
    assert "_ts_department_value eq hosp-20" in lookup_filter and "_ts_hospital_value eq hosp-20" in lookup_filter
    assert stats["processed"] == 2


def test_hie_parent_ignores_ended_relations_and_takes_the_latest(tmp_path):
    class TwoParents(HierarchyDataverse):
        def retrieve_multiple(self, entity_set, select=None, filter=None, **kwargs):
            if entity_set == "ts_affiliations":
                self.filters.append((entity_set, filter))
                return [
                    {"_ts_fromid_value": "hosp-a", "_ts_toid_value": "grp-new", "ts_startdate": "2026-09-01T00:00:00Z"},
                    {"_ts_fromid_value": "hosp-a", "_ts_toid_value": "grp-old", "ts_startdate": "2020-01-01T00:00:00Z"},
                ]
            return super().retrieve_multiple(entity_set, select=select, filter=filter, **kwargs)
    dataverse = TwoParents(accounts={"grp-new": {"ts_structuretype": GOR}, "grp-old": {"ts_structuretype": GOR}})
    sync = DynamicSyncEngine(PopulationOneKey([]), dataverse, sync_logger=SyncLogger(log_dir=tmp_path))
    assert sync._hie_group_parents({"hosp-a"}) == {"hosp-a": "grp-new"}
    relation_filter = next(f for es, f in dataverse.filters if es == "ts_affiliations")
    assert "ts_enddate eq null or ts_enddate ge " in relation_filter
