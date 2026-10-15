"""Picklist quarantine / gap reporting during transform."""

from pathlib import Path

import pytest

from dataverse.exceptions import DataverseHTTPError
from integration.dynamic_mapping import load_mapping_config
from integration.hierarchy import apply_updates, edges_from_hcos
from integration.transform import CATALOG_NAVIGATION, create_transform_engine


def test_lookup_without_codbase_records_gap(tmp_path: Path):
    engine = create_transform_engine(load_mapping_config())
    engine.picklist_gaps.clear()

    payload = engine.transform_hco(
        {
            "workplaceEid": "WUKHCO0001",
            "officialName": "Test Hospital",
            "typeCode": "HOS",
        }
    )
    assert "ts_type_ref@odata.bind" not in payload
    assert "ts_type" not in payload
    assert any(
        g.mapping_type == "TET" and g.source_value == "HOS"
        for g in engine.picklist_gaps.gaps
    )

    report = engine.picklist_gaps.write_report(tmp_path / "gaps.csv")
    assert report is not None
    text = report.read_text(encoding="utf-8")
    assert "HOS" in text
    assert "TET" in text


def test_lookup_hcotype_binds_full_external_id():
    engine = create_transform_engine(load_mapping_config())
    engine.picklist_gaps.clear()
    payload = engine.transform_hco(
        {
            "workplaceEid": "WUKHCO0001",
            "officialName": "Hospital",
            "codBase": "WUK",
            "typeCode": "HOS",
            "specialtyCode1": "01",
            "activityLocationCode": "H",
            "legalCategoryCode": "PRV",
        }
    )
    assert (
        payload["ts_Type_ref@odata.bind"]
        == "/ts_hcotype_refs(ts_externalid='WUK.TET.HOS')"
    )
    assert (
        payload["ts_Specialty1_ref@odata.bind"]
        == "/ts_specialty_refs(ts_externalid='WUK.SP.01')"
    )
    assert (
        payload["ts_Category_ref@odata.bind"]
        == "/ts_hcocategory_refs(ts_externalid='WUK.LEX.H')"
    )
    assert (
        payload["ts_LegalCategory_ref@odata.bind"]
        == "/ts_hcolegalcategory_refs(ts_externalid='WUK.CAJ.PRV')"
    )
    assert "ts_type" not in payload
    assert not engine.picklist_gaps.gaps


def test_every_catalog_lookup_has_a_navigation_name():
    config = load_mapping_config()
    fields = {
        fm.target_field
        for fm in config.field_mappings
        if fm.is_lookup and config.get_lookup_table(fm.lookup_entity)
    }
    assert fields == set(CATALOG_NAVIGATION)


def test_map_unmapped_gender_omits_and_gaps():
    engine = create_transform_engine(load_mapping_config())
    engine.picklist_gaps.clear()
    payload = engine.transform_hcp(
        {
            "individualEid": "WUKI0001",
            "lastName": "Smith",
            "genderCode": "X",
        }
    )
    assert "gendercode" not in payload
    assert any(g.source_value == "X" for g in engine.picklist_gaps.gaps)


def test_choices_and_states_are_json_integers():
    engine = create_transform_engine(load_mapping_config())
    account = engine.transform_hco({
        "workplaceEid": "WUKH1",
        "officialName": "Hospital",
        "activityLocationCode": "H",
        "stateCode": "3",
    })
    contact = engine.transform_hcp({
        "individualEid": "WUKI1",
        "lastName": "Doe",
        "genderCode": "F",
        "stateCode": "9",
        "preferredLanguageEID": "en",
    })

    assert account["industrycode"] == 11
    assert account["statecode"] == 0
    assert contact["gendercode"] == 2
    assert contact["statecode"] == 1
    assert contact["ts_preferredlanguage"] == 126870001
    for payload in (account, contact):
        for field in ("industrycode", "statecode", "gendercode", "ts_preferredlanguage", "ts_substatus"):
            if field in payload:
                assert type(payload[field]) is int
    # Numeric identifiers remain strings; only typed option columns convert.
    assert contact["ts_externalid"] == "WUKI1"


def test_edges_from_hcos():
    edges = edges_from_hcos(
        [
            {"workplaceEid": "A", "parentWorkplaceEid": "P"},
            {"workplaceEid": "B", "parentWorkplaceEid": None},
            {"workplaceEid": "C", "parentWorkplaceEid": "C"},
            {"workplaceEid": "D", "parentWorkplaceEid": "P"},
        ]
    )
    assert edges == [("A", "P"), ("D", "P")]


def test_edges_from_hcos_filters_to_contracted():
    edges = edges_from_hcos(
        [
            {"workplaceEid": "A", "parentWorkplaceEid": "P"},
            {"workplaceEid": "D", "parentWorkplaceEid": "P"},
            {"workplaceEid": "E", "parentWorkplaceEid": "OUT"},
        ],
        allowed_workplace_eids={"A", "D", "P"},
    )
    assert edges == [("A", "P"), ("D", "P")]


def test_transform_hco_does_not_include_address1_fields():
    engine = create_transform_engine(load_mapping_config())
    payload = engine.transform_hco(
        {
            "workplaceEid": "WUKHCO0001",
            "officialName": "Hospital",
            "address_street": "1 Main St",
            "address_city": "London",
        },
        include_address=True,  # deprecated kwarg; must still skip address1_*
    )
    assert not any(k.startswith("address1_") for k in payload)


def test_catalog_row_that_cannot_be_created_is_a_gap_not_a_crash():
    # One new code must not stop every account of the codebase.
    def failing_resolver(*args):
        raise RuntimeError("Dataverse rejected the catalog row")

    engine = create_transform_engine(load_mapping_config())
    engine.picklist_gaps.clear()
    engine.catalog_resolver = failing_resolver
    payload = engine.transform_hco(
        {
            "workplaceEid": "WUKHCO0001",
            "officialName": "Hospital",
            "codBase": "WUK",
            "typeCode": "HOS",
        }
    )
    assert payload["name"] == "Hospital"
    assert "ts_Type_ref@odata.bind" not in payload
    assert any(
        g.mapping_type == "TET" and g.source_value == "HOS"
        for g in engine.picklist_gaps.gaps
    )


class _HierarchyDataverse:
    """update_multiple fails with ``bulk``; single updates fail per account id."""

    def __init__(self, bulk, single=None):
        self.bulk = bulk
        self.single = single or {}
        self.updates = []

    def update_multiple(self, entity_set, records, logical_name=None):
        raise DataverseHTTPError("bulk failed", status_code=self.bulk)

    def update(self, entity_set, record_id, data, attempts=None):
        self.updates.append(record_id)
        if record_id in self.single:
            raise DataverseHTTPError("single failed", status_code=self.single[record_id])
        return record_id


def _links(count):
    return [
        {
            "accountid": f"acc-{n}",
            "parentaccountid@odata.bind": "/accounts(parent)",
            "ts_externalid": f"W{n}",
        }
        for n in range(count)
    ]


def test_a_rejected_hierarchy_link_is_recorded_and_the_rest_continue():
    dataverse = _HierarchyDataverse(bulk=400, single={"acc-1": 400})
    failures = []
    linked, failed = apply_updates(
        dataverse, _links(3), 100, match_field="ts_externalid", failures=failures
    )
    assert (linked, failed) == (2, 1)
    assert [label for label, _ in failures] == ["W1"]


def test_a_service_error_stops_hierarchy_links_at_the_first_single_write():
    # During an outage every link would fail the same way, three attempts each.
    dataverse = _HierarchyDataverse(bulk=503, single={"acc-0": 503})
    with pytest.raises(DataverseHTTPError):
        apply_updates(dataverse, _links(3), 100, match_field="ts_externalid", failures=[])
    assert dataverse.updates == ["acc-0"]


def test_hierarchy_links_are_not_written_singly_after_an_auth_failure():
    dataverse = _HierarchyDataverse(bulk=403)
    with pytest.raises(DataverseHTTPError):
        apply_updates(dataverse, _links(3), 100, match_field="ts_externalid", failures=[])
    assert dataverse.updates == []


# The preferredLanguageEID codes OneKey sends for each country (UAT, Oct 2026).
ONEKEY_LANGUAGES = {"en": 126870001, "de": 1031, "nl": 1043, "sv": 1053, "no": 1044, "da": 1030, "fi": 1035}


@pytest.mark.parametrize("code, option", sorted(ONEKEY_LANGUAGES.items()))
def test_every_onekey_language_decodes_to_its_option(code, option):
    engine = create_transform_engine(load_mapping_config())
    contact = engine.transform_hcp(
        {"individualEid": "WUKI1", "lastName": "Doe", "preferredLanguageEID": code}
    )
    assert contact["ts_preferredlanguage"] == option


def test_the_language_choice_syncs_the_same_rows_the_contact_decodes():
    # The option sync must create every value the decode can write.
    config = load_mapping_config()
    rule = next(
        fm.transform_rule
        for fm in config.get_field_mappings("contact")
        if fm.target_field == "ts_preferredlanguage"
    )
    decode_type = rule.split(":", 1)[1]
    targets = Path(__file__).resolve().parent.parent / "config" / "picklist_targets.csv"
    synced = [
        line.split(",")[0]
        for line in targets.read_text(encoding="utf-8").splitlines()
        if ",ts_preferredlanguage," in line
    ]
    assert synced == [decode_type]
