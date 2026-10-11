"""Picklist quarantine / gap reporting during transform."""

from pathlib import Path

from integration.dynamic_mapping import load_mapping_config
from integration.hierarchy import edges_from_hcos
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
