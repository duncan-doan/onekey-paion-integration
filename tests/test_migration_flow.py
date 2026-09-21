"""Picklist quarantine / gap reporting during transform."""

from pathlib import Path

from integration.dynamic_mapping import load_mapping_config
from integration.hierarchy import edges_from_hcos
from integration.transform import create_transform_engine


def test_decode_unmapped_omits_field_and_records_gap(tmp_path: Path):
    engine = create_transform_engine(load_mapping_config())
    engine.picklist_gaps.clear()

    source = {
        "workplaceEid": "WUKHCO0001",
        "officialName": "Test Hospital",
        "typeCode": "ZZZ_UNKNOWN_TYPE",  # not in value_mapping as this exact path — use fake
    }
    # Force an unmapped hcotype by using a code that is not in CSV.
    source["typeCode"] = "@@@NOT_A_REAL_HCOTYPE@@@"

    payload = engine.transform_hco(source)
    assert "ts_type" not in payload
    assert any(
        g.mapping_type == "hcotype" and g.source_value == "@@@NOT_A_REAL_HCOTYPE@@@"
        for g in engine.picklist_gaps.gaps
    )

    report = engine.picklist_gaps.write_report(tmp_path / "gaps.csv")
    assert report is not None
    text = report.read_text(encoding="utf-8")
    assert "@@@NOT_A_REAL_HCOTYPE@@@" in text
    assert "hcotype" in text


def test_decode_mapped_hcotype_sets_picklist():
    engine = create_transform_engine(load_mapping_config())
    engine.picklist_gaps.clear()
    payload = engine.transform_hco(
        {
            "workplaceEid": "WUKHCO0001",
            "officialName": "Hospital",
            "typeCode": "HOS",
        }
    )
    expected = engine.config.get_value("hcotype", "HOS")
    assert expected is not None
    assert str(payload.get("ts_type")) == str(expected)
    assert not engine.picklist_gaps.gaps


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
