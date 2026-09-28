"""Tests for HCO–HCO relation normalization and transform."""

import json
from pathlib import Path

from integration.dynamic_mapping import load_mapping_config
from integration.transform import create_transform_engine
from integration.w2w_relations import (
    affiliation_external_id,
    collect_w2w_relations,
    dedupe_w2w_relations,
    explode_w2w_relations,
    normalize_hco_hco_edge,
)

HCO_SAMPLE = Path(__file__).resolve().parents[2] / "output" / "hco" / "hco_300.json"


def _relation(is_parent: bool, related: str, link: str = "AUT") -> dict:
    return {
        "typeCode": "workplaceToWorkplaceLink",
        "isParent": is_parent,
        "onekeyEid": related,
        "linkCode": link,
        "beginValidDate": "2009-11-25T12:00:00Z",
        "endValidDate": "9999-12-31T12:00:00Z",
    }


def test_normalize_parent_side_is_parent_false():
    rel = _relation(False, "WUKE00117965")
    assert normalize_hco_hco_edge("WUKE00001260", rel) == (
        "WUKE00117965",
        "WUKE00001260",
        "AUT",
    )


def test_normalize_child_side_is_parent_true():
    rel = _relation(True, "WUKE00001260")
    assert normalize_hco_hco_edge("WUKE00117965", rel) == (
        "WUKE00117965",
        "WUKE00001260",
        "AUT",
    )


def test_normalize_ignores_mai_from_both_sides():
    assert normalize_hco_hco_edge(
        "WUKE00001260", _relation(False, "WUKE00001277", link="MAI")
    ) is None
    assert normalize_hco_hco_edge(
        "WUKE00001277", _relation(True, "WUKE00001260", link="MAI")
    ) is None


def test_normalize_skips_missing_or_non_boolean_is_parent():
    missing = _relation(True, "WUKE00001260")
    del missing["isParent"]
    assert normalize_hco_hco_edge("WUKE00117965", missing) is None

    # A string is truthy, so "false" must not be read as a parent link.
    as_string = _relation(True, "WUKE00001260")
    as_string["isParent"] = "false"
    assert normalize_hco_hco_edge("WUKE00117965", as_string) is None


def test_collect_keeps_hie_and_drops_matching_mai():
    parent_row = {
        "workplace": {
            "workplaceEid": "WUKE00001260",
            "relations": [
                _relation(False, "WUKE00001277", link="HIE"),
                _relation(False, "WUKE00001277", link="MAI"),
            ],
        }
    }
    child_row = {
        "workplace": {
            "workplaceEid": "WUKE00001277",
            "relations": [
                _relation(True, "WUKE00001260", link="HIE"),
                _relation(True, "WUKE00001260", link="MAI"),
            ],
        }
    }
    edges = collect_w2w_relations([parent_row, child_row])
    assert [(e["fromWorkplaceEid"], e["toWorkplaceEid"], e["linkCode"])
            for e in edges] == [
        ("WUKE00001260", "WUKE00001277", "HIE"),
        ("WUKE00001277", "WUKE00001260", "HIE"),
    ]


def test_each_side_of_a_link_gets_its_own_key():
    parent_row = {
        "workplace": {
            "workplaceEid": "WUKE00001260",
            "relations": [_relation(False, "WUKE00117965")],
        }
    }
    child_row = {
        "workplace": {
            "workplaceEid": "WUKE00117965",
            "relations": [_relation(True, "WUKE00001260")],
        }
    }
    exploded = explode_w2w_relations([parent_row, child_row])
    assert [e["ts_externalid"] for e in exploded] == [
        "WUKE00001260 - WUKE00117965 - AUT",
        # The child's side keeps the previous child - parent - linkCode key.
        "WUKE00117965 - WUKE00001260 - AUT",
    ]
    assert [(e["fromWorkplaceEid"], e["toWorkplaceEid"]) for e in exploded] == [
        ("WUKE00001260", "WUKE00117965"),
        ("WUKE00117965", "WUKE00001260"),
    ]
    # Both rows still record which side is the parent.
    assert {(e["childWorkplaceEid"], e["parentWorkplaceEid"]) for e in exploded} == {
        ("WUKE00117965", "WUKE00001260")
    }


def test_dedupe_keeps_both_sides_of_a_link():
    parent_row = {
        "workplace": {
            "workplaceEid": "WUKE00001260",
            "relations": [_relation(False, "WUKE00117965")],
        }
    }
    child_row = {
        "workplace": {
            "workplaceEid": "WUKE00117965",
            "relations": [_relation(True, "WUKE00001260")],
        }
    }
    deduped = dedupe_w2w_relations(explode_w2w_relations([parent_row, child_row]))
    assert len(deduped) == 2


def test_dedupe_drops_a_side_seen_twice():
    parent_row = {
        "workplace": {
            "workplaceEid": "WUKE00001260",
            "relations": [_relation(False, "WUKE00117965")],
        }
    }
    deduped = dedupe_w2w_relations(explode_w2w_relations([parent_row, parent_row]))
    assert [e["ts_externalid"] for e in deduped] == [
        "WUKE00001260 - WUKE00117965 - AUT"
    ]


def test_transform_w2w_binds_from_and_to():
    engine = create_transform_engine(load_mapping_config())
    # The parent's side of the link, so from/to differ from child/parent.
    edge = {
        "fromWorkplaceEid": "WUKE00001260",
        "toWorkplaceEid": "WUKE00117965",
        "childWorkplaceEid": "WUKE00117965",
        "parentWorkplaceEid": "WUKE00001260",
        "linkCode": "AUT",
        "ts_externalid": affiliation_external_id(
            "WUKE00001260", "WUKE00117965", "AUT"
        ),
        "beginValidDate": "2009-11-25T12:00:00Z",
        "endValidDate": "9999-12-31T12:00:00Z",
    }
    payload = engine.transform_w2w_affiliation(
        edge,
        from_account_id="from-guid",
        to_account_id="to-guid",
    )
    # The transform rebuilds the key from config, so this checks W2W-001/001b.
    assert payload["ts_externalid"] == "WUKE00001260 - WUKE00117965 - AUT"
    assert payload["ts_FromID_account@odata.bind"] == "/accounts(from-guid)"
    assert payload["ts_ToID@odata.bind"] == "/accounts(to-guid)"
    assert payload["ts_linktype"] == engine.config.get_value("linkcode", "AUT")
    assert payload["ts_isprimary"] is False


def test_transform_w2w_hie_is_primary_on_both_sides():
    engine = create_transform_engine(load_mapping_config())
    for from_eid, to_eid in (
        ("WUKE00001260", "WUKE00117965"),
        ("WUKE00117965", "WUKE00001260"),
    ):
        edge = {
            "fromWorkplaceEid": from_eid,
            "toWorkplaceEid": to_eid,
            "linkCode": "HIE",
            "ts_externalid": affiliation_external_id(from_eid, to_eid, "HIE"),
        }
        assert engine.transform_w2w_affiliation(edge)["ts_isprimary"] is True


def test_collect_w2w_filters_out_of_contract_related():
    parent_row = {
        "workplace": {
            "workplaceEid": "WUKE00001260",
            "relations": [
                _relation(False, "WUKE00117965"),
                _relation(False, "WUKOUTSIDE01"),
            ],
        }
    }
    child_row = {
        "workplace": {
            "workplaceEid": "WUKE00117965",
            "relations": [_relation(True, "WUKE00001260")],
        }
    }
    allowed = {"WUKE00001260", "WUKE00117965"}
    edges = collect_w2w_relations(
        [parent_row, child_row], allowed_workplace_eids=allowed
    )
    # Both sides of the in-contract link are kept; the outside one is dropped.
    assert [(e["fromWorkplaceEid"], e["toWorkplaceEid"]) for e in edges] == [
        ("WUKE00001260", "WUKE00117965"),
        ("WUKE00117965", "WUKE00001260"),
    ]


def test_collect_w2w_relations_on_sample_file():
    if not HCO_SAMPLE.is_file():
        return
    data = json.loads(HCO_SAMPLE.read_text())
    workplaces = data.get("results", data)
    raw = len(explode_w2w_relations(workplaces))
    deduped = len(collect_w2w_relations(workplaces))
    assert deduped <= raw
    assert deduped > 0
