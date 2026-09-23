"""Tests for HCO–HCO relation normalization and transform."""

import json
from pathlib import Path

from integration.dynamic_mapping import load_mapping_config
from integration.transform import create_transform_engine
from integration.w2w_relations import (
    canonical_external_id,
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


def test_bidirectional_edges_share_canonical_key():
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
    assert len(exploded) == 2
    assert exploded[0]["ts_externalid"] == exploded[1]["ts_externalid"]
    assert exploded[0]["ts_externalid"] == canonical_external_id(
        "WUKE00117965", "WUKE00001260", "AUT"
    )


def test_dedupe_keeps_one_row_per_link():
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
    assert len(deduped) == 1
    assert deduped[0]["childWorkplaceEid"] == "WUKE00117965"
    assert deduped[0]["parentWorkplaceEid"] == "WUKE00001260"


def test_transform_w2w_sets_child_and_parent_binds():
    engine = create_transform_engine(load_mapping_config())
    edge = {
        "childWorkplaceEid": "WUKE00117965",
        "parentWorkplaceEid": "WUKE00001260",
        "linkCode": "AUT",
        "ts_externalid": canonical_external_id(
            "WUKE00117965", "WUKE00001260", "AUT"
        ),
        "beginValidDate": "2009-11-25T12:00:00Z",
        "endValidDate": "9999-12-31T12:00:00Z",
    }
    payload = engine.transform_w2w_affiliation(
        edge,
        child_account_id="child-guid",
        parent_account_id="parent-guid",
    )
    assert payload["ts_externalid"] == edge["ts_externalid"]
    assert payload["ts_fromid_account@odata.bind"] == "/accounts(child-guid)"
    assert payload["ts_toid_account@odata.bind"] == "/accounts(parent-guid)"
    assert payload["ts_linkcode"] == engine.config.get_value("linkcode", "AUT")
    assert payload["ts_isprimary"] is False


def test_transform_w2w_hie_link_is_primary():
    engine = create_transform_engine(load_mapping_config())
    edge = {
        "childWorkplaceEid": "WUKE00117965",
        "parentWorkplaceEid": "WUKE00001260",
        "linkCode": "HIE",
        "ts_externalid": canonical_external_id(
            "WUKE00117965", "WUKE00001260", "HIE"
        ),
    }
    payload = engine.transform_w2w_affiliation(edge)
    assert payload["ts_isprimary"] is True


def test_collect_w2w_relations_on_sample_file():
    if not HCO_SAMPLE.is_file():
        return
    data = json.loads(HCO_SAMPLE.read_text())
    workplaces = data.get("results", data)
    raw = len(explode_w2w_relations(workplaces))
    deduped = len(collect_w2w_relations(workplaces))
    assert deduped < raw
    assert deduped > 0
