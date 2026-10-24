"""Numbered OneKey slots (specialties, telephones): an empty slot clears its column."""

import pytest

from integration import create_transform_engine, load_mapping_config
from onekey.parse import compose_hcp_result, flatten_hco, flatten_hcp


@pytest.fixture(scope="module")
def engine():
    return create_transform_engine(load_mapping_config())


def _specialties(*codes):
    return {f"SP,{rank}": {"listCode": "SP", "code": code, "rank": rank} for rank, code in enumerate(codes, 1)}


def test_a_specialty_onekey_removed_clears_the_slot_it_leaves(engine):
    # OneKey had A, B, C and now has B, C. Without clearing, slot 3 kept C:
    # the contact showed B, C, C.
    hcp = flatten_hcp(compose_hcp_result(
        {"codBase": "WUK", "individual": {"individualEid": "WUKI1", "lastName": "Doe", "ada": _specialties("B", "C")}}
    ))
    payload = engine.transform_hcp(hcp)
    assert payload["ts_Specialty1_ref@odata.bind"] and payload["ts_Specialty2_ref@odata.bind"]
    assert [payload[f"ts_Specialty{slot}_ref@odata.bind"] for slot in (3, 4, 5)] == [None, None, None]


def test_a_telephone_onekey_removed_clears_its_slot(engine):
    hco = flatten_hco({"codBase": "WUK", "workplace": {
        "workplaceEid": "WUKH1", "officialName": "One", "telephones": {"1,1": {"callNumberForSearch": "0123"}}}})
    payload = engine.transform_hco(hco)
    assert payload["telephone1"] == "0123"
    assert "telephone2" in payload and payload["telephone2"] is None


def test_a_record_without_the_slots_leaves_them_as_they_are(engine):
    payload = engine.transform_hcp({"individualEid": "WUKI1", "lastName": "Doe", "codBase": "WUK"})
    assert not any(key.startswith("ts_Specialty") for key in payload)
