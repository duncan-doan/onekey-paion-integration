"""Tests for Activity → ts_affiliation (AFF-*) transform."""

from integration.activity_affiliation import (
    flatten_activity_for_affiliation,
    _tih_role_codes,
)
from integration.dynamic_mapping import load_mapping_config
from integration.transform import create_transform_engine


def _activity_record(**kwargs):
    activity = {
        "activityEid": "WUKM0000000101",
        "individualEid": "WUKM00000001",
        "workplaceEid": "WUKE00001260",
        "isMainActivity": True,
        "workingStatusCode": "C",
        "statusCode": "3",
        "activityStartDate": "2020-01-01T00:00:00Z",
        "ada": {
            "TIH,1": {"code": "28", "listCode": "TIH", "rank": 1},
            "TIH,2": {"code": "AD", "listCode": "TIH", "rank": 2},
        },
    }
    activity.update(kwargs.pop("activity", {}))
    return {
        "onekeyEid": activity["activityEid"],
        "entityType": "ACTIVITY",
        "activity": activity,
        "individual": {
            "individualEid": "WUKM00000001",
            "firstName": "Ada",
            "lastName": "Lovelace",
        },
        "workplace": {
            "workplaceEid": "WUKE00001260",
            "usualName": "Test Hospital",
        },
        **kwargs,
    }


def test_tih_role_codes_ordered():
    codes = _tih_role_codes(
        {
            "ada": {
                "TIH,2": {"code": "AD", "listCode": "TIH", "rank": 2},
                "TIH,1": {"code": "28", "listCode": "TIH", "rank": 1},
            }
        }
    )
    assert codes == ["28", "AD"]


def test_flatten_activity_for_affiliation():
    flat = flatten_activity_for_affiliation(_activity_record())
    assert flat["activityEid"] == "WUKM0000000101"
    assert flat["individualEid"] == "WUKM00000001"
    assert flat["workplaceEid"] == "WUKE00001260"
    assert flat["role"] == "28"
    assert flat["ROLE_2"] == "AD"
    assert flat["isMainActivity"] is True
    assert "Ada Lovelace" in flat["ts_name"]


def test_transform_activity_affiliation_payload():
    engine = create_transform_engine(load_mapping_config())
    flat = flatten_activity_for_affiliation(_activity_record())
    payload = engine.transform_activity_affiliation(
        flat,
        contact_id="contact-guid",
        account_id="account-guid",
    )
    assert payload["ts_externalid"] == "WUKM0000000101"
    assert payload["ts_compoundkey"] == "WUKM00000001 - WUKE00001260"
    assert payload["ts_FromID_contact@odata.bind"] == "/contacts(contact-guid)"
    assert payload["ts_ToID@odata.bind"] == "/accounts(account-guid)"
    assert payload["ts_isprimary"] is True
    assert payload["ts_workingstatus"] in (126870001, "126870001")
    # Roles are rows on ts_affiliation_role, not a choice on ts_affiliation.
    assert "ts_role" not in payload
    assert payload.get("ts_name")


def test_role_lookup_uses_full_external_id():
    engine = create_transform_engine(load_mapping_config())
    role = next(
        fm for fm in engine.config.field_mappings if fm.mapping_id == "AFF-007"
    )
    engine.begin_record("ts_affiliationrole", "WUKM0000000101")
    payload = engine._catalog_payload(role, {"role": "28", "codBase": "WUK"})
    assert payload == {
        "ts_Role@odata.bind": "/ts_role_refs(ts_externalid='WUK.TIH.28')"
    }


def test_affiliation_role_rows_bind_parent_and_role():
    engine = create_transform_engine(load_mapping_config())
    flat = flatten_activity_for_affiliation(_activity_record(codBase="WUK"))
    rows = engine.affiliation_role_rows(flat, "affiliation-guid")
    assert rows == [
        {
            "ts_name": "WUKM0000000101 - 28",
            "ts_Affiliation@odata.bind": "/ts_affiliations(affiliation-guid)",
            "ts_Role@odata.bind": "/ts_role_refs(ts_externalid='WUK.TIH.28')",
        },
        {
            "ts_name": "WUKM0000000101 - AD",
            "ts_Affiliation@odata.bind": "/ts_affiliations(affiliation-guid)",
            "ts_Role@odata.bind": "/ts_role_refs(ts_externalid='WUK.TIH.AD')",
        },
    ]


def test_an_activity_without_its_id_gets_no_role_rows():
    # ts_name would be the bare code, so every such activity would update
    # (and re-parent) the same child row.
    engine = create_transform_engine(load_mapping_config())
    flat = flatten_activity_for_affiliation(_activity_record(codBase="WUK"))
    flat["activityEid"] = ""
    assert engine.affiliation_role_rows(flat, "affiliation-guid") == []


def test_transform_w2w_affiliation_compound_key_has_both_ends():
    engine = create_transform_engine(load_mapping_config())
    edge = {
        "childWorkplaceEid": "WUKH1",
        "parentWorkplaceEid": "WUKH2",
        "fromWorkplaceEid": "WUKH1",
        "toWorkplaceEid": "WUKH2",
    }
    payload = engine.transform_w2w_affiliation(
        edge, from_account_id="from-guid", to_account_id="to-guid"
    )
    # W2W-002b appends the parent; it used to overwrite the child.
    assert payload["ts_compoundkey"] == "WUKH1 - WUKH2"
