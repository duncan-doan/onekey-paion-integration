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
    # roles decoded to option values, comma-joined
    assert "ts_role" in payload
    assert "," in payload["ts_role"] or payload["ts_role"].isdigit()
    assert payload.get("ts_name")
