"""Country lookup binds ts_country by the ts_onekeycode alternate key."""

from integration.activity_affiliation import flatten_activity_for_affiliation
from integration.dynamic_mapping import load_mapping_config
from integration.transform import create_transform_engine

BIND = "/ts_countries(ts_onekeycode='WDE')"


def _engine():
    return create_transform_engine(load_mapping_config())


def test_hco_and_hcp_bind_country_from_codbase():
    engine = _engine()
    account = engine.transform_hco(
        {"workplaceEid": "WDEH0001", "officialName": "Klinik", "codBase": "WDE"}
    )
    contact = engine.transform_hcp(
        {"individualEid": "WDER0001", "lastName": "Lotze", "codBase": "WDE"}
    )
    assert account["ts_Country@odata.bind"] == BIND
    assert contact["ts_Country@odata.bind"] == BIND


def test_blank_codbase_leaves_country_empty():
    engine = _engine()
    account = engine.transform_hco(
        {"workplaceEid": "WDEH0001", "officialName": "Klinik"}
    )
    assert "ts_Country@odata.bind" not in account


def test_activity_and_workplace_relation_bind_country():
    engine = _engine()
    flat = flatten_activity_for_affiliation(
        {
            "codBase": "WDE",
            "activity": {
                "activityEid": "WDEA1",
                "individualEid": "WDER1",
                "workplaceEid": "WDEH1",
            },
            "individual": {"individualEid": "WDER1", "lastName": "Lotze"},
            "workplace": {"workplaceEid": "WDEH1", "usualName": "Klinik"},
        }
    )
    activity = engine.transform_activity_affiliation(flat, "contact-id", "account-id")
    relation = engine.transform_w2w_affiliation(
        {
            "fromWorkplaceEid": "WDEH1",
            "toWorkplaceEid": "WDEH2",
            "childWorkplaceEid": "WDEH1",
            "parentWorkplaceEid": "WDEH2",
            "linkCode": "HIE",
            "ts_externalid": "WDEH1 - WDEH2 - HIE",
            "codBase": "WDE",
        },
        "from-id",
        "to-id",
    )
    assert activity["ts_Country@odata.bind"] == BIND
    assert relation["ts_Country@odata.bind"] == BIND
