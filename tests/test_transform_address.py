"""Composite customeraddress key: workplaceEid - addressEid."""

from integration.dynamic_mapping import load_mapping_config
from integration.transform import create_transform_engine


def _shared_address_row(workplace_eid: str, account_guid: str = None) -> dict:
    """Two HCOs share the same OneKey addressEid / street details."""
    return {
        "workplaceEid": workplace_eid,
        "address_addressEid": "WUKADDR0001",
        "addressEid": "WUKADDR0001",
        "address_street": "10 Shared Street",
        "address_city": "London",
        "address_postalCode": "SW1A 1AA",
        "address_country": "GB",
        "address_addressType": "P",
        "buildingLabel": "Shared Building",
        "_account_guid": account_guid,
    }


def test_shared_address_produces_duplicate_rows_with_distinct_keys():
    engine = create_transform_engine(load_mapping_config())

    hco_a = _shared_address_row("WUKHCO000A")
    hco_b = _shared_address_row("WUKHCO000B")

    payload_a = engine.transform_address(hco_a, parent_account_id="guid-a")
    payload_b = engine.transform_address(hco_b, parent_account_id="guid-b")

    assert payload_a is not None and payload_b is not None
    key = engine.config.require_key_field("customeraddress")
    assert payload_a[key] == "WUKHCO000A - WUKADDR0001"
    assert payload_b[key] == "WUKHCO000B - WUKADDR0001"
    assert payload_a[key] != payload_b[key]

    # Same physical address content
    assert payload_a["line1"] == payload_b["line1"] == "10 Shared Street"
    assert payload_a["city"] == payload_b["city"] == "London"
    assert payload_a["postalcode"] == payload_b["postalcode"]

    assert payload_a["parentid_account@odata.bind"] == "/accounts(guid-a)"
    assert payload_b["parentid_account@odata.bind"] == "/accounts(guid-b)"


def test_compose_address_external_id_and_get_key_value():
    engine = create_transform_engine(load_mapping_config())
    source = {
        "workplaceEid": "WUKHCO000A",
        "address_addressEid": "WUKADDR0001",
    }
    assert engine.compose_address_external_id(source) == "WUKHCO000A - WUKADDR0001"
    assert engine.get_key_value(source, "customeraddress") == "WUKHCO000A - WUKADDR0001"


def test_transform_address_skips_incomplete_ids():
    engine = create_transform_engine(load_mapping_config())
    assert engine.transform_address({"workplaceEid": "WUKHCO000A"}) is None
    assert engine.transform_address({"addressEid": "WUKADDR0001"}) is None
    assert engine.compose_address_external_id({}) is None


def test_post_office_box_comes_from_the_address_not_the_workplace_id():
    from onekey.parse import flatten_hco

    engine = create_transform_engine(load_mapping_config())
    entry = {"address": {"addressEid": "WDEA1", "addressShortLabel": "Hauptstr 1"}}
    raw = {
        "codBase": "WDE",
        "workplace": {
            "workplaceEid": "WDEH00012345",
            "officialName": "Klinik",
            "workplaceAddresses": {"P_1": {**entry, "postalBoxLabel": "Postfach 12 34"}},
        },
    }
    payload = engine.transform_address(flatten_hco(raw), parent_account_id="guid")
    assert payload["postofficebox"] == "Postfach 12 34"

    # ADR-010 used to read workplaceEid, so every address got its own
    # workplace id as post-office box.
    raw["workplace"]["workplaceAddresses"]["P_1"] = entry
    payload = engine.transform_address(flatten_hco(raw), parent_account_id="guid")
    assert "postofficebox" not in payload


def test_the_post_office_box_keeps_the_box_without_its_postcode_and_town():
    # OneKey NL: "Postbus 10400,8000 GK,ZWOLLE" passed the 20-character
    # postofficebox column and was cut off inside the postcode.
    from onekey.parse import flatten_hco

    engine = create_transform_engine(load_mapping_config())
    for label, box in (("Postbus 10400,8000 GK,ZWOLLE", "Postbus 10400"),
                       ("Postbus 12, 5660 AB Geldrop", "Postbus 12"),
                       ("Postfach 12 34", "Postfach 12 34")):
        raw = {"codBase": "WNL", "workplace": {"workplaceEid": "WNLH1", "officialName": "X", "workplaceAddresses": {
            "P,1": {"typeCode": "P", "postalBoxLabel": label,
                    "address": {"addressEid": "WNLA1", "addressShortLabel": "Hoofdstraat 1"}}}}}
        payload = engine.transform_address(flatten_hco(raw), parent_account_id="guid")
        assert payload["postofficebox"] == box
