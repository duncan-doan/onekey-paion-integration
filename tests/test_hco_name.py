"""Nameless HCO account names: type, postal place, and one HCP."""

from integration.dynamic_mapping import load_mapping_config
from integration.hco_name import hcp_name_by_workplace, nameless_account_name
from integration.transform import create_transform_engine


def test_named_hco_keeps_official_name():
    engine = create_transform_engine(load_mapping_config())
    payload = engine.transform_hco(
        {
            "workplaceEid": "WDEH1",
            "officialName": "Klinik",
            "typeLabel": "Hospital",
            "address_city": "Berlin",
            "hcpName": "Ada Lovelace",
        }
    )
    assert payload["name"] == "Klinik"


def test_usual_name_still_wins_over_the_formula():
    engine = create_transform_engine(load_mapping_config())
    payload = engine.transform_hco(
        {
            "workplaceEid": "WDEH1",
            "usualName": "Praxis Nord",
            "typeLabel": "Surgery",
            "address_city": "Hamburg",
            "hcpName": "Ada Lovelace",
        }
    )
    assert payload["name"] == "Praxis Nord"


def test_nameless_hco_uses_type_postal_place_and_one_hcp():
    engine = create_transform_engine(load_mapping_config())
    payload = engine.transform_hco(
        {
            "workplaceEid": "WDEH1",
            "typeLabel": "Pharmacy",
            "typeCode": "PHA",
            "address_city": "Berlin",
            "address_postalCode": "10115",
            "hcpName": "Andrea Ey",
        }
    )
    assert payload["name"] == "Pharmacy Berlin 10115 Andrea Ey"


def test_nameless_hco_omits_blank_parts_and_falls_back_to_noname():
    assert nameless_account_name({"typeCode": "PHA", "address_postalCode": "10115"}) == "PHA 10115"
    assert nameless_account_name({}) == "noname"


def test_many_hcps_prefer_the_main_activity_then_alphabetical_name():
    individuals = [
        {"individual": {"individualEid": "I1", "firstName": "Zoe", "lastName": "Zimmer"}},
        {"individual": {"individualEid": "I2", "firstName": "Ada", "lastName": "Lovelace"}},
        {"individual": {"individualEid": "I3", "firstName": "Boris", "lastName": "Hait"}},
    ]
    activities = [
        {"activity": {"workplaceEid": "W1", "individualEid": "I1", "isMainActivity": False}},
        {"activity": {"workplaceEid": "W1", "individualEid": "I2", "isMainActivity": True}},
        {"activity": {"workplaceEid": "W1", "individualEid": "I3", "isMainActivity": True}},
        {"activity": {"workplaceEid": "W2", "individualEid": "I1", "isMainActivity": False}},
        {"activity": {"workplaceEid": "W2", "individualEid": "I3", "isMainActivity": False}},
    ]
    names = hcp_name_by_workplace(individuals, activities)
    assert names["W1"] == "Ada Lovelace"
    assert names["W2"] == "Boris Hait"
