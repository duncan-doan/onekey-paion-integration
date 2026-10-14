"""Country lookup binds ts_country by row id, or by the ts_onekeycode alternate key."""

from dataverse.client import _expected_columns
from integration.activity_affiliation import flatten_activity_for_affiliation
from integration.dynamic_mapping import load_mapping_config
from integration.dynamic_sync import DynamicSyncEngine
from integration.lookup_sync import CountryResolver
from integration.sync_logger import SyncLogger
from integration.transform import create_transform_engine

BIND = "/ts_countries(ts_onekeycode='WDE')"
WDE_ID = "9c6c130b-91c3-f111-aaaf-70a8a52cd4fb"
WNL_ID = "1f0e2a4c-91c3-f111-aaaf-70a8a52cd4fb"


class Countries:
    """retrieve_multiple stub for ts_countries that records its queries."""

    def __init__(self, rows):
        self.rows = rows
        self.queries = []

    def retrieve_multiple(self, entity_set, **kwargs):
        self.queries.append((entity_set, kwargs))
        return list(self.rows)


COUNTRY_ROWS = [
    {"ts_onekeycode": "WDE", "ts_countryid": WDE_ID},
    {"ts_onekeycode": "wnl ", "ts_countryid": WNL_ID},
]


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


# -- bind by row id (skip unchanged can compare it) ------------------------------


def test_country_binds_by_row_id_once_the_table_is_read():
    engine = _engine()
    countries = Countries(COUNTRY_ROWS)
    engine.country_resolver = CountryResolver(countries).bind
    account = engine.transform_hco(
        {"workplaceEid": "WDEH0001", "officialName": "Klinik", "codBase": "WDE"}
    )
    contact = engine.transform_hcp(
        {"individualEid": "WNLR0001", "lastName": "Visser", "codBase": "WNL"}
    )
    assert account["ts_Country@odata.bind"] == f"/ts_countries({WDE_ID})"
    assert contact["ts_Country@odata.bind"] == f"/ts_countries({WNL_ID})"
    assert countries.queries == [
        ("ts_countries", {"select": ["ts_onekeycode", "ts_countryid"]})
    ]


def test_skip_unchanged_compares_the_row_id_bind_only():
    by_id = {"ts_Country@odata.bind": f"/ts_countries({WDE_ID})"}
    assert _expected_columns(by_id, set()) == {"_ts_country_value": WDE_ID}
    # A bind by alternate key cannot be compared, so the record is always written.
    assert _expected_columns({"ts_Country@odata.bind": BIND}, set()) is None


def test_a_code_the_table_lacks_keeps_the_alternate_key_bind():
    resolver = CountryResolver(Countries(COUNTRY_ROWS))
    assert resolver.bind("ts_onekeycode", "WSE") == "/ts_countries(ts_onekeycode='WSE')"


def test_an_unreadable_country_table_keeps_the_alternate_key_bind_and_is_read_once():
    class Down:
        calls = 0

        def retrieve_multiple(self, entity_set, **kwargs):
            Down.calls += 1
            raise RuntimeError("HTTP 503")

    resolver = CountryResolver(Down())
    assert resolver.bind("ts_onekeycode", "WDE") == BIND
    assert resolver.bind("ts_onekeycode", "WDE") == BIND
    assert Down.calls == 1


def test_the_sync_engine_binds_countries_by_row_id(tmp_path):
    engine = DynamicSyncEngine(
        object(), Countries(COUNTRY_ROWS), sync_logger=SyncLogger(log_dir=tmp_path)
    )
    account = engine.transformer.transform_hco(
        {"workplaceEid": "WDEH0001", "officialName": "Klinik", "codBase": "WDE"}
    )
    assert account["ts_Country@odata.bind"] == f"/ts_countries({WDE_ID})"
