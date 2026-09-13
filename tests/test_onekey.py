"""Offline tests for the OneKey client.

These stub the transport, so they run without credentials or network access.
See tests/test_live.py for the tests that actually call UAT.
"""

import json

import pytest

from onekey import (
    ACTIVITY,
    WORKPLACE,
    OneKeyAuthError,
    OneKeyClient,
    OneKeyConfig,
    OneKeyConfigError,
    OneKeyForbiddenError,
    OneKeyRateLimitError,
    OneKeySession,
    basic_auth_header,
    flatten_hcp,
    flatten_hco,
    search_field,
)
from onekey.exceptions import OneKeyAPIResponseError


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.text = text if payload is None else json.dumps(payload)

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class FakeTransport:
    """Stands in for requests.Session, recording calls and replaying answers."""

    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []
        self.headers = {}

    def request(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        return self.responses.pop(0)

    def close(self):
        pass


def make_session(responses):
    config = OneKeyConfig("https://okws-uat.onekey.iqvia.com", "u", "p", "494")
    return OneKeySession(config, session=FakeTransport(responses))


def search_payload(results, **extra):
    body = {"status": "SUCCESS", "success": True, "results": results}
    body.update(extra)
    return {"response": body}


# -- config ---------------------------------------------------------------


def test_connector_id_goes_in_the_path_not_a_query_param():
    config = OneKeyConfig("https://okws-uat.onekey.iqvia.com/", "u", "p", "494")
    assert config.url("/ok/search") == "https://okws-uat.onekey.iqvia.com/ok/search/494"


def test_connection_test_url_omits_the_connector():
    config = OneKeyConfig("https://x", "u", "p", "494")
    assert config.url("/system/connection/test", with_connector=False) == (
        "https://x/system/connection/test"
    )


@pytest.mark.parametrize("missing", ["base_url", "username", "password", "connector_id"])
def test_blank_settings_are_rejected(missing):
    kwargs = {
        "base_url": "https://x",
        "username": "u",
        "password": "p",
        "connector_id": "494",
    }
    kwargs[missing] = "  "
    with pytest.raises(OneKeyConfigError):
        OneKeyConfig(**kwargs)


def test_password_is_not_in_the_repr():
    config = OneKeyConfig("https://x", "u", "hunter2", "494")
    assert "hunter2" not in repr(config)


# -- auth -----------------------------------------------------------------


def test_basic_auth_header_is_base64_of_user_colon_password():
    # 494_PAION_API_UAT:secret -> base64
    assert basic_auth_header("user", "pass") == "Basic dXNlcjpwYXNz"


def test_authorization_header_is_applied_to_every_request():
    session = make_session([FakeResponse(200, search_payload([]))])
    session.post_json("/ok/search", {})
    assert session._session.headers["Authorization"] == basic_auth_header("u", "p")


def test_401_raises_auth_error():
    session = make_session([FakeResponse(401, text="denied")])
    with pytest.raises(OneKeyAuthError):
        session.test_connection()


def test_403_raises_forbidden_error():
    session = make_session([FakeResponse(403, text="nope")])
    with pytest.raises(OneKeyForbiddenError):
        session.test_connection()


def test_429_is_retried_then_surfaces_as_rate_limit(monkeypatch):
    monkeypatch.setattr("onekey.auth.time.sleep", lambda _: None)
    session = make_session([FakeResponse(429) for _ in range(3)])
    with pytest.raises(OneKeyRateLimitError):
        session.test_connection()
    assert len(session._session.calls) == 3  # exhausted max_retries


def test_transient_500_then_success(monkeypatch):
    monkeypatch.setattr("onekey.auth.time.sleep", lambda _: None)
    session = make_session([FakeResponse(500), FakeResponse(200, search_payload([]))])
    assert session.post_json("/ok/search", {}) == {
        "status": "SUCCESS",
        "success": True,
        "results": [],
    }


def test_payload_level_error_status_raises_even_on_http_200():
    session = make_session(
        [FakeResponse(200, {"response": {"status": "ERROR", "statusMessage": "bad"}})]
    )
    with pytest.raises(OneKeyAPIResponseError):
        session.post_json("/ok/search", {})


def test_utf8_charset_is_declared_on_json_posts():
    session = make_session([FakeResponse(200, search_payload([]))])
    session.post_json("/ok/search", {"a": 1})
    headers = session._session.calls[0]["headers"]
    assert headers["Content-Type"] == "application/json; charset=utf-8"


# -- request building -----------------------------------------------------


def test_search_field_rejects_an_unknown_method():
    with pytest.raises(ValueError):
        search_field("individual.lastName", "X", method="REGEX")


def test_search_field_wraps_a_scalar_into_a_list():
    assert search_field("a", "b")["values"] == ["b"]


def test_cod_base_and_iso_cod2_are_mutually_exclusive():
    client = OneKeyClient(make_session([]))
    with pytest.raises(ValueError, match="not both"):
        client.search(ACTIVITY, [], cod_base="WDE", iso_cod2="DE")


def test_a_territory_is_required():
    client = OneKeyClient(make_session([]))
    with pytest.raises(ValueError, match="required"):
        client.search(ACTIVITY, [])


def test_search_hcp_builds_an_activity_request():
    session = make_session([FakeResponse(200, search_payload([]))])
    OneKeyClient(session).search_hcp(cod_base="WDE", last_name="Schmidt")

    body = session._session.calls[0]["json"]
    assert body["entityType"] == ACTIVITY
    assert body["codBase"] == ["WDE"]
    names = [f["name"] for f in body["fields"]]
    assert "individual.lastName" in names
    assert "activity.statusCode" in names  # only_valid defaults to True


def test_search_hco_builds_a_workplace_request():
    session = make_session([FakeResponse(200, search_payload([]))])
    OneKeyClient(session).search_hco(cod_base="WDE", name="Klinik", only_valid=False)

    body = session._session.calls[0]["json"]
    assert body["entityType"] == WORKPLACE
    assert [f["name"] for f in body["fields"]] == ["workplace.officialName"]


def test_search_needs_at_least_one_criterion():
    client = OneKeyClient(make_session([]))
    with pytest.raises(ValueError, match="criterion"):
        client.search_hcp(cod_base="WDE", only_valid=False)


# -- population paging ----------------------------------------------------


def test_iter_population_walks_packets_until_complete():
    pages = [
        FakeResponse(
            200,
            search_payload(
                [{"onekeyEid": "A"}, {"onekeyEid": "B"}],
                isTransactionComplete=False,
                transactionUniqueIdentifier="tx1",
            ),
        ),
        FakeResponse(
            200,
            search_payload(
                [{"onekeyEid": "C"}],
                isTransactionComplete=True,
                transactionUniqueIdentifier="tx1",
            ),
        ),
    ]
    session = make_session(pages)
    records = list(OneKeyClient(session).iter_population(cod_base="WDE", page_size=100))

    assert [r["onekeyEid"] for r in records] == ["A", "B", "C"]
    second = session._session.calls[1]["json"]
    assert second["transactionUniqueIdentifier"] == "tx1"
    # Dropping codBase on a continuation makes OneKey answer 403.
    assert second["codBase"] == "WDE"


def test_iter_population_stops_at_max_records():
    session = make_session(
        [
            FakeResponse(
                200,
                search_payload(
                    [{"onekeyEid": str(i)} for i in range(5)],
                    isTransactionComplete=False,
                    transactionUniqueIdentifier="tx1",
                ),
            )
        ]
    )
    records = list(
        OneKeyClient(session).iter_population(cod_base="WDE", max_records=3)
    )
    assert len(records) == 3


def test_iter_population_stops_when_no_transaction_id_comes_back():
    # Without this guard the first packet would replay forever.
    session = make_session(
        [FakeResponse(200, search_payload([{"onekeyEid": "A"}], isTransactionComplete=False))]
    )
    assert len(list(OneKeyClient(session).iter_population(cod_base="WDE"))) == 1


@pytest.mark.parametrize("size", [99, 2001])
def test_iter_population_rejects_page_sizes_outside_onekey_limits(size):
    client = OneKeyClient(make_session([]))
    with pytest.raises(ValueError, match="between 100 and 2000"):
        list(client.iter_population(cod_base="WDE", page_size=size))


# -- parsing --------------------------------------------------------------

# Trimmed from a real UAT response for connector 494.
HCP_RESULT = {
    "onekeyEid": "WDER0011707001",
    "entityType": "ACTIVITY",
    "country": "DE",
    "codBase": "WDE",
    "isInContract": False,
    "activity": {
        "activityEid": "WDER0011707001",
        "individualEid": "WDER00117070",
        "workplaceEid": "WDEF02227530",
        "statusCode": "3",
    },
    "individual": {
        "individualEid": "WDER00117070",
        "firstName": "Ilenia",
        "lastName": "Mule",
        "genderCode": "F",
        "genderLabel": "Frau",
        "statusCode": "3",
        "typeCode": "PX",
        "typeCorporateLabel": "Pharmacist Assistant / Technician",
        "ada": {
            "SP,1": {
                "code": "1S",
                "codeLabel": "PKA",
                "codeCorporateLabel": "Pharmaceutical Commercial Assistant",
                "listCode": "SP",
                "rank": 1,
            }
        },
    },
    "workplace": {
        "workplaceEid": "WDEF02227530",
        "officialName": "Apotheke am Markt",
        "workplaceAddresses": {
            "S,2": {
                "rank": 2,
                "typeCode": "S",
                "address": {"addressLongLabel": "Nebenstr. 1"},
            },
            "P,1": {
                "rank": 1,
                "typeCode": "P",
                "address": {
                    "addressEid": "WDE00000036193",
                    "addressLongLabel": "Hauptstr. 8",
                    "longPostalCode": "34537",
                    "postalTownReference": {
                        "villageLabel": "Bad Wildungen",
                        "country": "DE",
                    },
                },
            },
        },
    },
}


def test_flatten_hcp_pulls_person_specialty_and_workplace():
    row = flatten_hcp(HCP_RESULT)
    assert row["lastName"] == "Mule"
    assert row["individualEid"] == "WDER00117070"
    assert row["activityEid"] == "WDER0011707001"
    assert row["specialtyCode"] == "1S"
    assert row["specialtyLabel"] == "Pharmaceutical Commercial Assistant"
    assert row["workplaceName"] == "Apotheke am Markt"
    assert row["isValid"] is True


def test_primary_address_prefers_type_p_over_a_lower_rank_key():
    row = flatten_hcp(HCP_RESULT)
    assert row["address_street"] == "Hauptstr. 8"
    assert row["address_postalCode"] == "34537"
    assert row["address_city"] == "Bad Wildungen"


def test_flatten_hco_reads_the_workplace_branch():
    row = flatten_hco(
        {
            "onekeyEid": "WDEH02963729",
            "codBase": "WDE",
            "country": "DE",
            "workplace": {
                "workplaceEid": "WDEH02963729",
                "officialName": "Klinik Wildetal",
                "usualName": "Kliniken Hartenstein",
                "statusCode": "3",
                "typeCode": "RKL",
                "typeCorporateLabel": "Rehabilitation Hospital",
                "workplaceAddresses": {},
            },
        }
    )
    assert row["officialName"] == "Klinik Wildetal"
    assert row["typeLabel"] == "Rehabilitation Hospital"
    assert row["isValid"] is True
    # Address columns are always present so exported rows share a schema.
    assert row["address_street"] is None
    assert row["address_city"] is None


def test_flattening_tolerates_missing_branches():
    assert flatten_hcp({})["lastName"] is None
    assert flatten_hco({})["officialName"] is None


def test_rows_keep_a_stable_column_set_with_or_without_an_address():
    with_address = flatten_hcp(HCP_RESULT)
    without_address = flatten_hcp({})
    assert set(with_address) == set(without_address)
