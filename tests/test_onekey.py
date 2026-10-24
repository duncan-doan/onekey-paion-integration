"""Offline tests for the OneKey client.

These stub the transport, so they run without credentials or network access.
See tests/test_live.py for the tests that actually call UAT.
"""

import json
from datetime import datetime, timezone

import pytest
import requests

from onekey import (
    ACTIVITY,
    WORKPLACE,
    OneKeyAuthError,
    OneKeyClient,
    OneKeyConfig,
    OneKeyConfigError,
    OneKeyForbiddenError,
    OneKeyHTTPError,
    OneKeyRateLimitError,
    OneKeySession,
    basic_auth_header,
    flatten_hcp,
    flatten_hco,
    search_field,
)
from onekey.exceptions import OneKeyAPIResponseError


class FakeResponse:
    def __init__(self, status_code=200, payload=None, text="", headers=None):
        self.status_code = status_code
        self._payload = payload
        self.text = text if payload is None else json.dumps(payload)
        self.headers = headers or {}

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
        answer = self.responses.pop(0)
        if isinstance(answer, Exception):  # no response arrived
            raise answer
        return answer

    def close(self):
        pass


def make_session(responses, max_retries=3):
    config = OneKeyConfig(
        "https://okws-uat.onekey.iqvia.com", "u", "p", "494", max_retries=max_retries
    )
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


@pytest.mark.parametrize("base_url", ["http://x", "ftp://x", "https:///missing-host", "https://user:password@x"])
def test_basic_auth_requires_https_without_credentials_in_the_url(base_url):
    with pytest.raises(OneKeyConfigError, match="HTTPS"):
        OneKeyConfig(base_url, "u", "p", "494")


@pytest.mark.parametrize(
    "settings",
    [
        {"max_retries": 0},
        {"max_retries": -1},
        {"max_retries": 1.5},
        {"timeout": 0},
        {"timeout": float("nan")},
        {"packet_delay": -1},
        {"packet_delay": float("inf")},
    ],
)
def test_invalid_retry_and_timeout_settings_fail_at_configuration(settings):
    with pytest.raises(OneKeyConfigError):
        OneKeyConfig("https://x", "u", "p", "494", **settings)


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


def test_a_warning_envelope_is_returned_with_its_records():
    # Only ERROR raises. What success=false means on a WARNING search is not
    # documented, and a truncated search still carries its records.
    envelope = {
        "status": "WARNING",
        "success": False,
        "statusMessage": "Partial result",
        "results": [{"onekeyEid": "A"}],
    }
    session = make_session([FakeResponse(200, {"response": envelope})])
    assert session.post_json("/ok/search", {})["results"] == [{"onekeyEid": "A"}]


def test_a_population_packet_answered_with_success_false_is_requested_again(monkeypatch):
    # The vendor sample (Developer Guide 6.13) waits and asks again.
    sleeps = []
    monkeypatch.setattr("onekey.client.time.sleep", sleeps.append)
    session = make_session(
        [
            FakeResponse(200, {"response": {"status": "WARNING", "success": False}}),
            FakeResponse(
                200,
                search_payload(
                    [{"onekeyEid": "A"}],
                    isTransactionComplete=True,
                    transactionPacketIdentifier=1,
                ),
            ),
        ]
    )
    records = list(OneKeyClient(session).iter_population(cod_base="WDE"))
    assert [r["onekeyEid"] for r in records] == ["A"]
    assert len(session._session.calls) == 2
    assert sleeps == [5]


def test_a_population_packet_that_keeps_failing_stops_the_walk(monkeypatch):
    monkeypatch.setattr("onekey.client.time.sleep", lambda _: None)
    failed = FakeResponse(200, {"response": {"status": "WARNING", "success": False}})
    session = make_session([failed, failed, failed])
    with pytest.raises(OneKeyAPIResponseError, match="not delivered after 3"):
        list(OneKeyClient(session).iter_population(cod_base="WDE"))


@pytest.mark.parametrize("payload", [[], {"response": None}, {"response": []}])
def test_invalid_json_envelope_raises_a_onekey_error(payload):
    session = make_session([FakeResponse(200, payload)])
    with pytest.raises(OneKeyAPIResponseError):
        session.post_json("/ok/search", {})


@pytest.mark.parametrize("status", [429, 503])
def test_a_long_retry_after_is_capped_and_keeps_the_heartbeat_fresh(status, monkeypatch):
    # A huge Retry-After must not park the worker for hours while its
    # heartbeat keeps the job looking alive.
    sleeps, heartbeats = [], []
    monkeypatch.setattr("onekey.auth.time.sleep", sleeps.append)
    session = make_session(
        [FakeResponse(status, headers={"Retry-After": "3600"}), FakeResponse(200, search_payload([]))]
    )
    session.after_request = lambda: heartbeats.append(1)

    session.post_json("/ok/search", {})

    assert sum(sleeps) == 300
    assert max(sleeps) <= 60
    assert len(heartbeats) == 6  # before each minute of waiting and after the request
    assert len(session._session.calls) == 2


def test_a_short_retry_after_on_throttling_still_waits_the_floor(monkeypatch):
    # The quota is per minute: retrying a 429 within seconds only spends attempts.
    sleeps = []
    monkeypatch.setattr("onekey.auth.time.sleep", sleeps.append)
    session = make_session(
        [FakeResponse(429, headers={"Retry-After": "1"}), FakeResponse(200, search_payload([]))]
    )
    session.post_json("/ok/search", {})
    assert sum(sleeps) == 30


def test_http_date_retry_after_is_respected(monkeypatch):
    class FixedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return cls(2015, 10, 21, 7, 26, 0, tzinfo=timezone.utc)

    monkeypatch.setattr("onekey.auth.datetime", FixedDatetime)
    sleeps = []
    monkeypatch.setattr("onekey.auth.time.sleep", sleeps.append)
    session = make_session(
        [
            FakeResponse(429, headers={"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"}),
            FakeResponse(200, search_payload([])),
        ]
    )
    session.post_json("/ok/search", {})
    assert sum(sleeps) == 120


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


def test_search_rejects_more_than_thirty_criteria_before_calling_the_api():
    session = make_session([])
    with pytest.raises(ValueError, match="at most 30"):
        OneKeyClient(session).search(ACTIVITY, [search_field("a", "b")] * 31, cod_base="WDE")
    assert session._session.calls == []


def test_search_does_not_count_or_operators_as_criteria():
    session = make_session([FakeResponse(200, search_payload([]))])
    criteria = [search_field("a", "b"), "OR"] * 29 + [search_field("a", "b")]
    OneKeyClient(session).search(ACTIVITY, criteria, cod_base="WDE")
    assert session._session.calls[0]["json"]["fields"] == criteria


def test_search_rejects_combining_the_entitys_own_id_with_other_criteria():
    session = make_session([])
    with pytest.raises(ValueError, match="no other criterion"):
        OneKeyClient(session).search(
            ACTIVITY,
            [search_field("activity.activityEid", "WUKA1"), "OR", search_field("individual.lastName", "Smith")],
            cod_base="WUK",
        )
    assert session._session.calls == []


def test_a_related_id_is_an_ordinary_filter():
    # sample_sync's search: activities at the sampled workplaces, valid only.
    session = make_session([FakeResponse(200, search_payload([]))])
    OneKeyClient(session).search_hcp(
        cod_base="WUK",
        fields=[search_field("workplace.workplaceEid", ["WUKH1", "WUKH2"])],
        result_size=50,
        is_in_contract=True,
    )
    names = [f["name"] for f in session._session.calls[0]["json"]["fields"]]
    assert names == ["workplace.workplaceEid", "activity.statusCode"]


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


def test_iter_population_walks_packets_until_complete(monkeypatch):
    sleeps = []
    monkeypatch.setattr("onekey.client.time.sleep", sleeps.append)
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
    # Packets are paced to stay under OneKey's per-minute quota.
    assert sleeps == [session.config.packet_delay]  # between the two packets


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


def test_iter_population_fails_when_an_incomplete_packet_omits_the_transaction_id():
    # Silent success would advance the sync checkpoint past unreturned records.
    session = make_session(
        [FakeResponse(200, search_payload([{"onekeyEid": "A"}], isTransactionComplete=False))]
    )
    with pytest.raises(OneKeyAPIResponseError, match="incomplete"):
        list(OneKeyClient(session).iter_population(cod_base="WDE"))


@pytest.mark.parametrize("complete", [False, None, "false"])
def test_iter_population_rejects_empty_or_ambiguous_incomplete_packets(complete):
    session = make_session(
        [FakeResponse(200, search_payload([], isTransactionComplete=complete, transactionUniqueIdentifier="tx1"))]
    )
    with pytest.raises(OneKeyAPIResponseError):
        list(OneKeyClient(session).iter_population(cod_base="WDE"))


def _packet(records, number, complete):
    return FakeResponse(
        200,
        search_payload(
            records,
            isTransactionComplete=complete,
            transactionUniqueIdentifier="tx1",
            transactionPacketIdentifier=number,
        ),
    )


def test_iter_population_refetches_a_packet_lost_in_transit(monkeypatch):
    # A lost response is retried, and OneKey answers with the packet after it;
    # the walk asks for the missed one by number (Developer Guide 6.13.4).
    monkeypatch.setattr("onekey.client.time.sleep", lambda _: None)
    session = make_session(
        [
            _packet([{"onekeyEid": "A"}], 1, False),
            _packet([{"onekeyEid": "C"}], 3, False),
            _packet([{"onekeyEid": "B"}], 2, False),  # packet 2, asked for by number
            _packet([{"onekeyEid": "D"}], 4, True),
        ]
    )
    records = list(OneKeyClient(session).iter_population(cod_base="WDE", page_size=100))
    assert [r["onekeyEid"] for r in records] == ["A", "B", "C", "D"]
    refetch = session._session.calls[2]["json"]
    assert refetch["transactionPacketIdentifier"] == 2
    assert refetch["transactionUniqueIdentifier"] == "tx1"
    assert refetch["codBase"] == "WDE"
    assert "transactionPacketIdentifier" not in session._session.calls[3]["json"]


def _refused(code):
    return FakeResponse(400, {"response": {"status": "ERROR", "statusMessage": f"{code} refused"}})


def test_a_continuation_without_an_answer_resumes_with_the_delivered_packet(monkeypatch):
    # 23 Oct: a continuation timed out, was sent again, and OneKey refused it with
    # a 400. Developer Guide 6.13.4: ask for the delivered packet by number instead.
    monkeypatch.setattr("onekey.client.time.sleep", lambda _: None)
    session = make_session(
        [
            _packet([{"onekeyEid": "A"}], 1, False),
            requests.ReadTimeout("read timed out"),
            _packet([{"onekeyEid": "B"}], 2, False),  # packet 2, asked for by number
            _packet([{"onekeyEid": "C"}], 3, True),
        ]
    )
    records = list(OneKeyClient(session).iter_population(cod_base="WDE", page_size=100))
    assert [r["onekeyEid"] for r in records] == ["A", "B", "C"]
    calls = [call["json"] for call in session._session.calls]
    assert len(calls) == 4  # the lost continuation was not sent again blindly
    assert calls[2]["transactionPacketIdentifier"] == 2 and calls[2]["transactionUniqueIdentifier"] == "tx1"
    assert "transactionPacketIdentifier" not in calls[3]


def test_a_continuation_without_an_answer_asks_again_when_it_was_not_delivered(monkeypatch):
    monkeypatch.setattr("onekey.client.time.sleep", lambda _: None)
    session = make_session(
        [
            _packet([{"onekeyEid": "A"}], 1, False),
            requests.ConnectionError("connection reset"),
            _refused("OKWS-3208"),  # packet 2 by number: never delivered
            _packet([{"onekeyEid": "B"}], 2, True),  # so the next packet again
        ]
    )
    records = list(OneKeyClient(session).iter_population(cod_base="WDE", page_size=100))
    assert [r["onekeyEid"] for r in records] == ["A", "B"]
    assert "transactionPacketIdentifier" not in session._session.calls[3]["json"]


def test_a_walk_stops_when_the_resumed_continuation_is_refused(monkeypatch):
    monkeypatch.setattr("onekey.client.time.sleep", lambda _: None)
    session = make_session(
        [
            _packet([{"onekeyEid": "A"}], 1, False),
            requests.ReadTimeout("read timed out"),
            _refused("OKWS-3208"),
            _refused("OKWS-3653"),  # the next packet again: transaction sealed
        ]
    )
    with pytest.raises(OneKeyHTTPError, match="OKWS-3653"):
        list(OneKeyClient(session).iter_population(cod_base="WDE", page_size=100))


def test_a_refused_continuation_keeps_onekeys_reason_in_the_error(monkeypatch):
    monkeypatch.setattr("onekey.client.time.sleep", lambda _: None)
    session = make_session([_packet([{"onekeyEid": "A"}], 1, False), _refused("OKWS-3650")])
    with pytest.raises(OneKeyHTTPError, match="OKWS-3650"):
        list(OneKeyClient(session).iter_population(cod_base="WDE", page_size=100))


def test_dropping_pooled_sockets_keeps_the_session_and_its_load_balancer_cookie():
    transport = requests.Session()
    transport.cookies.set("BIGipServerokws.ok.imshealth.com_prod_http_pool", "node-2")
    config = OneKeyConfig("https://okws-uat.onekey.iqvia.com", "u", "p", "494")
    session = OneKeySession(config, session=transport)
    session._reset_connection_pool()
    assert session._session is transport
    assert transport.cookies.get("BIGipServerokws.ok.imshealth.com_prod_http_pool") == "node-2"


def test_iter_population_stops_when_too_many_packets_were_lost(monkeypatch):
    monkeypatch.setattr("onekey.client.time.sleep", lambda _: None)
    session = make_session(
        [_packet([{"onekeyEid": "A"}], 1, False), _packet([{"onekeyEid": "F"}], 6, False)]
    )
    with pytest.raises(OneKeyAPIResponseError, match="packet sequence"):
        list(OneKeyClient(session).iter_population(cod_base="WDE"))


def test_iter_population_rejects_a_packet_number_going_backwards(monkeypatch):
    monkeypatch.setattr("onekey.client.time.sleep", lambda _: None)
    session = make_session(
        [_packet([{"onekeyEid": "A"}], 1, False), _packet([{"onekeyEid": "A"}], 1, True)]
    )
    with pytest.raises(OneKeyAPIResponseError, match="packet sequence"):
        list(OneKeyClient(session).iter_population(cod_base="WDE"))


def test_iter_population_rejects_two_territories_before_calling_the_api():
    session = make_session([])
    with pytest.raises(ValueError, match="not both"):
        list(OneKeyClient(session).iter_population(cod_base="WDE", iso_cod2="DE"))
    assert session._session.calls == []


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


def test_qualifications_specialties_are_not_hidden_by_unrelated_ada():
    row = flatten_hcp(
        {
            "individual": {
                "ada": {"TIT,1": {"code": "DR"}},
                "qualifications": {
                    "SP,2": {"code": "SECOND"},
                    "SP,1": {"code": "FIRST"},
                },
            }
        }
    )
    assert row["specialtyCode1"] == "FIRST"
    assert row["specialtyCode2"] == "SECOND"


def test_flatten_hcp_falls_back_to_activity_eids_when_nested_entities_missing():
    """Population ACTIVITY rows carry link EIDs only on the activity object."""
    from onekey.parse import flatten_hcp as flatten

    row = flatten(
        {
            "onekeyEid": "WUKA0000408401",
            "entityType": "ACTIVITY",
            "activity": {
                "activityEid": "WUKA0000408401",
                "individualEid": "WUKA00004084",
                "workplaceEid": "WUKF06988766",
                "statusCode": "3",
            },
            "individual": None,
            "workplace": None,
        }
    )
    assert row["individualEid"] == "WUKA00004084"
    assert row["workplaceEid"] == "WUKF06988766"
    assert row["activityEid"] == "WUKA0000408401"
    assert row["firstName"] is None


def test_compose_hcp_result_joins_population_entity_rows():
    from onekey.parse import compose_hcp_result, flatten_hcp as flatten

    composed = compose_hcp_result(
        {
            "entityType": "INDIVIDUAL",
            "country": "GB",
            "codBase": "WUK",
            "individual": {
                "individualEid": "WUKA00004084",
                "firstName": "Jane",
                "lastName": "Doe",
                "statusCode": "3",
                "typeCorporateLabel": "Pharmacist",
            },
        },
        {
            "onekeyEid": "WUKA0000408401",
            "entityType": "ACTIVITY",
            "activity": {
                "activityEid": "WUKA0000408401",
                "individualEid": "WUKA00004084",
                "workplaceEid": "WUKF06988766",
                "isMainActivity": True,
            },
        },
        {
            "entityType": "WORKPLACE",
            "workplace": {
                "workplaceEid": "WUKF06988766",
                "officialName": "High Street Pharmacy",
            },
        },
    )
    row = flatten(composed)
    assert row["individualEid"] == "WUKA00004084"
    assert row["workplaceEid"] == "WUKF06988766"
    assert row["firstName"] == "Jane"
    assert row["lastName"] == "Doe"
    assert row["workplaceName"] == "High Street Pharmacy"


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


def test_primary_address_uses_short_label_when_long_missing():
    """UK OneKey often only sends addressShortLabel + buildingLabel."""
    from onekey.parse import primary_address

    row = primary_address(
        {
            "workplaceAddresses": {
                "P,1": {
                    "typeCode": "P",
                    "rank": 1,
                    "buildingLabel": "St Cadoc's Hospital",
                    "address": {
                        "addressEid": "WUK01933424023",
                        "addressShortLabel": "Lodge Road",
                        "longPostalCode": "NP18 3XQ",
                        "postalTownReference": {
                            "villageLabel": "Caerleon",
                            "dispatchLabel": "Newport",
                            "country": "GB",
                            "subdivisions": {
                                "SUB.3": {
                                    "typeCode": "COUNTY",
                                    "longLocalizedLabel": "Gwent",
                                },
                                "SUB.5": {
                                    "typeCode": "DISTRICT",
                                    "longLocalizedLabel": "CAERLEON",
                                },
                            },
                        },
                    },
                }
            }
        }
    )
    assert row["street"] == "Lodge Road"
    assert row["buildingLabel"] == "St Cadoc's Hospital"
    assert row["city"] == "Caerleon"
    assert row["county"] == "Gwent"
    assert row["district"] == "CAERLEON"
    assert row["postalCode"] == "NP18 3XQ"


def test_iter_population_uses_the_configured_page_size():
    session = make_session(
        [FakeResponse(200, search_payload([], isTransactionComplete=True))]
    )
    list(OneKeyClient(session).iter_population(cod_base="WDE"))
    assert session._session.calls[0]["json"]["resultSize"] == 1000


def test_page_size_comes_from_the_environment(monkeypatch):
    monkeypatch.setenv("ONEKEY_BASE_URL", "https://x")
    monkeypatch.setenv("ONEKEY_USERNAME", "u")
    monkeypatch.setenv("ONEKEY_PASSWORD", "p")
    monkeypatch.setenv("ONEKEY_CONNECTOR_ID", "494")
    monkeypatch.setenv("ONEKEY_PAGE_SIZE", "2000")
    assert OneKeyConfig.from_env().page_size == 2000
    monkeypatch.setenv("ONEKEY_PAGE_SIZE", "5000")
    with pytest.raises(OneKeyConfigError, match="page_size"):
        OneKeyConfig.from_env()


def test_after_request_hook_runs_after_each_call():
    session = make_session(
        [FakeResponse(200, search_payload([], isTransactionComplete=True))]
    )
    calls = []
    session.after_request = lambda: calls.append(1)
    list(OneKeyClient(session).iter_population(cod_base="WDE"))
    assert calls == [1]


def test_get_code_list_searches_the_list_and_flattens_its_codes():
    body = {
        "status": "SUCCESS",
        "success": True,
        "resultSize": 2,
        "results": {
            "WUK.TIT": {
                "uid": "WUK.TIT",
                "externalId": "TIT",
                "codBase": "WUK",
                "codes": {
                    "WUK.TIT.02": {"uid": "WUK.TIT.02", "externalId": "02", "longLabel": "Mx", "isValid": True},
                    "WUK.TIT.03": {"uid": "WUK.TIT.03", "externalId": "03", "longLabel": "Old", "isValid": False},
                },
            }
        },
    }
    session = make_session([FakeResponse(200, {"response": body})])
    codes = OneKeyClient(session).get_code_list("WUK", "TIT", iso_language="en")

    assert [code["externalId"] for code in codes] == ["02", "03"]  # invalid codes kept
    call = session._session.calls[0]
    assert call["url"].endswith("/ok/searchCodeList/494")
    assert call["json"] == {
        "codBase": "WUK",
        "isoLanguage": "en",
        "fields": [{"name": "list.externalId", "method": "EXACT", "values": ["TIT"]}],
    }


def test_get_code_list_without_results_is_empty():
    session = make_session([FakeResponse(200, {"response": {"status": "SUCCESS", "results": {}}})])
    assert OneKeyClient(session).get_code_list("WUK", "XYZ") == []


def test_after_request_hook_runs_before_each_retry_wait(monkeypatch):
    # Throttling waits run to minutes; the hook keeps a job's heartbeat fresh.
    events = []
    monkeypatch.setattr("onekey.auth.time.sleep", lambda delay: events.append("sleep"))
    session = make_session(
        [FakeResponse(503), FakeResponse(429, headers={"Retry-After": "1"}), FakeResponse(200, search_payload([]))]
    )
    session.after_request = lambda: events.append("hook")
    session.post_json("/ok/search", {})
    assert events == ["hook", "sleep", "hook", "sleep", "hook"]


def test_a_lost_packet_is_recovered_when_the_last_packet_omits_the_transaction_id(monkeypatch):
    monkeypatch.setattr("onekey.client.time.sleep", lambda _: None)
    last = FakeResponse(
        200,
        search_payload([{"onekeyEid": "C"}], isTransactionComplete=True, transactionPacketIdentifier=3),
    )
    session = make_session(
        [_packet([{"onekeyEid": "A"}], 1, False), last, _packet([{"onekeyEid": "B"}], 2, False)]
    )
    records = list(OneKeyClient(session).iter_population(cod_base="WDE"))
    assert [r["onekeyEid"] for r in records] == ["A", "B", "C"]
    assert session._session.calls[2]["json"]["transactionUniqueIdentifier"] == "tx1"


# -- production check (function_app registers sample_ok_to_dv outside it) ---------


def test_is_production_reads_the_environment_name():
    from onekey.config import is_production

    assert is_production({"ONEKEY_ENV": "prod"})
    assert is_production({"ONEKEY_ENV": " PRD "})  # the old name: not valid for from_env, still production
    assert not is_production({"ONEKEY_ENV": "uat"})
    assert not is_production({})


def test_is_production_prefers_the_base_url():
    from onekey.config import ENVIRONMENTS, is_production

    assert is_production({"ONEKEY_BASE_URL": ENVIRONMENTS["prod"] + "/", "ONEKEY_ENV": "uat"})
    assert not is_production({"ONEKEY_BASE_URL": ENVIRONMENTS["uat"], "ONEKEY_ENV": "prod"})


def test_from_env_reaches_production_as_prod(monkeypatch):
    from onekey.config import ENVIRONMENTS, OneKeyConfig
    from onekey.exceptions import OneKeyConfigError

    for name, value in {"ONEKEY_USERNAME": "user", "ONEKEY_PASSWORD": "secret",
                        "ONEKEY_CONNECTOR_ID": "494"}.items():
        monkeypatch.setenv(name, value)
    monkeypatch.delenv("ONEKEY_BASE_URL", raising=False)
    monkeypatch.setenv("ONEKEY_ENV", "prod")
    assert OneKeyConfig.from_env().base_url == ENVIRONMENTS["prod"]
    monkeypatch.setenv("ONEKEY_ENV", "prd")
    with pytest.raises(OneKeyConfigError, match="prod, qa, uat"):
        OneKeyConfig.from_env()
