"""Offline tests for the Dataverse client and the OneKey -> D365 mapping."""

import json

import pytest
import requests
from urllib3.exceptions import MaxRetryError, NewConnectionError

from dataverse import (
    DataverseAmbiguousWriteError,
    DataverseAuthError,
    DataverseClient,
    DataverseConfig,
    DataverseConfigError,
    DataverseHTTPError,
    TokenProvider,
    escape_odata,
)
from dataverse.client import DEFAULT_LOOKUP_CHUNK_SIZE, MAX_RETRY_AFTER_SECONDS
from integration import (
    ACCOUNT_KEY_FIELD,
    CONTACT_KEY_FIELD,
    hco_to_account,
    hcp_to_contact,
)


class FakeResponse:
    def __init__(self, status_code=200, payload=None, headers=None, text=""):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = text if payload is None else json.dumps(payload)
        self.content = self.text.encode()

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class FakeTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, **kwargs):
        self.calls.append({"method": method, "url": url, **kwargs})
        answer = self.responses.pop(0)
        if isinstance(answer, Exception):
            raise answer
        return answer

    def close(self):
        pass


class StubTokens:
    def __init__(self):
        self.calls = 0

    def token(self, force_refresh=False):
        self.calls += 1
        return "stub-token"


def make_client(responses):
    config = DataverseConfig(
        "https://paion-crm-test.api.crm4.dynamics.com", "tenant", "client", "secret"
    )
    return DataverseClient(config, token_provider=StubTokens(),
                           session=FakeTransport(responses))


# -- config ---------------------------------------------------------------


def test_scope_gets_the_default_suffix():
    config = DataverseConfig("https://x.crm4.dynamics.com", "t", "c", "s")
    assert config.scope == "https://x.crm4.dynamics.com/.default"


def test_a_pasted_web_api_url_is_reduced_to_the_org_root():
    config = DataverseConfig(
        "https://x.crm4.dynamics.com/api/data/v9.2/", "t", "c", "s"
    )
    assert config.environment_url == "https://x.crm4.dynamics.com"
    assert config.api_url == "https://x.crm4.dynamics.com/api/data/v9.2"


def test_a_bare_host_gets_https():
    assert DataverseConfig("x.crm4.dynamics.com", "t", "c", "s").environment_url == (
        "https://x.crm4.dynamics.com"
    )


@pytest.mark.parametrize(
    "missing", ["environment_url", "tenant_id", "client_id", "client_secret"]
)
def test_blank_settings_are_rejected(missing):
    kwargs = {
        "environment_url": "https://x",
        "tenant_id": "t",
        "client_id": "c",
        "client_secret": "s",
    }
    kwargs[missing] = ""
    with pytest.raises(DataverseConfigError):
        DataverseConfig(**kwargs)


def test_client_secret_is_not_in_the_repr():
    config = DataverseConfig("https://x", "t", "c", "super-secret")
    assert "super-secret" not in repr(config)


@pytest.mark.parametrize("url", [
    "http://x.crm4.dynamics.com", "https://user:secret@x.crm4.dynamics.com",
    "https://x.crm4.dynamics.com?secret=value", "https://x.crm4.dynamics.com/other",
    "https://x.crm4.dynamics.com:bad", "https://bad host.crm4.dynamics.com",
])
def test_config_rejects_unsafe_or_invalid_organization_urls(url):
    with pytest.raises(DataverseConfigError):
        DataverseConfig(url, "t", "c", "s")


@pytest.mark.parametrize("options", [
    {"max_retries": 0}, {"max_retries": 1.5}, {"bulk_batch_size": 0},
    {"timeout": 0}, {"timeout": float("nan")}, {"timeout": float("inf")},
    {"api_version": "v9.2/accounts"},
])
def test_config_rejects_values_that_cannot_produce_a_valid_request(options):
    with pytest.raises(DataverseConfigError):
        DataverseConfig("https://x.crm4.dynamics.com", "t", "c", "s", **options)


def test_dataverse_attempt_limit_can_be_configured_from_environment(monkeypatch):
    for name, value in {"URL": "https://x", "TENANT_ID": "t", "CLIENT_ID": "c",
                        "CLIENT_SECRET": "s", "MAX_RETRIES": "7"}.items():
        monkeypatch.setenv(f"DV_TEST_{name}", value)
    assert DataverseConfig.from_env(prefix="DV_TEST_").max_retries == 7


def test_invalid_numeric_environment_setting_raises_a_configuration_error(monkeypatch):
    for name, value in {"URL": "https://x", "TENANT_ID": "t", "CLIENT_ID": "c",
                        "CLIENT_SECRET": "s", "MAX_RETRIES": "invalid"}.items():
        monkeypatch.setenv(f"DV_TEST_{name}", value)
    with pytest.raises(DataverseConfigError, match="numeric environment settings"):
        DataverseConfig.from_env(prefix="DV_TEST_")


# -- auth -----------------------------------------------------------------


def test_token_request_requires_every_field():
    from dataverse.auth import get_access_token

    with pytest.raises(DataverseConfigError):
        get_access_token("", "c", "s", "scope")


def test_token_endpoint_url_tolerates_a_stray_slash(monkeypatch):
    import dataverse.auth as auth_module

    seen = {}

    def fake_post(url, **kwargs):
        seen["url"] = url
        return FakeResponse(200, {"access_token": "t", "expires_in": 3600})

    monkeypatch.setattr(auth_module.requests, "post", fake_post)
    # A pasted authority with a doubled slash must not produce '//tenant'.
    auth_module.get_access_token("/tenant-id/", "c", "s", "scope")
    assert seen["url"] == (
        "https://login.microsoftonline.com/tenant-id/oauth2/v2.0/token"
    )


def test_auth_error_carries_the_aad_code(monkeypatch):
    import dataverse.auth as auth_module

    monkeypatch.setattr(
        auth_module.requests, "post",
        lambda url, **kw: FakeResponse(401, {
            "error": "invalid_client",
            "error_description": "AADSTS7000215: Invalid client secret provided.",
            "error_codes": [7000215],
        }),
    )
    with pytest.raises(DataverseAuthError) as excinfo:
        auth_module.get_access_token("t", "c", "bad", "scope")
    assert excinfo.value.error == "invalid_client"
    assert 7000215 in excinfo.value.error_codes


def test_token_provider_caches_until_expiry(monkeypatch):
    import dataverse.auth as auth_module

    calls = []

    def fake_get(*args, **kwargs):
        calls.append(1)
        return {"access_token": f"token-{len(calls)}", "expires_in": 3600}

    monkeypatch.setattr(auth_module, "get_access_token", fake_get)
    provider = TokenProvider("t", "c", "s", "scope")

    assert provider.token() == "token-1"
    assert provider.token() == "token-1"  # cached
    assert len(calls) == 1

    assert provider.token(force_refresh=True) == "token-2"
    assert len(calls) == 2


def test_token_provider_renews_before_expiry(monkeypatch):
    import dataverse.auth as auth_module

    calls = []

    def fake_get(*args, **kwargs):
        calls.append(1)
        # Expires inside the renewal margin, so it must not be cached long.
        return {"access_token": f"token-{len(calls)}", "expires_in": 60}

    monkeypatch.setattr(auth_module, "get_access_token", fake_get)
    provider = TokenProvider("t", "c", "s", "scope")
    provider.token()
    monkeypatch.setattr(auth_module.time, "time", lambda: provider._expires_at + 1)
    provider.token()
    assert len(calls) == 2


def test_token_provider_does_not_cache_a_short_lived_token_after_it_expires(monkeypatch):
    import dataverse.auth as auth_module

    clock = [1000]
    calls = []
    monkeypatch.setattr(auth_module.time, "time", lambda: clock[0])

    def fake_get(*args, **kwargs):
        calls.append(1)
        return {"access_token": f"token-{len(calls)}", "expires_in": 30}

    monkeypatch.setattr(auth_module, "get_access_token", fake_get)
    provider = TokenProvider("t", "c", "s", "scope")
    assert provider.token() == "token-1"
    clock[0] += 31
    assert provider.token() == "token-2"


# -- client ---------------------------------------------------------------


def test_create_parses_the_id_from_the_odata_header():
    guid = "eb6f3fb0-07ad-f111-aaac-7ced8d73a9eb"
    client = make_client([
        FakeResponse(204, headers={"OData-EntityId": f"https://x/accounts({guid})"})
    ])
    assert client.create("accounts", {"name": "X"}) == guid


def test_create_raises_when_the_id_header_is_unusable():
    client = make_client([FakeResponse(204, headers={})])
    with pytest.raises(DataverseHTTPError, match="could not parse its id"):
        client.create("accounts", {"name": "X"})


def test_a_401_triggers_one_token_refresh_then_succeeds():
    guid = "eb6f3fb0-07ad-f111-aaac-7ced8d73a9eb"
    client = make_client([
        FakeResponse(401, text="expired"),
        FakeResponse(204, headers={"OData-EntityId": f"https://x/accounts({guid})"}),
    ])
    assert client.create("accounts", {"name": "X"}) == guid
    assert client.tokens.calls >= 2


def test_error_body_is_surfaced_in_the_message():
    client = make_client([
        FakeResponse(400, {"error": {"code": "0x80073002", "message": "Sql error 2601"}})
    ])
    with pytest.raises(DataverseHTTPError) as excinfo:
        client.create("accounts", {"name": "X"})
    assert "Sql error 2601" in str(excinfo.value)
    assert excinfo.value.code == "0x80073002"


def test_retrieve_multiple_follows_the_next_link():
    next_link = "https://paion-crm-test.api.crm4.dynamics.com/api/data/v9.2/accounts?$skiptoken=opaque%26cookie"
    client = make_client([
        FakeResponse(200, {"value": [{"id": 1}], "@odata.nextLink": next_link}),
        FakeResponse(200, {"value": [{"id": 2}]}),
    ])
    assert [r["id"] for r in client.retrieve_multiple("accounts")] == [1, 2]
    assert client._session.calls[1]["url"] == next_link
    assert client._session.calls[1]["params"] is None


@pytest.mark.parametrize("next_link", [
    "https://attacker.example/accounts", "http://paion-crm-test.api.crm4.dynamics.com/accounts",
    "https://paion-crm-test.api.crm4.dynamics.com:8443/accounts", "//attacker.example/accounts",
    "https://paion-crm-test.api.crm4.dynamics.com:0/accounts",
    "https://user@paion-crm-test.api.crm4.dynamics.com/accounts",
])
def test_paging_does_not_send_credentials_to_an_untrusted_url(next_link):
    client = make_client([
        FakeResponse(200, {"value": [{"id": 1}], "@odata.nextLink": next_link}),
    ])
    with pytest.raises(DataverseHTTPError, match="outside the configured origin"):
        client.retrieve_multiple("accounts")
    assert len(client._session.calls) == 1
    assert client.tokens.calls == 1


def test_redirects_do_not_forward_a_write_body():
    client = make_client([FakeResponse(307, headers={"Location": "https://attacker.example/"})])
    with pytest.raises(DataverseHTTPError) as error:
        client.create("accounts", {"name": "Personal data"})
    assert error.value.status_code == 307
    assert client._session.calls[0]["allow_redirects"] is False


def test_update_cannot_recreate_a_concurrently_deleted_record():
    client = make_client([FakeResponse(404, {"error": {"message": "deleted"}})])
    with pytest.raises(DataverseHTTPError):
        client.update("accounts", "existing-id", {"name": "X"})
    assert client._session.calls[0]["headers"]["If-Match"] == "*"


def test_retrieve_accepts_select_as_a_comma_separated_string():
    client = make_client([FakeResponse(200, {"accountid": "A", "name": "X"})])
    client.retrieve("accounts", "A", select="accountid,name")
    assert client._session.calls[0]["params"]["$select"] == "accountid,name"


def test_retrieve_multiple_honours_max_records():
    client = make_client([
        FakeResponse(200, {"value": [{"id": 1}, {"id": 2}, {"id": 3}]}),
    ])
    assert len(client.retrieve_multiple("accounts", max_records=2)) == 2


def test_upsert_updates_when_a_match_exists():
    client = make_client([
        FakeResponse(200, {"value": [{"accountid": "abc", "accountnumber": "W1"}]}),
        FakeResponse(204),
    ])
    record_id, created = client.upsert_by_field(
        "accounts", "accountnumber", "W1", {"name": "X"}
    )
    assert (record_id, created) == ("abc", False)
    assert client._session.calls[1]["method"] == "PATCH"


def test_upsert_creates_when_nothing_matches():
    guid = "eb6f3fb0-07ad-f111-aaac-7ced8d73a9eb"
    client = make_client([
        FakeResponse(200, {"value": []}),
        FakeResponse(204, headers={"OData-EntityId": f"https://x/accounts({guid})"}),
    ])
    record_id, created = client.upsert_by_field(
        "accounts", "accountnumber", "W1", {"name": "X"}
    )
    assert (record_id, created) == (guid, True)


def test_create_multiple_returns_ids_in_order():
    client = make_client([
        FakeResponse(200, {"Ids": ["id-1", "id-2"]}),
    ])
    ids = client.create_multiple(
        "accounts",
        [{"name": "A", "accountnumber": "1"}, {"name": "B", "accountnumber": "2"}],
    )
    assert ids == ["id-1", "id-2"]
    call = client._session.calls[0]
    assert call["method"] == "POST"
    assert call["url"].endswith("accounts/Microsoft.Dynamics.CRM.CreateMultiple")
    targets = call["json"]["Targets"]
    assert targets[0]["@odata.type"] == "Microsoft.Dynamics.CRM.account"
    assert "accountnumber" in targets[0]


def test_update_multiple_requires_primary_id():
    client = make_client([])
    with pytest.raises(DataverseHTTPError, match="accountid"):
        client.update_multiple("accounts", [{"name": "A"}])


def test_bulk_upsert_by_field_splits_create_and_update():
    client = make_client([
        # find_many lookup
        FakeResponse(200, {"value": [{"accountid": "existing", "accountnumber": "W1"}]}),
        # CreateMultiple (creates are sent before updates)
        FakeResponse(200, {"Ids": ["new-id"]}),
        # UpdateMultiple
        FakeResponse(204),
    ])
    results = client.bulk_upsert_by_field(
        "accounts",
        "accountnumber",
        [
            {"accountnumber": "W1", "name": "Updated"},
            {"accountnumber": "W2", "name": "Created"},
        ],
        batch_size=100,
        fallback_on_error=False,
    )
    assert results == [
        ("W1", "existing", False),
        ("W2", "new-id", True),
    ]
    methods = [c["method"] for c in client._session.calls]
    assert methods == ["GET", "POST", "POST"]
    assert client._session.calls[1]["url"].endswith(
        "accounts/Microsoft.Dynamics.CRM.CreateMultiple"
    )
    assert client._session.calls[2]["url"].endswith(
        "accounts/Microsoft.Dynamics.CRM.UpdateMultiple"
    )


def test_bulk_upsert_falls_back_to_single_on_create_failure():
    guid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    client = make_client([
        FakeResponse(200, {"value": []}),  # lookup
        FakeResponse(400, {"error": {"message": "plugin failed"}}),  # CreateMultiple
        # fallback upsert_by_field: find + create
        FakeResponse(200, {"value": []}),
        FakeResponse(204, headers={"OData-EntityId": f"https://x/accounts({guid})"}),
    ])
    results = client.bulk_upsert_by_field(
        "accounts",
        "accountnumber",
        [{"accountnumber": "W1", "name": "X"}],
        fallback_on_error=True,
    )
    assert results == [("W1", guid, True)]


def batch_response(*parts):
    """A $batch response; each part is (status, entity_id or None, error or None)."""
    boundary = "batchresponse_test"
    chunks = []
    for status, entity_id, error in parts:
        lines = [
            f"--{boundary}",
            "Content-Type: application/http",
            "Content-Transfer-Encoding: binary",
            "",
            f"HTTP/1.1 {status} {'No Content' if status < 400 else 'Bad Request'}",
            "OData-Version: 4.0",
        ]
        if entity_id:
            lines.append(f"OData-EntityId: https://x/api/data/v9.2/ts_affiliations({entity_id})")
        lines.append("")
        lines.append(json.dumps({"error": {"code": "0x1", "message": error}}) if error else "")
        chunks.append("\r\n".join(lines))
    text = "\r\n".join(chunks) + f"\r\n--{boundary}--\r\n"
    return FakeResponse(
        200,
        headers={"Content-Type": f"multipart/mixed; boundary={boundary}"},
        text=text,
    )


@pytest.fixture
def batch_creates(monkeypatch):
    """Send ts_affiliation creates as a $batch of POSTs: the path of a table
    that rejects CreateMultiple, as ts_affiliation did until 2026-10-18."""
    monkeypatch.setattr("dataverse.client._NO_CREATE_MULTIPLE", frozenset({"ts_affiliations"}))


ADDRESSES = [
    {"ts_externalid": "W1 - A1", "line1": "1 Main"},
    {"ts_externalid": "W2 - A2", "line1": "2 High"},
]
EXISTING_ADDRESSES = FakeResponse(200, {
    "value": [
        {"customeraddressid": "addr-1", "ts_externalid": "W1 - A1"},
        {"customeraddressid": "addr-2", "ts_externalid": "W2 - A2"},
    ]
})


def test_bulk_upsert_customeraddress_updates_in_one_batch():
    """customeraddress rejects UpdateMultiple, so its updates share one $batch."""
    client = make_client([
        EXISTING_ADDRESSES,
        batch_response((204, None, None), (204, None, None)),
    ])
    results = client.bulk_upsert_by_field(
        "customeraddresses", "ts_externalid", ADDRESSES,
        batch_size=100, fallback_on_error=False,
    )
    assert results == [
        ("W1 - A1", "addr-1", False),
        ("W2 - A2", "addr-2", False),
    ]
    calls = client._session.calls
    assert len(calls) == 2
    assert calls[1]["url"].endswith("/$batch")
    assert calls[1]["headers"]["Prefer"] == "odata.continue-on-error"
    body = calls[1]["data"].decode()
    assert "PATCH https://paion-crm-test.api.crm4.dynamics.com/api/data/v9.2/customeraddresses(addr-1) HTTP/1.1" in body
    assert "customeraddresses(addr-2)" in body
    assert "customeraddressid" not in body  # the id is in the URL, not the body
    assert body.count("If-Match: *") == 2
    assert not any("UpdateMultiple" in c["url"] for c in calls)


def test_batch_update_rejected_row_is_retried_alone():
    client = make_client([
        EXISTING_ADDRESSES,
        batch_response((204, None, None), (412, None, "Record is locked")),
        FakeResponse(204),  # PATCH addr-2 on its own
    ])
    results = client.bulk_upsert_by_field(
        "customeraddresses", "ts_externalid", ADDRESSES,
        batch_size=100, fallback_on_error=False,
    )
    assert [r[1] for r in results] == ["addr-1", "addr-2"]
    last = client._session.calls[-1]
    assert last["method"] == "PATCH"
    assert last["url"].endswith("customeraddresses(addr-2)")


def test_batch_update_row_that_fails_alone_raises():
    client = make_client([
        EXISTING_ADDRESSES,
        batch_response((204, None, None), (400, None, "bad value")),
        FakeResponse(400, {"error": {"message": "bad value"}}),
    ])
    with pytest.raises(DataverseHTTPError, match="bad value"):
        client.bulk_upsert_by_field(
            "customeraddresses", "ts_externalid", ADDRESSES,
            batch_size=100, fallback_on_error=False,
        )


@pytest.mark.usefixtures("batch_creates")
def test_bulk_upsert_ts_affiliation_creates_in_one_batch():
    """A table that rejects CreateMultiple sends its creates in one $batch."""
    g1 = "aaaaaaaa-bbbb-cccc-dddd-000000000001"
    g2 = "aaaaaaaa-bbbb-cccc-dddd-000000000002"
    client = make_client([
        FakeResponse(200, {"value": []}),  # lookup existing
        batch_response((204, g1, None), (204, g2, None)),
    ])
    results = client.bulk_upsert_by_field(
        "ts_affiliations",
        "ts_externalid",
        [
            {"ts_externalid": "W1 - W2 - AUT", "ts_name": "a"},
            {"ts_externalid": "W1 - W3 - AUT", "ts_name": "b"},
        ],
        batch_size=100,
        fallback_on_error=False,
    )
    assert results == [("W1 - W2 - AUT", g1, True), ("W1 - W3 - AUT", g2, True)]
    calls = client._session.calls
    assert len(calls) == 2
    body = calls[1]["data"].decode()
    assert body.count("POST https://paion-crm-test.api.crm4.dynamics.com/api/data/v9.2/ts_affiliations HTTP/1.1") == 2
    assert not any("CreateMultiple" in c["url"] for c in calls)


@pytest.mark.usefixtures("batch_creates")
def test_batch_create_with_unknown_outcome_creates_only_what_did_not_land():
    """A $batch of creates is not resent: keep the rows that landed, create the rest."""
    landed = "aaaaaaaa-bbbb-cccc-dddd-000000000001"
    created = "aaaaaaaa-bbbb-cccc-dddd-000000000002"
    client = make_client([
        FakeResponse(200, {"value": []}),  # lookup existing
        requests.exceptions.ReadTimeout("read timed out"),  # the $batch
        FakeResponse(200, {"value": [{"ts_externalid": "A", "ts_affiliationid": landed}]}),
        FakeResponse(200, {"value": []}),  # upsert_by_field find for B
        FakeResponse(204, headers={"OData-EntityId": f"https://x/ts_affiliations({created})"}),
    ])
    results = client.bulk_upsert_by_field(
        "ts_affiliations",
        "ts_externalid",
        [{"ts_externalid": "A"}, {"ts_externalid": "B"}],
        batch_size=100,
    )
    assert results == [("A", landed, True), ("B", created, True)]
    assert sum(c["url"].endswith("/$batch") for c in client._session.calls) == 1


# Dataverse ignores statecode on a POST, so an inactive affiliation would be
# created active (533 in prod on 2026-10-17); a PATCH sets the state.
G1 = "aaaaaaaa-bbbb-cccc-dddd-000000000001"
G2 = "aaaaaaaa-bbbb-cccc-dddd-000000000002"


def batch_parts(call):
    return call["data"].decode().split("--batch_")[1:-1]


@pytest.mark.usefixtures("batch_creates")
def test_batch_create_sets_a_non_default_state_with_a_patch():
    client = make_client([
        FakeResponse(200, {"value": []}),  # lookup existing
        batch_response((204, G1, None), (204, G2, None)),  # the creates
        batch_response((204, None, None)),  # the state of the inactive one
    ])
    results = client.bulk_upsert_by_field(
        "ts_affiliations",
        "ts_externalid",
        [
            {"ts_externalid": "A", "statecode": 1, "ts_substatus": 126870005},
            {"ts_externalid": "B", "statecode": 0},
        ],
        batch_size=100,
        fallback_on_error=False,
    )
    assert results == [("A", G1, True), ("B", G2, True)]
    calls = client._session.calls
    assert len(calls) == 3
    inactive, active = batch_parts(calls[1])
    assert "statecode" not in inactive and "126870005" in inactive
    assert '"statecode": 0' in active
    [state] = batch_parts(calls[2])
    assert f"PATCH https://paion-crm-test.api.crm4.dynamics.com/api/data/v9.2/ts_affiliations({G1}) HTTP/1.1" in state
    assert '{"statecode": 1}' in state and "If-Match: *" in state


@pytest.mark.usefixtures("batch_creates")
def test_batch_state_patch_that_fails_alone_is_the_record_error():
    errors = {}
    client = make_client([
        FakeResponse(200, {"value": []}),
        batch_response((204, G1, None), (204, G2, None)),
        batch_response((204, None, None), (412, None, "Record is locked")),
        FakeResponse(400, {"error": {"message": "bad state"}}),  # B's PATCH alone
    ])
    results = client.bulk_upsert_by_field(
        "ts_affiliations",
        "ts_externalid",
        [{"ts_externalid": "A", "statecode": 1}, {"ts_externalid": "B", "statecode": 1}],
        batch_size=100,
        fallback_on_error=False,
        record_errors=errors,
    )
    # B exists but stays active: reported as failed, so a later run updates it.
    assert results == [("A", G1, True), ("B", None, False)]
    assert list(errors) == ["B"] and "bad state" in str(errors["B"])
    last = client._session.calls[-1]
    assert last["method"] == "PATCH" and last["url"].endswith(f"ts_affiliations({G2})")
    assert last["json"] == {"statecode": 1}


@pytest.mark.usefixtures("batch_creates")
def test_batch_create_with_unknown_outcome_sets_the_state_of_both_halves():
    client = make_client([
        FakeResponse(200, {"value": []}),
        requests.exceptions.ReadTimeout("read timed out"),  # the $batch of creates
        FakeResponse(200, {"value": [{"ts_externalid": "A", "ts_affiliationid": G1}]}),
        batch_response((204, None, None)),  # A landed: its state
        FakeResponse(200, {"value": []}),  # upsert_by_field find for B
        FakeResponse(204, headers={"OData-EntityId": f"https://x/ts_affiliations({G2})"}),
        FakeResponse(204),  # B's state
    ])
    results = client.bulk_upsert_by_field(
        "ts_affiliations",
        "ts_externalid",
        [{"ts_externalid": "A", "statecode": 1}, {"ts_externalid": "B", "statecode": 1}],
        batch_size=100,
    )
    assert results == [("A", G1, True), ("B", G2, True)]
    calls = client._session.calls
    assert f"ts_affiliations({G1})" in calls[3]["data"].decode()
    assert calls[5]["method"] == "POST" and calls[5]["json"] == {"ts_externalid": "B"}
    assert calls[6]["method"] == "PATCH" and calls[6]["url"].endswith(f"ts_affiliations({G2})")
    assert calls[6]["json"] == {"statecode": 1}


def test_single_create_sets_a_non_default_state_with_a_patch():
    client = make_client([
        FakeResponse(204, headers={"OData-EntityId": f"https://x/ts_affiliations({G1})"}),
        FakeResponse(204),
    ])
    record_id = client.create(
        "ts_affiliations", {"ts_externalid": "A", "statecode": 1, "statuscode": 2}
    )
    assert record_id == G1
    post, patch = client._session.calls
    assert post["json"] == {"ts_externalid": "A"}
    assert patch["method"] == "PATCH" and patch["url"].endswith(f"ts_affiliations({G1})")
    assert patch["json"] == {"statecode": 1, "statuscode": 2}


def test_upsert_whose_create_landed_unseen_still_sets_the_state():
    client = make_client([
        FakeResponse(200, {"value": []}),  # find: not there yet
        requests.exceptions.ReadTimeout("read timed out"),  # the POST
        FakeResponse(200, {"value": [{"ts_affiliationid": G1}]}),  # it had landed
        FakeResponse(204),  # its state
    ])
    assert client.upsert_by_field(
        "ts_affiliations", "ts_externalid", "A", {"ts_externalid": "A", "statecode": 1}
    ) == (G1, True)
    patch = client._session.calls[-1]
    assert patch["method"] == "PATCH" and patch["url"].endswith(f"ts_affiliations({G1})")
    assert patch["json"] == {"statecode": 1}


def test_single_create_in_the_default_state_is_one_post():
    client = make_client([
        FakeResponse(204, headers={"OData-EntityId": f"https://x/ts_affiliations({G1})"}),
    ])
    assert client.create("ts_affiliations", {"ts_externalid": "A", "statecode": 0}) == G1
    [post] = client._session.calls
    assert post["json"] == {"ts_externalid": "A", "statecode": 0}


def test_create_multiple_keeps_the_state_in_the_create():
    """CreateMultiple applies statecode itself: no second write."""
    client = make_client([
        FakeResponse(200, {"value": []}),
        FakeResponse(200, {"Ids": [G1]}),
    ])
    results = client.bulk_upsert_by_field(
        "contacts", "ts_externalid", [{"ts_externalid": "C", "statecode": 1}],
        batch_size=100, fallback_on_error=False,
    )
    assert results == [("C", G1, True)]
    calls = client._session.calls
    assert len(calls) == 2
    [target] = calls[1]["json"]["Targets"]
    assert target["statecode"] == 1


def test_a_cleared_lookup_is_sent_as_its_navigation_property_on_update():
    """UpdateMultiple rejects ``X@odata.bind: null``; ``X: null`` clears the lookup."""
    client = make_client([
        FakeResponse(200, {"value": [{"ts_externalid": "C", "contactid": G1}]}),
        FakeResponse(204),  # UpdateMultiple
    ])
    client.bulk_upsert_by_field(
        "contacts", "ts_externalid", [{"ts_externalid": "C", "ts_Hospital@odata.bind": None}],
        batch_size=100, fallback_on_error=False,
    )
    [target] = client._session.calls[1]["json"]["Targets"]
    assert target["ts_Hospital"] is None and "ts_Hospital@odata.bind" not in target


def test_a_cleared_lookup_is_left_out_of_a_create():
    client = make_client([
        FakeResponse(200, {"value": []}),
        FakeResponse(200, {"Ids": [G1]}),  # CreateMultiple
    ])
    client.bulk_upsert_by_field(
        "contacts", "ts_externalid", [{"ts_externalid": "C", "ts_Hospital@odata.bind": None}],
        batch_size=100, fallback_on_error=False,
    )
    [target] = client._session.calls[1]["json"]["Targets"]
    assert not any(key.startswith("ts_Hospital") for key in target)


@pytest.mark.parametrize("stored, written", [(None, False), (G2, True)])
def test_skip_unchanged_compares_a_cleared_lookup(stored, written):
    """An empty lookup that should stay empty is no write; a set one is cleared."""
    client = make_client([
        FakeResponse(200, {"value": [{"ts_externalid": "C", "contactid": G1, "_ts_hospital_value": stored}]}),
        FakeResponse(204),
    ])
    skipped = set()
    client.bulk_upsert_by_field(
        "contacts", "ts_externalid", [{"ts_externalid": "C", "ts_Hospital@odata.bind": None}],
        batch_size=100, fallback_on_error=False, skip_unchanged=True, skipped=skipped,
    )
    assert "_ts_hospital_value" in str(client._session.calls[0])  # read for the comparison
    assert (len(client._session.calls) == 2) is written and (skipped == {"C"}) is (not written)


def test_customeraddress_creates_share_one_batch():
    """customeraddress has no CreateMultiple: its creates are POSTs in one $batch."""
    client = make_client([
        FakeResponse(200, {"value": []}),
        batch_response((204, G1, None), (204, G2, None)),
    ])
    results = client.bulk_upsert_by_field(
        "customeraddresses", "ts_externalid", ADDRESSES, batch_size=100, fallback_on_error=False,
    )
    assert results == [("W1 - A1", G1, True), ("W2 - A2", G2, True)]
    calls = client._session.calls
    assert len(calls) == 2 and calls[1]["url"].endswith("/$batch")
    body = calls[1]["data"].decode()
    assert body.count("POST https://paion-crm-test.api.crm4.dynamics.com/api/data/v9.2/customeraddresses HTTP/1.1") == 2
    assert not any("CreateMultiple" in c["url"] for c in calls)


def test_a_rejected_address_create_fails_alone():
    """An address Dataverse refuses (a choice value it lacks) is one failure; the rest land."""
    errors = {}
    refused = "The value 5 of 'addresstypecode' on record of type 'customeraddress' is outside the valid range."
    client = make_client([
        FakeResponse(200, {"value": []}),
        batch_response((204, G1, None), (400, None, refused)),
        FakeResponse(200, {"value": []}),  # W2 retried alone: find
        FakeResponse(400, {"error": {"message": refused}}),  # and create
    ])
    results = client.bulk_upsert_by_field(
        "customeraddresses", "ts_externalid", ADDRESSES, batch_size=100, record_errors=errors,
    )
    assert results == [("W1 - A1", G1, True), ("W2 - A2", None, False)]
    assert list(errors) == ["W2 - A2"] and "outside the valid range" in str(errors["W2 - A2"])


def test_ts_affiliation_creates_use_create_multiple_with_their_state():
    """One write each: CreateMultiple keeps an inactive affiliation inactive."""
    client = make_client([
        FakeResponse(200, {"value": []}),
        FakeResponse(200, {"Ids": [G1, G2]}),
    ])
    results = client.bulk_upsert_by_field(
        "ts_affiliations", "ts_externalid",
        [{"ts_externalid": "A", "statecode": 1}, {"ts_externalid": "B", "statecode": 0}],
        batch_size=100, fallback_on_error=False,
    )
    assert results == [("A", G1, True), ("B", G2, True)]
    calls = client._session.calls
    assert len(calls) == 2
    assert calls[1]["url"].endswith("ts_affiliations/Microsoft.Dynamics.CRM.CreateMultiple")
    a, b = calls[1]["json"]["Targets"]
    assert a["statecode"] == 1 and b["statecode"] == 0
    assert a["@odata.type"] == "Microsoft.Dynamics.CRM.ts_affiliation"


def test_failed_create_multiple_falls_back_to_single_creates_with_their_state():
    client = make_client([
        FakeResponse(200, {"value": []}),
        FakeResponse(400, {"error": {"message": "one bad record"}}),  # CreateMultiple
        FakeResponse(200, {"value": []}),  # A: find
        FakeResponse(204, headers={"OData-EntityId": f"https://x/ts_affiliations({G1})"}),
        FakeResponse(204),  # A: its state
        FakeResponse(200, {"value": []}),  # B: find
        FakeResponse(204, headers={"OData-EntityId": f"https://x/ts_affiliations({G2})"}),
    ])
    results = client.bulk_upsert_by_field(
        "ts_affiliations", "ts_externalid",
        [{"ts_externalid": "A", "statecode": 1}, {"ts_externalid": "B", "statecode": 0}],
        batch_size=100,
    )
    assert results == [("A", G1, True), ("B", G2, True)]
    patches = [c for c in client._session.calls if c["method"] == "PATCH"]
    assert len(patches) == 1
    assert patches[0]["url"].endswith(f"ts_affiliations({G1})") and patches[0]["json"] == {"statecode": 1}


@pytest.mark.parametrize("stored, sent", [(0, 1), (1, 0)])
def test_delta_update_of_an_affiliation_writes_its_new_state(stored, sent):
    """An activity that turns invalid (or valid again) updates statecode in its UpdateMultiple."""
    client = make_client([
        FakeResponse(200, {"value": [{"ts_externalid": "A", "ts_affiliationid": G1, "statecode": stored}]}),
        FakeResponse(204),  # UpdateMultiple
    ])
    results = client.bulk_upsert_by_field(
        "ts_affiliations", "ts_externalid", [{"ts_externalid": "A", "statecode": sent}],
        batch_size=100, fallback_on_error=False, skip_unchanged=True,
    )
    assert results == [("A", G1, False)]
    update = client._session.calls[1]
    assert update["url"].endswith("ts_affiliations/Microsoft.Dynamics.CRM.UpdateMultiple")
    [target] = update["json"]["Targets"]
    assert target["ts_affiliationid"] == G1 and target["statecode"] == sent


def test_affiliation_update_multiple_with_a_bad_record_updates_the_rest_one_by_one():
    """One bad record fails the whole UpdateMultiple; the others are still written."""
    errors = {}
    client = make_client([
        FakeResponse(200, {"value": [
            {"ts_externalid": "A", "ts_affiliationid": G1},
            {"ts_externalid": "B", "ts_affiliationid": G2},
        ]}),
        FakeResponse(400, {"error": {"message": "ts_name is too long"}}),  # UpdateMultiple
        FakeResponse(204),  # A alone
        FakeResponse(400, {"error": {"message": "ts_name is too long"}}),  # B alone
    ])
    results = client.bulk_upsert_by_field(
        "ts_affiliations", "ts_externalid",
        [{"ts_externalid": "A", "ts_name": "a"}, {"ts_externalid": "B", "ts_name": "b" * 999}],
        batch_size=100, record_errors=errors,
    )
    assert results == [("A", G1, False), ("B", None, False)]
    assert list(errors) == ["B"]
    first = client._session.calls[2]
    assert first["method"] == "PATCH" and first["url"].endswith(f"ts_affiliations({G1})")
    assert first["json"] == {"ts_externalid": "A", "ts_name": "a"}


def test_delta_update_with_the_state_already_set_writes_nothing():
    skipped = set()
    client = make_client([
        FakeResponse(200, {"value": [{"ts_externalid": "A", "ts_affiliationid": G1, "statecode": 1}]}),
    ])
    results = client.bulk_upsert_by_field(
        "ts_affiliations", "ts_externalid", [{"ts_externalid": "A", "statecode": 1}],
        batch_size=100, fallback_on_error=False, skip_unchanged=True, skipped=skipped,
    )
    assert results == [("A", G1, False)] and skipped == {"A"}
    assert len(client._session.calls) == 1



def test_batch_delete_sends_no_body():
    client = make_client([batch_response((204, None, None), (204, None, None))])
    results = client.batch_write(
        [("DELETE", "customeraddresses(addr-3)", None), ("PATCH", "customeraddresses(addr-1)", {"ts_externalid": "W1 - A1"})],
        idempotent=True,
    )
    assert [status for status, _, _ in results] == [204, 204]
    body = client._session.calls[0]["data"].decode()
    delete, patch = body.split("--batch_")[1:3]
    assert "DELETE https://paion-crm-test.api.crm4.dynamics.com/api/data/v9.2/customeraddresses(addr-3) HTTP/1.1" in delete
    assert "Content-Type: application/json" not in delete and "null" not in delete
    assert '{"ts_externalid": "W1 - A1"}' in patch and "If-Match: *" in patch

def test_odata_quotes_are_escaped():
    # O'Brien would otherwise terminate the filter literal early.
    assert escape_odata("O'Brien") == "O''Brien"


def test_batch_rejects_more_than_the_dataverse_limit_before_sending():
    client = make_client([])
    with pytest.raises(DataverseHTTPError, match="at most 1000"):
        client.batch_write([("PATCH", "accounts(A)", {"name": "X"})] * 1001, idempotent=True)
    assert client._session.calls == []


def test_batch_upserts_split_at_the_dataverse_batch_limit():
    records = [{"ts_externalid": str(index), "line1": "Updated"} for index in range(1001)]
    existing = [{"ts_externalid": str(index), "customeraddressid": str(index)} for index in range(1001)]
    step = DEFAULT_LOOKUP_CHUNK_SIZE
    client = make_client([
        *[FakeResponse(200, {"value": existing[start:start + step]}) for start in range(0, 1001, step)],
        batch_response(*[(204, None, None)] * 1000),
        batch_response((204, None, None)),
    ])
    results = client.bulk_upsert_by_field(
        "customeraddresses", "ts_externalid", records, batch_size=1500,
        fallback_on_error=False,
    )
    assert len(results) == 1001
    batches = [call for call in client._session.calls if call["url"].endswith("/$batch")]
    assert [call["data"].count(b"PATCH ") for call in batches] == [1000, 1]


# -- mapping --------------------------------------------------------------

HCO = {
    "workplaceEid": "WDEH02963729",
    "officialName": "Kliniken Hartenstein - Klinik Wildetal",
    "usualName": "Kliniken Hartenstein",
    "typeLabel": "Rehabilitation Hospital",
    "specialtyLabel": "Miscellaneous",
    "codBase": "WDE",
    "address_street": "Günter-Hartenstein-Str. 8",
    "address_postalCode": "34537",
    "address_city": "Bad Wildungen",
    "address_country": "DE",
}

HCP = {
    "individualEid": "WDER00117070",
    "activityEid": "WDER0011707001",
    "firstName": "Ilenia",
    "lastName": "Mule",
    "genderCode": "F",
    "specialtyLabel": "Pharmaceutical Commercial Assistant",
    "typeLabel": "Pharmacist Assistant",
    "workplaceName": "Apotheke am Markt",
    "codBase": "WDE",
    "address_city": "Bad Wildungen",
}


def test_hco_maps_the_onekey_id_into_account_key():
    assert hco_to_account(HCO)[ACCOUNT_KEY_FIELD] == "WDEH02963729"
    assert ACCOUNT_KEY_FIELD  # from entity_mapping.csv


def test_hcp_maps_the_onekey_id_into_contact_key():
    assert hcp_to_contact(HCP)[CONTACT_KEY_FIELD] == "WDER00117070"


def test_gender_is_translated_to_the_dataverse_picklist():
    assert hcp_to_contact(HCP)["gendercode"] == 2          # F -> Female
    assert hcp_to_contact({**HCP, "genderCode": "M"})["gendercode"] == 1


def test_unknown_gender_is_omitted_rather_than_guessed():
    assert "gendercode" not in hcp_to_contact({**HCP, "genderCode": None})


def test_the_parent_account_is_bound_by_url_not_raw_guid():
    record = hcp_to_contact(HCP, account_id="abc-123")
    assert record["parentcustomerid_account@odata.bind"] == "/accounts(abc-123)"


def test_no_parent_binding_without_an_account():
    assert not any("odata.bind" in k for k in hcp_to_contact(HCP))


def test_values_are_trimmed_to_the_dataverse_column_limits():
    long_name = "A" * 500
    assert len(hco_to_account({**HCO, "officialName": long_name})["name"]) == 160
    assert len(hcp_to_contact({**HCP, "lastName": long_name})["lastname"]) == 50


def test_addresses_are_not_loaded_by_default():
    # Only HCP/HCO identity data is written; the org rejects address writes.
    account = hco_to_account(HCO)
    contact = hcp_to_contact(HCP)
    assert not [k for k in account if k.startswith("address1_")]
    assert not [k for k in contact if k.startswith("address1_")]
    assert account["name"] and contact["lastname"]


def test_addresses_can_be_switched_back_on():
    # Retained so it works once the customeraddress key is removed.
    account = hco_to_account(HCO, include_address=True)
    assert account["address1_city"] == "Bad Wildungen"
    assert account["address1_postalcode"] == "34537"
    assert hcp_to_contact(HCP, include_address=True)["address1_city"] == "Bad Wildungen"


def test_empty_values_are_dropped_so_updates_do_not_blank_fields():
    record = hco_to_account({"workplaceEid": "W1", "officialName": "X",
                             "address_city": None, "address_street": ""},
                            include_address=True)
    assert "address1_city" not in record
    assert "address1_line1" not in record


def test_a_contact_without_a_surname_still_gets_one():
    # lastname is mandatory on contact, so a create must not fail on it.
    record = hcp_to_contact({"individualEid": "WDER1", "firstName": "X"})
    assert record["lastname"]


def test_account_falls_back_to_the_usual_name():
    record = hco_to_account({**HCO, "officialName": None})
    assert record["name"] == "Kliniken Hartenstein"


# -- writes that may or may not have been applied ---------------------------

GUID_1 = "11111111-1111-1111-1111-111111111111"
GUID_2 = "22222222-2222-2222-2222-222222222222"


def created(guid, entity_set="accounts"):
    return FakeResponse(204, headers={"OData-EntityId": f"https://x/{entity_set}({guid})"})


@pytest.fixture
def no_sleep(monkeypatch):
    monkeypatch.setattr("dataverse.client.time.sleep", lambda _: None)


def test_create_is_not_repeated_after_a_read_timeout(no_sleep):
    client = make_client([requests.exceptions.ReadTimeout("read timed out")])
    with pytest.raises(DataverseAmbiguousWriteError):
        client.create("accounts", {"name": "X"})
    assert len(client._session.calls) == 1


def test_create_after_a_gateway_timeout_is_not_repeated(no_sleep):
    client = make_client([FakeResponse(504, {"error": {"message": "Gateway Timeout"}})])
    with pytest.raises(DataverseAmbiguousWriteError) as raised:
        client.create("accounts", {"name": "X"})
    assert raised.value.status_code == 504
    assert len(client._session.calls) == 1


@pytest.mark.parametrize(
    "never_sent",
    [
        requests.exceptions.ConnectTimeout("connect timed out"),
        requests.exceptions.ConnectionError(
            MaxRetryError(None, "https://x", NewConnectionError(None, "refused"))
        ),
    ],
)
def test_create_is_retried_when_it_never_reached_dataverse(no_sleep, never_sent):
    client = make_client([never_sent, created(GUID_1)])
    assert client.create("accounts", {"name": "X"}) == GUID_1
    assert len(client._session.calls) == 2


def test_create_is_retried_when_throttled(no_sleep):
    client = make_client([FakeResponse(429, headers={"Retry-After": "1"}), created(GUID_1)])
    assert client.create("accounts", {"name": "X"}) == GUID_1


def test_dataverse_waits_for_the_full_retry_after(monkeypatch):
    delays = []
    monkeypatch.setattr("dataverse.client.time.sleep", delays.append)
    client = make_client([FakeResponse(429, headers={"Retry-After": "300"}), created(GUID_1)])
    assert client.create("accounts", {"name": "X"}) == GUID_1
    assert sum(delays) == 300


@pytest.mark.parametrize("status", [429, 503])
def test_a_longer_retry_after_is_cut_to_the_cap(monkeypatch, status):
    # Two capped waits plus timeouts stay inside the 20-minute busy check.
    delays = []
    monkeypatch.setattr("dataverse.client.time.sleep", delays.append)
    client = make_client([
        FakeResponse(status, headers={"Retry-After": "1265"}), created(GUID_1),
    ])
    assert client.create("accounts", {"name": "X"}) == GUID_1
    assert delays == [MAX_RETRY_AFTER_SECONDS]


@pytest.mark.parametrize(
    "first",
    [
        FakeResponse(429, headers={"Retry-After": "120"}),
        requests.exceptions.ConnectTimeout("connect timed out"),
    ],
)
def test_retry_waits_do_not_run_the_heartbeat_hook(monkeypatch, first):
    # The heartbeat is a Dataverse write: run during a throttling wait, it is
    # throttled too, and its own waits stretch the request's.
    events = []
    monkeypatch.setattr("dataverse.client.time.sleep", lambda s: events.append("sleep"))
    client = make_client([first, created(GUID_1)])
    client.after_request = lambda: events.append("hook")
    assert client.create("accounts", {"name": "X"}) == GUID_1
    assert events == ["sleep", "hook"]


def test_a_throttled_request_is_not_stretched_by_heartbeat_writes(monkeypatch):
    from integration.job_trace import HEARTBEAT_EVERY_SECONDS, JobTracer

    clock = [1000.0]
    monkeypatch.setattr("dataverse.client.time.sleep", lambda s: clock.__setitem__(0, clock[0] + s))
    monkeypatch.setattr("integration.job_trace.time.monotonic", lambda: clock[0])
    throttled = FakeResponse(429, {"error": {"message": "throttled"}}, headers={"Retry-After": "300"})
    client = make_client([throttled] * 10)
    tracer = JobTracer(client)
    tracer.enabled, tracer.job_guid, tracer.job_id = True, "job-guid", "job-1"
    tracer._last_beat = clock[0] - HEARTBEAT_EVERY_SECONDS  # a heartbeat is due
    client.after_request = tracer.beat

    with pytest.raises(DataverseHTTPError):
        client.request("GET", "accounts")
    # Three attempts and two waits; then one heartbeat, sent once and dropped.
    assert clock[0] - 1000.0 == 600
    assert len(client._session.calls) == 4
    assert client._session.calls[-1]["method"] == "PATCH"


@pytest.mark.parametrize("retry_after", ["-1", "NaN", "Infinity", "not a duration"])
def test_invalid_retry_after_uses_backoff_without_breaking_the_request(monkeypatch, retry_after):
    delays = []
    monkeypatch.setattr("dataverse.client.time.sleep", delays.append)
    client = make_client([FakeResponse(429, headers={"Retry-After": retry_after}), created(GUID_1)])
    assert client.create("accounts", {"name": "X"}) == GUID_1
    assert len(delays) == 1 and 1 <= delays[0] <= 1.5


def test_retry_after_http_date_is_understood(monkeypatch):
    from datetime import datetime, timedelta, timezone
    from email.utils import format_datetime

    delays = []
    monkeypatch.setattr("dataverse.client.time.sleep", delays.append)
    future = format_datetime(datetime.now(timezone.utc) + timedelta(seconds=240), usegmt=True)
    client = make_client([FakeResponse(503, headers={"Retry-After": future}), FakeResponse(200, {})])
    client.whoami()
    assert len(delays) == 1 and 238 < delays[0] <= 240


def test_lowercase_post_is_not_repeated_after_a_read_timeout(no_sleep):
    client = make_client([requests.exceptions.ReadTimeout("read timed out")])
    with pytest.raises(DataverseAmbiguousWriteError):
        client.request("post", "accounts", json={"name": "X"})
    assert len(client._session.calls) == 1


def test_update_and_update_multiple_are_retried_after_a_read_timeout(no_sleep):
    client = make_client(
        [
            requests.exceptions.ReadTimeout("read timed out"),
            FakeResponse(204),
            requests.exceptions.ReadTimeout("read timed out"),
            FakeResponse(204),
        ]
    )
    client.update("accounts", GUID_1, {"name": "X"})
    client.update_multiple("accounts", [{"accountid": GUID_1, "name": "X"}])
    assert len(client._session.calls) == 4


def test_upsert_by_field_patches_the_primary_key_not_another_id_field():
    client = make_client(
        [
            # ts_externalid also ends in "id" and comes first in the row.
            FakeResponse(200, {"value": [
                {"@odata.etag": "W/1", "ts_externalid": "WUKH1", "accountid": GUID_1}
            ]}),
            FakeResponse(204),
        ]
    )
    assert client.upsert_by_field("accounts", "ts_externalid", "WUKH1", {"name": "X"}) == (
        GUID_1,
        False,
    )
    assert client._session.calls[1]["url"].endswith(f"accounts({GUID_1})")


def test_upsert_by_field_keeps_a_create_that_landed(no_sleep):
    client = make_client(
        [
            FakeResponse(200, {"value": []}),
            requests.exceptions.ReadTimeout("read timed out"),
            FakeResponse(200, {"value": [{"accountid": GUID_1}]}),
        ]
    )
    assert client.upsert_by_field("accounts", "ts_externalid", "WUKH1", {"name": "X"}) == (
        GUID_1,
        True,
    )
    assert [call["method"] for call in client._session.calls] == ["GET", "POST", "GET"]


def test_upsert_by_field_creates_again_when_nothing_landed(no_sleep):
    client = make_client(
        [
            FakeResponse(200, {"value": []}),
            requests.exceptions.ReadTimeout("read timed out"),
            FakeResponse(200, {"value": []}),
            created(GUID_1),
        ]
    )
    assert client.upsert_by_field("accounts", "ts_externalid", "WUKH1", {"name": "X"}) == (
        GUID_1,
        True,
    )


def test_bulk_upsert_checks_an_uncertain_create_multiple(no_sleep):
    client = make_client(
        [
            FakeResponse(200, {"value": []}),  # lookup: both new
            requests.exceptions.ReadTimeout("read timed out"),  # CreateMultiple
            # check: W1 landed, W2 did not
            FakeResponse(200, {"value": [{"accountnumber": "W1", "accountid": GUID_1}]}),
            FakeResponse(200, {"value": []}),  # upsert_by_field(W2): find
            created(GUID_2),  # upsert_by_field(W2): create
        ]
    )
    results = client.bulk_upsert_by_field(
        "accounts",
        "accountnumber",
        [{"accountnumber": "W1"}, {"accountnumber": "W2"}],
    )
    assert results == [("W1", GUID_1, True), ("W2", GUID_2, True)]
    creates = [call for call in client._session.calls if call["method"] == "POST"]
    assert len(creates) == 2  # the CreateMultiple, then W2 alone


def test_bulk_upsert_writes_a_repeated_key_once():
    client = make_client(
        [
            FakeResponse(200, {"value": []}),
            FakeResponse(200, {"Ids": [GUID_1]}),
        ]
    )
    results = client.bulk_upsert_by_field(
        "accounts",
        "accountnumber",
        [{"accountnumber": "W1", "name": "Old"}, {"accountnumber": "W1", "name": "New"}],
    )
    assert results == [("W1", GUID_1, True), ("W1", GUID_1, False)]
    [target] = client._session.calls[1]["json"]["Targets"]
    assert target["name"] == "New"


# -- record_errors: one rejected record does not fail the rest ---------------

GUID_A = "aaaaaaaa-bbbb-cccc-dddd-00000000000a"
NEW_ACCOUNTS = [{"accountnumber": "A", "name": "a"}, {"accountnumber": "B", "name": "b"}]


def _create_one_by_one(second_create):
    """CreateMultiple rejected, then A created alone and B answered by second_create."""
    return [
        FakeResponse(200, {"value": []}),  # lookup existing
        FakeResponse(400, {"error": {"message": "bad batch"}}),  # CreateMultiple
        FakeResponse(200, {"value": []}),  # find A
        FakeResponse(204, headers={"OData-EntityId": f"https://x/accounts({GUID_A})"}),
        FakeResponse(200, {"value": []}),  # find B
        *second_create,
    ]


def test_record_errors_keeps_a_rejected_record_out_of_the_way():
    client = make_client(
        _create_one_by_one([FakeResponse(400, {"error": {"message": "bad value"}})])
    )
    rejected = {}
    results = client.bulk_upsert_by_field(
        "accounts", "accountnumber", NEW_ACCOUNTS, record_errors=rejected
    )
    assert results == [("A", GUID_A, True), ("B", None, False)]
    assert list(rejected) == ["B"]
    assert rejected["B"].status_code == 400


def test_without_record_errors_a_rejected_record_still_raises():
    client = make_client(
        _create_one_by_one([FakeResponse(400, {"error": {"message": "bad value"}})])
    )
    with pytest.raises(DataverseHTTPError, match="bad value"):
        client.bulk_upsert_by_field("accounts", "accountnumber", NEW_ACCOUNTS)


def test_record_errors_raises_when_every_record_of_the_batch_is_rejected():
    # The same fault on every record is a mapping or schema problem.
    client = make_client([
        FakeResponse(200, {"value": []}),
        FakeResponse(400, {"error": {"message": "bad batch"}}),
        FakeResponse(200, {"value": []}),
        FakeResponse(400, {"error": {"message": "no such column"}}),
        FakeResponse(200, {"value": []}),
        FakeResponse(400, {"error": {"message": "no such column"}}),
    ])
    with pytest.raises(DataverseHTTPError, match="no such column"):
        client.bulk_upsert_by_field(
            "accounts", "accountnumber", NEW_ACCOUNTS, record_errors={}
        )


def test_record_errors_still_raises_a_server_error(monkeypatch):
    monkeypatch.setattr("dataverse.client.time.sleep", lambda _: None)
    client = make_client(_create_one_by_one([FakeResponse(500, {"error": {"message": "down"}})] * 3))
    with pytest.raises(DataverseHTTPError, match="down"):
        client.bulk_upsert_by_field(
            "accounts", "accountnumber", NEW_ACCOUNTS, record_errors={}
        )


GUID_3 = "33333333-3333-3333-3333-333333333333"


def _existing_accounts(existing):
    if not existing:
        return []
    return [{"accountnumber": "A", "accountid": GUID_1},
            {"accountnumber": "B", "accountid": GUID_2}]


@pytest.mark.parametrize("status", [401, 403])
@pytest.mark.parametrize("existing", [False, True])
def test_bulk_auth_failures_are_not_retried_record_by_record(no_sleep, status, existing):
    # A 401 is retried with a fresh token inside the request; 403 is not.
    attempts = 1 if status == 403 else 3
    client = make_client([
        FakeResponse(200, {"value": _existing_accounts(existing)}),
        *[FakeResponse(status, {"error": {"message": "denied"}}) for _ in range(attempts)],
    ])
    with pytest.raises(DataverseHTTPError, match="denied"):
        client.bulk_upsert_by_field("accounts", "accountnumber", NEW_ACCOUNTS)
    assert len(client._session.calls) == 1 + attempts
    assert all("Multiple" in call["url"] for call in client._session.calls[1:])


@pytest.mark.parametrize("status", [429, 500, 503])
def test_a_failed_bulk_create_is_written_record_by_record(no_sleep, status):
    # A bulk 5xx is often the batch itself (a SQL or plug-in timeout).
    client = make_client([
        FakeResponse(200, {"value": []}),
        *[FakeResponse(status, {"error": {"message": "busy"}}) for _ in range(3)],
        FakeResponse(200, {"value": []}), created(GUID_1),  # A alone
        FakeResponse(200, {"value": []}), created(GUID_2),  # B alone
    ])
    results = client.bulk_upsert_by_field("accounts", "accountnumber", NEW_ACCOUNTS)
    assert results == [("A", GUID_1, True), ("B", GUID_2, True)]


@pytest.mark.parametrize("status", [429, 500, 503])
@pytest.mark.parametrize("existing", [False, True])
def test_a_lasting_service_failure_stops_at_the_first_single_write(no_sleep, status, existing):
    client = make_client([
        FakeResponse(200, {"value": _existing_accounts(existing)}),
        *[FakeResponse(status, {"error": {"message": "service failure"}}) for _ in range(6)],
    ])
    with pytest.raises(DataverseHTTPError, match="service failure"):
        client.bulk_upsert_by_field("accounts", "accountnumber", NEW_ACCOUNTS)
    # The bulk call's three attempts, then one single write's: not one per record.
    assert len(client._session.calls) == 1 + 3 + 3


def _existing_addresses(existing, count=2):
    if not existing:
        return []
    guids = [GUID_1, GUID_2, GUID_3]
    return [{"ts_externalid": key, "customeraddressid": guids[n]}
            for n, key in enumerate("ABC"[:count])]


@pytest.mark.parametrize("status", [429, 500, 503])
@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.usefixtures("batch_creates")
def test_a_service_failure_inside_a_batch_keeps_the_rest_and_retries_alone(
    no_sleep, status, existing
):
    entity_set = "customeraddresses" if existing else "ts_affiliations"
    alone = (
        [FakeResponse(204)]  # PATCH B
        if existing
        else [FakeResponse(200, {"value": []}), created(GUID_2, entity_set)]  # find B, create B
    )
    client = make_client([
        FakeResponse(200, {"value": _existing_addresses(existing)}),
        batch_response((204, GUID_1, None), (status, None, "service failure")),
        *alone,
    ])
    results = client.bulk_upsert_by_field(
        entity_set, "ts_externalid", [{"ts_externalid": "A"}, {"ts_externalid": "B"}],
    )
    # A's result from the batch is kept; only B is sent again.
    assert [result[1] for result in results] == [GUID_1, GUID_2]
    assert len(client._session.calls) == 2 + len(alone)


@pytest.mark.parametrize("status", [403, 429, 500, 503])
@pytest.mark.parametrize("existing", [False, True])
@pytest.mark.usefixtures("batch_creates")
def test_a_lasting_service_failure_inside_a_batch_stops_at_one_single_write(
    no_sleep, status, existing
):
    entity_set = "customeraddresses" if existing else "ts_affiliations"
    attempts = 1 if status == 403 else 3
    client = make_client([
        FakeResponse(200, {"value": _existing_addresses(existing, count=3)}),
        batch_response(
            (204, GUID_1, None),
            (status, None, "service failure"),
            (status, None, "service failure"),
        ),
        *[FakeResponse(status, {"error": {"message": "service failure"}}) for _ in range(attempts)],
    ])
    with pytest.raises(DataverseHTTPError, match="service failure"):
        client.bulk_upsert_by_field(
            entity_set,
            "ts_externalid",
            [{"ts_externalid": "A"}, {"ts_externalid": "B"}, {"ts_externalid": "C"}],
        )
    # C is never sent alone: the first single write stops the fallback.
    assert len(client._session.calls) == 2 + attempts


def test_find_many_by_field_sends_no_top():
    # With $top, a value stored twice would push another value off the page.
    client = make_client([FakeResponse(200, {"value": []})])
    client.find_many_by_field("accounts", "accountnumber", ["A", "B"])
    assert "$top" not in client._session.calls[0]["params"]


# -- skip unchanged --------------------------------------------------------

TYPE_GUID = "aaaaaaaa-0000-0000-0000-000000000001"


def account(key, name, type_bind=f"/ts_hcotype_refs({TYPE_GUID})"):
    return {"ts_externalid": key, "name": name, "ts_Type_ref@odata.bind": type_bind}


def stored(key, record_id, name):
    return {"ts_externalid": key, "accountid": record_id, "name": name,
            "_ts_type_ref_value": TYPE_GUID}


def test_skip_unchanged_leaves_records_that_already_match():
    client = make_client([
        FakeResponse(200, {"value": [stored("W1", "acc-1", "Same"), stored("W2", "acc-2", "Old")]}),
        FakeResponse(204),  # UpdateMultiple
    ])
    skipped = set()
    results = client.bulk_upsert_by_field(
        "accounts", "ts_externalid", [account("W1", "Same"), account("W2", "New")],
        fallback_on_error=False, skip_unchanged=True, skipped=skipped,
    )
    assert results == [("W1", "acc-1", False), ("W2", "acc-2", False)]
    assert skipped == {"W1"}
    lookup, update = client._session.calls
    assert lookup["params"]["$select"] == "ts_externalid,accountid,name,_ts_type_ref_value"
    targets = update["json"]["Targets"]
    assert [t["accountid"] for t in targets] == ["acc-2"]


def test_a_bind_by_alternate_key_is_always_written():
    client = make_client([
        FakeResponse(200, {"value": [stored("W1", "acc-1", "Same")]}),
        FakeResponse(204),
    ])
    skipped = set()
    client.bulk_upsert_by_field(
        "accounts", "ts_externalid",
        [account("W1", "Same", type_bind="/ts_hcotype_refs(ts_externalid='WUK.TET.HOS')")],
        fallback_on_error=False, skip_unchanged=True, skipped=skipped,
    )
    assert skipped == set()
    lookup, update = client._session.calls
    assert lookup["params"]["$select"] == "ts_externalid,accountid"
    assert len(update["json"]["Targets"]) == 1


def test_an_unreadable_column_turns_the_comparison_off():
    client = make_client([
        FakeResponse(400, {"error": {"message": "Could not find a property named 'ts_bogus'"}}),
        FakeResponse(200, {"value": [{"ts_externalid": "W1", "accountid": "acc-1"}]}),
        FakeResponse(204),
    ])
    skipped = set()
    results = client.bulk_upsert_by_field(
        "accounts", "ts_externalid", [{"ts_externalid": "W1", "ts_bogus": "x"}],
        fallback_on_error=False, skip_unchanged=True, skipped=skipped,
    )
    assert results == [("W1", "acc-1", False)]
    assert skipped == set()
    assert client._session.calls[1]["params"]["$select"] == "ts_externalid,accountid"
    assert "UpdateMultiple" in client._session.calls[2]["url"]


def test_a_customer_lookup_compares_by_its_column():
    parent = "BBBBBBBB-0000-0000-0000-000000000002"
    client = make_client([
        FakeResponse(200, {"value": [{
            "ts_externalid": "I1", "contactid": "con-1", "lastname": "Doe",
            "_parentcustomerid_value": parent.lower(),
        }]}),
    ])
    skipped = set()
    client.bulk_upsert_by_field(
        "contacts", "ts_externalid",
        [{"ts_externalid": "I1", "lastname": "Doe",
          "parentcustomerid_account@odata.bind": f"/accounts({parent})"}],
        fallback_on_error=False, skip_unchanged=True, skipped=skipped,
    )
    assert skipped == {"I1"}
    assert "_parentcustomerid_value" in client._session.calls[0]["params"]["$select"]
    assert len(client._session.calls) == 1  # nothing written


@pytest.mark.parametrize("target", ["account", "contact"])
def test_an_affiliation_from_lookup_compares_by_ts_fromid(target):
    # ts_fromid is one lookup to accounts or contacts. Read back as
    # _ts_fromid_<target>_value, a column that does not exist, Dataverse
    # answered 400 and no affiliation was ever skipped.
    source = "AAAAAAAA-0000-0000-0000-000000000001"
    workplace = "BBBBBBBB-0000-0000-0000-000000000002"
    client = make_client([
        FakeResponse(200, {"value": [{
            "ts_externalid": "A1", "ts_affiliationid": "aff-1",
            "_ts_fromid_value": source.lower(), "_ts_toid_value": workplace.lower(),
        }]}),
    ])
    skipped = set()
    client.bulk_upsert_by_field(
        "ts_affiliations", "ts_externalid",
        [{"ts_externalid": "A1",
          f"ts_FromID_{target}@odata.bind": f"/{target}s({source})",
          "ts_ToID@odata.bind": f"/accounts({workplace})"}],
        fallback_on_error=False, skip_unchanged=True, skipped=skipped,
    )
    assert skipped == {"A1"}
    select = client._session.calls[0]["params"]["$select"].split(",")
    assert "_ts_fromid_value" in select
    assert len(client._session.calls) == 1  # nothing written


def test_lookups_send_100_values_per_request():
    client = make_client([FakeResponse(200, {"value": []}) for _ in range(3)])
    client.find_many_by_field("accounts", "ts_externalid", [f"W{n}" for n in range(250)])
    filters = [call["params"]["$filter"] for call in client._session.calls]
    assert [f.count(" eq ") for f in filters] == [100, 100, 50]


def test_long_lookup_values_make_smaller_requests():
    long_key = "X" * 200
    values = [f"{long_key}{n:03d}" for n in range(100)]
    client = make_client([FakeResponse(200, {"value": []}) for _ in range(10)])
    client.find_many_by_field("ts_affiliations", "ts_externalid", values)
    filters = [call["params"]["$filter"] for call in client._session.calls]
    assert len(filters) > 1
    assert all(len(f) <= 6000 for f in filters)
    assert sum(f.count(" eq ") for f in filters) == 100


def test_skip_unchanged_is_off_unless_asked():
    client = make_client([
        FakeResponse(200, {"value": [stored("W1", "acc-1", "Same")]}),
        FakeResponse(204),
    ])
    client.bulk_upsert_by_field(
        "accounts", "ts_externalid", [account("W1", "Same")], fallback_on_error=False,
    )
    lookup, update = client._session.calls
    assert lookup["params"]["$select"] == "ts_externalid,accountid"
    assert len(update["json"]["Targets"]) == 1


@pytest.mark.parametrize(
    "expected, actual, same",
    [
        ("Clinic", "Clinic", True),
        ("Clinic", "clinic", False),
        ("", None, True),
        (None, "", True),
        (None, "x", False),
        (3, 3.0, True),
        (9007199254740992, 9007199254740993, False),
        (9007199254740993, 9007199254740992.0, False),
        (126870001, 126870002, False),
        (True, True, True),
        (True, 1, False),
        ("3", 3, True),
        ("9007199254740992", 9007199254740993, False),
        ("126870001", 126870001, True),
        ("126870001", 126870002, False),
        ("3a", 3, False),
        ("1e3", 1000, False),
        (3, "3", False),
        ("2026-08-18T14:44:44", "2026-08-18T14:44:44Z", True),
        ("2026-08-18T14:44:44Z", "2026-08-18T14:44:45Z", False),
        ("2026-08-18", "2026-08-18T00:00:00Z", False),
    ],
)
def test_same_value_only_matches_when_certain(expected, actual, same):
    from dataverse.client import _same_value

    assert _same_value(expected, actual) is same
