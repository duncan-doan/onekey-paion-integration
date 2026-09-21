"""Offline tests for the Dataverse client and the OneKey -> D365 mapping."""

import json

import pytest

from dataverse import (
    DataverseAuthError,
    DataverseClient,
    DataverseConfig,
    DataverseConfigError,
    DataverseHTTPError,
    TokenProvider,
    escape_odata,
)
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
        return self.responses.pop(0)

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
    client = make_client([
        FakeResponse(200, {"value": [{"id": 1}], "@odata.nextLink": "https://x/page2"}),
        FakeResponse(200, {"value": [{"id": 2}]}),
    ])
    assert [r["id"] for r in client.retrieve_multiple("accounts")] == [1, 2]


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


def test_odata_quotes_are_escaped():
    # O'Brien would otherwise terminate the filter literal early.
    assert escape_odata("O'Brien") == "O''Brien"


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
