"""Live tests against a real OneKey environment.

Skipped unless --live is passed and credentials are present, so the default
`pytest` run stays offline even with a configured .env:

    export ONEKEY_ENV=uat
    export ONEKEY_USERNAME=... ONEKEY_PASSWORD=... ONEKEY_CONNECTOR_ID=494
    pytest tests/test_live.py --live -v

These call the real service and count against rate limits, so they are kept few
and small.
"""

import os

import pytest

from onekey import (
    ACTIVITY,
    WORKPLACE,
    OneKeyAuthError,
    OneKeyClient,
    OneKeyConfig,
    OneKeySession,
    flatten_hcp,
    flatten_hco,
)

pytestmark = [
    pytest.mark.live,
    pytest.mark.skipif(
        not os.environ.get("ONEKEY_USERNAME"),
        reason="live OneKey credentials not configured; see the module docstring",
    ),
]

# Territory to exercise. WDE is Germany.
COD_BASE = os.environ.get("ONEKEY_TEST_CODBASE", "WDE")


@pytest.fixture(scope="module")
def client():
    config = OneKeyConfig.from_env()
    with OneKeyClient(OneKeySession(config)) as c:
        yield c


def test_credentials_are_accepted(client):
    assert client.session.test_connection() is True


def test_wrong_password_is_rejected(client):
    config = client.config
    bad = OneKeySession(
        OneKeyConfig(
            base_url=config.base_url,
            username=config.username,
            password="definitely-not-the-password",
            connector_id=config.connector_id,
            max_retries=1,
        )
    )
    with pytest.raises(OneKeyAuthError):
        bad.test_connection()
    bad.close()


def test_search_hcp_returns_usable_rows(client):
    results = client.search_hcp(cod_base=COD_BASE, last_name="Schmidt", result_size=3)
    assert results, "expected at least one HCP"

    for raw in results:
        assert raw["entityType"] == ACTIVITY
        row = flatten_hcp(raw)
        assert row["lastName"]
        assert row["individualEid"]
        assert row["activityEid"]


def test_search_hco_returns_usable_rows(client):
    results = client.search_hco(cod_base=COD_BASE, name="Klinik", result_size=3)
    assert results, "expected at least one HCO"

    for raw in results:
        assert raw["entityType"] == WORKPLACE
        row = flatten_hco(raw)
        assert row["workplaceEid"]
        assert row["officialName"] or row["usualName"]


def test_population_paging_advances_across_packets(client):
    records = list(
        client.iter_population(cod_base=COD_BASE, page_size=100, max_records=250)
    )
    assert len(records) == 250
    # Distinct ids prove the cursor moved rather than replaying packet 1.
    assert len({r["onekeyEid"] for r in records}) == 250
