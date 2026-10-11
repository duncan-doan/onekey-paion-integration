"""Offline tests for the pre-sync Dataverse schema check."""

import pytest

from dataverse.exceptions import DataverseHTTPError
from integration.validation import DataverseValidator


class Client:
    def __init__(self, fail=False):
        self.fail = fail
        self.queries = []

    def retrieve_multiple(self, entity_set, **kwargs):
        self.queries.append((entity_set, kwargs))
        if self.fail is not False:
            raise DataverseHTTPError("Resource not found for the segment 'nope'", status_code=self.fail)
        return [{"accountid": "a-1"}]


def test_entity_check_reads_one_row_not_the_whole_table():
    client = Client()
    assert DataverseValidator(client).validate_entity_exists("accounts") is True
    # top=0 is falsy, so it used to send no $top and page through every row.
    assert client.queries == [("accounts", {"top": 1, "max_records": 1})]


@pytest.mark.parametrize("status", [403, 404])
def test_missing_entity_set_fails_the_check(status):
    assert DataverseValidator(Client(fail=status)).validate_entity_exists("nope") is False


@pytest.mark.parametrize("status", [429, 503, None])
def test_an_outage_is_not_reported_as_a_missing_entity(status):
    # A schema error is not retried; a throttled or failed read must be.
    with pytest.raises(DataverseHTTPError):
        DataverseValidator(Client(fail=status)).validate_entity_exists("accounts")
