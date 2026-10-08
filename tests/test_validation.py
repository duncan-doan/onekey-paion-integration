"""Offline tests for the pre-sync Dataverse schema check."""

from integration.validation import DataverseValidator


class Client:
    def __init__(self, fail=False):
        self.fail = fail
        self.queries = []

    def retrieve_multiple(self, entity_set, **kwargs):
        self.queries.append((entity_set, kwargs))
        if self.fail:
            raise RuntimeError("Resource not found for the segment 'nope'")
        return [{"accountid": "a-1"}]


def test_entity_check_reads_one_row_not_the_whole_table():
    client = Client()
    assert DataverseValidator(client).validate_entity_exists("accounts") is True
    # top=0 is falsy, so it used to send no $top and page through every row.
    assert client.queries == [("accounts", {"top": 1, "max_records": 1})]


def test_missing_entity_set_fails_the_check():
    assert DataverseValidator(Client(fail=True)).validate_entity_exists("nope") is False
