"""Offline tests for the pre-sync Dataverse schema check."""

import pytest

from dataverse.exceptions import DataverseHTTPError
from integration.dynamic_mapping import load_mapping_config
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


class SchemaClient(Client):
    """Tables hold every mapped column except ``drop``; ``metadata`` fails a read."""

    def __init__(self, config, drop=None, metadata=None, missing_tables=()):
        super().__init__()
        self.columns = {
            name: {fm.target_field for fm in config.get_field_mappings(name)}
            for name in {fm.target_entity for fm in config.field_mappings}
        }
        for table, column in (drop or {}).items():
            self.columns[table].discard(column)
        self.metadata = metadata
        self.missing_tables = set(missing_tables)
        self.paths = []

    def request(self, method, path, **kwargs):
        assert method == "GET"
        self.paths.append(path)
        table = path.split("'")[1]
        if self.metadata or table in self.missing_tables:
            raise DataverseHTTPError("no", status_code=self.metadata or 404)
        body = {"Attributes": [{"LogicalName": name} for name in self.columns.get(table, ())]}
        return type("Response", (), {"json": lambda self: body})()


def _messages(result):
    return [str(issue) for issue in result.errors + result.warnings]


def test_the_mapped_columns_all_present_pass():
    config = load_mapping_config()
    result = DataverseValidator(SchemaClient(config)).validate_schema(config)
    assert result.is_valid
    assert result.warnings == []


def test_a_missing_column_on_a_required_table_stops_the_sync():
    # Dataverse rejects every account that carries the column, so the sync
    # used to start and fail on each record instead.
    config = load_mapping_config()
    client = SchemaClient(config, drop={"account": "ts_substatus"})
    result = DataverseValidator(client).validate_schema(config)
    assert not result.is_valid
    assert any("account" in m and "ts_substatus" in m for m in _messages(result))
    assert "ts_substatus" in result.summary()  # the job row says what failed


def test_a_missing_column_or_table_elsewhere_is_a_warning():
    config = load_mapping_config()
    client = SchemaClient(
        config, drop={"ts_affiliation": "ts_substatus"}, missing_tables={"ts_affiliationrole"}
    )
    result = DataverseValidator(client).validate_schema(config)
    assert result.is_valid
    warnings = " ".join(str(w) for w in result.warnings)
    assert "ts_substatus" in warnings
    assert "ts_affiliationroles" in warnings


@pytest.mark.parametrize("status", [403, 503])
def test_columns_that_cannot_be_read_do_not_stop_the_sync(status):
    config = load_mapping_config()
    result = DataverseValidator(SchemaClient(config, metadata=status)).validate_schema(config)
    assert result.is_valid


def test_each_table_is_read_once():
    config = load_mapping_config()
    client = SchemaClient(config)
    validator = DataverseValidator(client)
    validator.validate_schema(config)
    validator.validate_schema(config)
    assert len(client.paths) == len(set(client.paths))
