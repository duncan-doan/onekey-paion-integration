"""Log lines: readable lead + key=value, short job ID, IDs only, timing, HTTP totals."""

import json
import logging
from types import SimpleNamespace

import pytest

from dataverse import DataverseClient, DataverseConfig
from dataverse.exceptions import DataverseHTTPError
from integration.dynamic_sync import DynamicSyncEngine
from integration.sync_logger import SyncLogger, entity_counts, kv, msg, short_job
from integration.validation import ValidationError, ValidationResult
from onekey import OneKeyConfig, OneKeySession
from onekey.exceptions import OneKeyRateLimitError


class FakeResponse:
    def __init__(self, status_code=200, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}
        self.text = "" if payload is None else json.dumps(payload)
        self.content = self.text.encode()

    def json(self):
        if self._payload is None:
            raise ValueError("no json")
        return self._payload


class FakeTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.headers = {}

    def request(self, method, url, **kwargs):
        return self.responses.pop(0)

    def close(self):
        pass


class StubTokens:
    def token(self, force_refresh=False):
        return "stub-token"


@pytest.fixture(autouse=True)
def no_log_files(monkeypatch):
    monkeypatch.setenv("SYNC_LOG_DIR", "off")
    monkeypatch.setattr("onekey.auth.time.sleep", lambda _: None)
    monkeypatch.setattr("dataverse.client.time.sleep", lambda _: None)


def _messages(caplog, logger_name=None, level=None):
    return [
        record.getMessage()
        for record in caplog.records
        if (logger_name is None or record.name == logger_name)
        and (level is None or record.levelno == level)
    ]


def onekey_session(responses):
    config = OneKeyConfig("https://okws-uat.onekey.iqvia.com", "u", "p", "494", max_retries=3)
    return OneKeySession(config, session=FakeTransport(responses))


def dataverse_client(responses):
    config = DataverseConfig("https://org.crm4.dynamics.com", "tenant", "client", "secret")
    return DataverseClient(config, token_provider=StubTokens(), session=FakeTransport(responses))


# -- kv ---------------------------------------------------------------------


def test_kv_formats_quotes_and_skips_none():
    line = kv(
        job="abc",
        codbases=["WUK", "WDE"],
        seconds=12.345,
        ok=True,
        error='Contact "x" not found',
        missing=None,
        empty="",
    )
    assert line == (
        'job=abc codbases=WUK,WDE seconds=12.3 ok=True '
        'error="Contact \\"x\\" not found" empty=""'
    )


def test_msg_puts_fields_after_the_lead_and_context_last():
    line = msg("Accounts done in 1.0s", {"codbase": "WUK", "job": "e6125523"}, processed=2)
    assert line == "Accounts done in 1.0s: processed=2 | codbase=WUK job=e6125523"
    assert msg("Accounts started", {"job": None}) == "Accounts started"
    assert short_job("e6125523-0bb7-436e-89c8-05d9c70f57e0") == "e6125523"


def test_codebase_phase_names_the_codebase_in_the_lead(caplog):
    caplog.set_level(logging.INFO)
    sync_logger = SyncLogger()
    sync_logger.job_id = "job-1"
    started = sync_logger.phase_started("codebase", codbase="WUK", date_from="2026-10-01")
    sync_logger.phase_finished("codebase", started, codbase="WUK")
    start, finish = _messages(caplog, "integration.sync_logger")
    assert start == "Codebase WUK started | date_from=2026-10-01 job=job-1"
    assert finish.startswith("Codebase WUK done in ")
    assert finish.endswith(" | job=job-1")


def test_entity_counts_lists_touched_entities():
    results = {
        "accounts": {"processed": 3, "created": 1, "updated": 2, "failed": 0},
        "contacts": {"processed": 0, "created": 0, "updated": 0, "failed": 0},
    }
    assert entity_counts(results) == {"accounts": "3/1/2/0/0"}


# -- SyncLogger ----------------------------------------------------------------


def test_record_success_is_debug_and_ids_only(caplog):
    caplog.set_level(logging.DEBUG)
    sync_logger = SyncLogger()
    sync_logger.job_id = "job-1"
    sync_logger.log_record_success("contact", "WUKM00000001", "guid-1", "create")
    record = caplog.records[-1]
    assert record.levelno == logging.DEBUG
    assert record.getMessage() == "Contact WUKM00000001 created: target=guid-1 | job=job-1"


def test_end_sync_reports_the_engines_counts(caplog):
    caplog.set_level(logging.INFO)
    sync_logger = SyncLogger()
    sync_logger.job_id = "job-1"
    sync_logger.start_sync("delta", ["WUK"], "2026-10-01T00:00:00Z", "2026-10-02T00:00:00Z")
    results = {
        "accounts": {"processed": 3, "created": 1, "updated": 2, "failed": 0},
        "contacts": {"processed": 5, "created": 5, "updated": 0, "failed": 1},
        "hierarchy": {"linked": 2},
        "errors": [],
    }
    sync_logger.end_sync(success=True, results=results)
    start, finish = _messages(caplog, "integration.sync_logger")
    assert start == (
        "==== Delta sync WUK started: date_from=2026-10-01T00:00:00Z "
        "date_to=2026-10-02T00:00:00Z | job=job-1 ===="
    )
    assert finish.startswith("==== Delta sync WUK finished in ")
    assert (
        "(processed/created/updated/skipped/failed): success=True "
        "accounts=3/1/2/0/0 contacts=5/5/0/0/1 hierarchy_linked=2"
    ) in finish
    assert finish.endswith("| job=job-1 ====")
    # The summary JSON used to report zero accounts and contacts.
    assert sync_logger.session.stats.accounts_processed == 3
    assert sync_logger.session.stats.contacts_failed == 1


def test_validation_lines_leave_out_source_values(caplog):
    caplog.set_level(logging.INFO)
    sync_logger = SyncLogger()
    result = ValidationResult()
    result.add_error(
        ValidationError(
            error_type="exceeds_max_length",
            severity="warning",
            entity="contact",
            field="lastname",
            message="Value exceeds max length 50, will be truncated",
            source_value="Personal Surname",
        )
    )
    sync_logger.log_validation_result(result, "HCP WUKM00000001")
    line = _messages(caplog, "integration.sync_logger")[-1]
    assert line.startswith(
        "Validation warning (HCP WUKM00000001): entity=contact field=lastname "
        "type=exceeds_max_length"
    )
    assert "Personal Surname" not in line


def test_phase_finished_logs_counts_and_rate(caplog, monkeypatch):
    caplog.set_level(logging.INFO)
    sync_logger = SyncLogger()
    sync_logger.job_id = "job-1"
    clock = iter([100.0, 110.0])
    monkeypatch.setattr("integration.sync_logger.time.monotonic", lambda: next(clock))
    started = sync_logger.phase_started("contacts", codbase="WUK")
    sync_logger.phase_finished(
        "contacts",
        started,
        {"processed": 500, "created": 10, "updated": 490, "failed": 0},
        codbase="WUK",
    )
    start, finish = _messages(caplog, "integration.sync_logger")[-2:]
    assert start == "Contacts started | codbase=WUK job=job-1"
    assert finish == (
        "Contacts done in 10.0s: processed=500 created=10 updated=490 failed=0 per_s=50 "
        "| codbase=WUK job=job-1"
    )


# -- engine: phases and HTTP totals ------------------------------------------------


class Engine:
    """Just enough of DynamicSyncEngine for its job and phase helpers."""

    _begin_job = DynamicSyncEngine._begin_job
    _finish_job = DynamicSyncEngine._finish_job
    _http_snapshot = DynamicSyncEngine._http_snapshot
    _log_http_summary = DynamicSyncEngine._log_http_summary
    _set_request_hook = DynamicSyncEngine._set_request_hook
    _phase = DynamicSyncEngine._phase

    def __init__(self):
        self.succeeded_ids = set()
        self.succeeded_records = set()
        self.onekey = SimpleNamespace(session=onekey_session([]))
        self.dataverse = dataverse_client([])
        self.sync_logger = SyncLogger()
        self.tracer = SimpleNamespace(
            job_id=None,
            begin=lambda **kwargs: setattr(self.tracer, "job_id", "job-7"),
            finish=lambda results, aborted=False: None,
            flush=lambda heartbeat=False: None,
            beat=lambda: None,
        )


def test_job_logs_http_summary_for_its_own_calls(caplog):
    caplog.set_level(logging.INFO)
    engine = Engine()
    engine.dataverse.stats.calls = engine.dataverse.stats.attempts = 40  # before the job
    engine._begin_job(1, ["WUK"], dry_run=False)
    stats = engine.dataverse.stats
    stats.calls, stats.attempts, stats.throttled, stats.seconds = 50, 53, 1, 4.0
    engine._finish_job({})
    lines = _messages(caplog, "integration.dynamic_sync")
    assert lines == [
        "Dataverse calls: calls=10 retries=3 throttled=1 failed=0 seconds=4.0 avg_ms=400 "
        "| job=job-7"
    ]
    assert engine.sync_logger.job_id == "job-7"


def test_phase_logs_start_and_outcome(caplog):
    caplog.set_level(logging.INFO)
    engine = Engine()
    engine.sync_logger.job_id = "job-7"
    with engine._phase(1, "accounts", codbase="WUK") as phase:
        phase["stats"] = {"processed": 2, "created": 1, "updated": 1, "failed": 0}
    with pytest.raises(RuntimeError):
        with engine._phase(2, "addresses"):
            raise RuntimeError("Dataverse down")
    lines = _messages(caplog, "integration.sync_logger")
    assert lines[0] == "Accounts started | codbase=WUK job=job-7"
    assert lines[1].startswith("Accounts done in ")
    assert ": processed=2 created=1 updated=1 failed=0 per_s=" in lines[1]
    assert lines[1].endswith("| codbase=WUK job=job-7")
    assert lines[2] == "Addresses started | job=job-7"
    assert lines[3].startswith("Addresses failed after ")
    assert lines[3].endswith(": error=RuntimeError | job=job-7")


# -- HTTP clients ---------------------------------------------------------------------


def test_onekey_session_counts_retries_and_logs_each_call(caplog):
    caplog.set_level(logging.DEBUG)
    session = onekey_session([FakeResponse(503), FakeResponse(200, {"status": "SUCCESS"})])
    session.request("GET", "/system/connection/test", with_connector=False)
    assert session.stats.calls == 1
    assert session.stats.attempts == 2
    assert session.stats.failed == 0
    retry, call = _messages(caplog, "onekey.auth")
    assert retry.startswith(
        "OneKey GET /system/connection/test retrying: attempt=1/3 reason=503"
    )
    assert call.startswith("OneKey GET /system/connection/test: status=200 attempts=2 ms=")


def test_onekey_session_counts_throttling_and_failure():
    session = onekey_session([FakeResponse(429) for _ in range(3)])
    with pytest.raises(OneKeyRateLimitError):
        session.request("GET", "/system/connection/test", with_connector=False)
    assert (session.stats.calls, session.stats.attempts) == (1, 3)
    assert (session.stats.throttled, session.stats.failed) == (3, 1)


def test_onekey_warning_status_does_not_log_records(caplog):
    caplog.set_level(logging.WARNING)
    envelope = {
        "status": "WARNING",
        "statusMessage": "Partial result",
        "results": [{"individual": {"lastName": "Personal Surname"}}],
    }
    session = onekey_session([FakeResponse(200, {"response": envelope})])
    session.post_json("/ok/search", {})
    line = _messages(caplog, "onekey.auth")[-1]
    assert line == "OneKey returned WARNING for /ok/search: message='Partial result' results=1"


def test_dataverse_client_counts_retries_and_trims_paths(caplog):
    caplog.set_level(logging.DEBUG)
    client = dataverse_client([FakeResponse(503), FakeResponse(200, {"value": []})])
    client.request("GET", "https://org.crm4.dynamics.com/api/data/v9.2/contacts?$skiptoken=x")
    assert (client.stats.calls, client.stats.attempts, client.stats.failed) == (1, 2, 0)
    retry, call = _messages(caplog, "dataverse.client")
    assert retry.startswith("Dataverse GET contacts retrying: attempt=1/")
    assert call.startswith("Dataverse GET contacts: status=200 attempts=2 ms=")


def test_dataverse_client_counts_a_failed_call():
    client = dataverse_client([FakeResponse(400, {"error": {"message": "Bad"}})])
    with pytest.raises(DataverseHTTPError):
        client.request("GET", "accounts")
    assert (client.stats.calls, client.stats.failed) == (1, 1)
