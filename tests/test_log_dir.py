"""Local log files must never stop a sync (the Azure app folder is read-only)."""

import logging

import pytest

from integration.env import REPO_ROOT, log_dir
from integration.picklist_gaps import PicklistGapCollector
from integration.sync_logger import SyncLogger, setup_logging


@pytest.fixture
def root_handlers():
    """Restore the root logger's handlers after the test."""
    root = logging.getLogger()
    before = list(root.handlers)
    yield root
    for handler in root.handlers[len(before):]:
        handler.close()
    root.handlers[:] = before


@pytest.fixture
def read_only_dir(tmp_path):
    """A log path that cannot be created: its parent is a regular file."""
    blocker = tmp_path / "blocker"
    blocker.write_text("not a directory", encoding="utf-8")
    return blocker / "logs"


def _gaps():
    gaps = PicklistGapCollector()
    gaps.record("specialty", "SP.WUK.XX", target_field="ts_specialty1")
    return gaps


@pytest.mark.parametrize("value", ["off", "OFF", "none", "false", "0"])
def test_log_dir_can_be_turned_off(monkeypatch, value):
    monkeypatch.setenv("SYNC_LOG_DIR", value)
    assert log_dir() is None


def test_log_dir_defaults_to_repo_logs(monkeypatch, tmp_path):
    monkeypatch.delenv("SYNC_LOG_DIR", raising=False)
    assert log_dir() == REPO_ROOT / "logs"
    monkeypatch.setenv("SYNC_LOG_DIR", str(tmp_path))
    assert log_dir() == tmp_path


def test_sync_logger_writes_nothing_when_off(monkeypatch, root_handlers):
    monkeypatch.setenv("SYNC_LOG_DIR", "off")
    before = list(root_handlers.handlers)
    sync_logger = SyncLogger()
    sync_logger.start_sync("delta", ["WUK"])
    sync_logger.end_sync()
    assert sync_logger.log_dir is None
    assert root_handlers.handlers == before


def test_sync_logger_survives_a_read_only_dir(monkeypatch, root_handlers, read_only_dir):
    monkeypatch.setenv("SYNC_LOG_DIR", str(read_only_dir))
    sync_logger = SyncLogger()
    sync_logger.end_sync()
    assert sync_logger.log_dir is None


def test_sync_logger_writes_log_and_summary(monkeypatch, root_handlers, tmp_path):
    monkeypatch.setenv("SYNC_LOG_DIR", str(tmp_path))
    sync_logger = SyncLogger(session_id="t1")
    sync_logger.end_sync()
    assert (tmp_path / "sync_t1.log").is_file()
    assert (tmp_path / "sync_t1_summary.json").is_file()


def test_setup_logging_survives_a_read_only_dir(root_handlers, read_only_dir):
    before = list(root_handlers.handlers)
    setup_logging(log_dir=read_only_dir, include_console=False)
    assert root_handlers.handlers == before


def test_gap_report_skipped_when_off(monkeypatch):
    monkeypatch.setenv("SYNC_LOG_DIR", "off")
    assert _gaps().write_report() is None


def test_gap_report_survives_a_read_only_dir(monkeypatch, read_only_dir):
    monkeypatch.setenv("SYNC_LOG_DIR", str(read_only_dir))
    assert _gaps().write_report() is None


def test_gap_report_written_to_log_dir(monkeypatch, tmp_path):
    monkeypatch.setenv("SYNC_LOG_DIR", str(tmp_path))
    path = _gaps().write_report()
    assert path is not None and path.parent == tmp_path
    assert "SP.WUK.XX" in path.read_text(encoding="utf-8")


def test_closing_a_sync_logger_detaches_its_log_file(tmp_path):
    # A warm instance builds a SyncLogger per queue message; an open handler
    # left on the root logger would copy every later run into this file.
    import logging

    from integration.sync_logger import SyncLogger

    root = logging.getLogger()
    before = list(root.handlers)
    sync_logger = SyncLogger(session_id="closed", log_dir=tmp_path)
    assert len(root.handlers) == len(before) + 1
    sync_logger.close()
    assert root.handlers == before
    logging.getLogger("integration").warning("after close")
    assert "after close" not in (tmp_path / "sync_closed.log").read_text()
    sync_logger.close()  # closing twice is harmless
