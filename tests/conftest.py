"""Shared pytest setup.

Live API calls require an explicit --live flag, even when credentials exist.
"""

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def pytest_addoption(parser):
    parser.addoption("--live", action="store_true", help="Enable live OneKey API tests")


def pytest_configure(config):
    config.addinivalue_line("markers", "live: calls the real OneKey API")
    if config.getoption("--live"):
        from integration import load_env

        load_env()


def pytest_collection_modifyitems(config, items):
    if not config.getoption("--live"):
        for item in items:
            if "live" in item.keywords:
                item.add_marker(pytest.mark.skip(reason="live API tests require --live"))
