"""Shared pytest setup.

Loads .env so the live tests pick up credentials automatically. They still skip
cleanly when the file is absent, which is what happens in CI.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from integration import load_env  # noqa: E402

load_env()
