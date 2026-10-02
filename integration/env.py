"""Load settings from a .env file without adding a dependency.

The scripts previously required ``set -a && source .env && set +a`` before every
run, which is easy to forget and fails in a confusing way -- the config classes
report a missing variable rather than a missing file.
"""

import os
from pathlib import Path

# Repo root: integration/env.py -> integration/ -> repo root.
REPO_ROOT = Path(__file__).resolve().parent.parent


def log_dir():
    """Directory for local log files and reports, or None when they are off.

    ``SYNC_LOG_DIR`` picks the directory; ``off`` (or none/false/0) turns file
    output off. Unset means ``logs/`` beside the repo. The Azure app folder is
    read-only, so function_app.py defaults it to off; App Insights has the logs.
    """
    value = os.environ.get("SYNC_LOG_DIR", "").strip()
    if value.lower() in {"off", "none", "false", "0"}:
        return None
    return Path(value) if value else REPO_ROOT / "logs"


def load_env(path=None, override=False):
    """Read a ``.env`` file into ``os.environ``.

    Args:
        path: File to read. Defaults to ``.env`` beside the repo root.
        override: When False (the default) real environment variables win, so
            an exported value or a CI secret is never clobbered by the file.

    Returns:
        dict: the keys that were applied.
    """
    env_path = Path(path) if path else REPO_ROOT / ".env"
    if not env_path.is_file():
        return {}

    applied = {}
    for raw in env_path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, _, value = line.partition("=")
        key, value = key.strip(), value.strip()
        if key.startswith("export "):
            key = key[len("export "):].strip()

        # Strip matching surrounding quotes, if any.
        if len(value) >= 2 and value[0] == value[-1] and value[0] in ("'", '"'):
            value = value[1:-1]

        if override or key not in os.environ:
            os.environ[key] = value
            applied[key] = value
    return applied
