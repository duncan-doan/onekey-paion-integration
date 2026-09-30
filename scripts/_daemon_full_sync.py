#!/usr/bin/env python3
"""Double-fork daemon launcher for long-running full sync."""

from __future__ import annotations

import os
import sys
from datetime import datetime
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
LOG_DIR = REPO / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_path = LOG_DIR / f"full_sync_wuk_{stamp}.log"
pid_path = LOG_DIR / "full_sync_wuk.pid"


def daemonize() -> None:
    if os.fork() > 0:
        return
    os.setsid()
    if os.fork() > 0:
        os._exit(0)

    os.chdir(REPO)
    sys.path.insert(0, str(REPO))

    with open(log_path, "a", encoding="utf-8") as log_f:
        os.dup2(log_f.fileno(), 1)
        os.dup2(log_f.fileno(), 2)

    # stdin -> /dev/null
    with open(os.devnull, "rb") as devnull:
        os.dup2(devnull.fileno(), 0)

    pid_path.write_text(str(os.getpid()), encoding="utf-8")
    os.execl(
        sys.executable,
        sys.executable,
        "-u",
        str(REPO / "scripts" / "run_dynamic_sync.py"),
        "--codbase",
        "WUK",
        "--trigger",
        "daemon",
    )


if __name__ == "__main__":
    daemonize()
    # Parent prints paths then exits
    print(f"LOG={log_path}")
    print(f"PID_FILE={pid_path}")
