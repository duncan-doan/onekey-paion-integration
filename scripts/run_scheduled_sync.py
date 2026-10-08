#!/usr/bin/env python3
"""Scheduled OneKey job: retry pending failures, then pull the delta.

The delta starts where the last Full or Delta integration job of the codebase
that covered its window stopped reading (the earlier of its ts_watermarkto and
ts_startedon). Each codebase retries only its own failed IDs, which stay in
ts_integrationfailure so the next run retries them even if OneKey does not
change those records again. Unlike the queue worker, the CLI does not check
whether the codebase is already syncing.

Usage:
    python scripts/run_scheduled_sync.py --dry-run --codbase WUK
    python scripts/run_scheduled_sync.py --codbase WUK
    python scripts/run_scheduled_sync.py --date-from 2026-09-29 --date-to 2026-09-30
    python scripts/run_scheduled_sync.py --full --codbase WUK
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from integration import setup_logging
from integration.job_trace import TRIGGER_BY_NAME
from integration.scheduled_sync import run_from_env


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--codbase", default="WUK", help="Comma-separated OneKey codebases")
    parser.add_argument(
        "--full", action="store_true", help="Run the monthly full sync instead of the delta"
    )
    parser.add_argument(
        "--date-from",
        help="Delta start (YYYY-MM-DD). Defaults to the last integration job's start.",
    )
    parser.add_argument("--date-to", help="Delta end (YYYY-MM-DD). Defaults to today.")
    parser.add_argument("--retry-limit", type=int, default=100, help="Pending failures per run")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--trigger",
        choices=sorted(TRIGGER_BY_NAME),
        default="timer",
    )
    parser.add_argument("--bulk-batch-size", type=int, default=None)
    parser.add_argument("--config-dir")
    parser.add_argument("--verbose", "-v", action="store_true")
    parser.add_argument("--quiet", "-q", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    log_level = (
        logging.DEBUG if args.verbose else (logging.WARNING if args.quiet else logging.INFO)
    )
    setup_logging(level=log_level)
    codebases = [part.strip().upper() for part in args.codbase.split(",") if part.strip()]
    result = run_from_env(
        codebases=codebases,
        full=args.full,
        date_from=args.date_from,
        date_to=args.date_to,
        dry_run=args.dry_run,
        trigger=TRIGGER_BY_NAME[args.trigger],
        retry_limit=args.retry_limit,
        config_dir=args.config_dir,
        bulk_batch_size=args.bulk_batch_size,
    )
    retry = result.get("retry") or {}
    delta = result.get("delta") or {}
    print("\n" + "=" * 60)
    print("SCHEDULED SYNC")
    print("=" * 60)
    print(f"Success: {result.get('success', False)}")
    print(
        "Retry:   "
        f"selected={retry.get('selected', 0)} "
        f"resolved={retry.get('resolved', 0)} "
        f"pending={retry.get('pending', 0)} "
        f"ignored={retry.get('ignored', 0)}"
    )
    if retry.get("jobs"):
        print(f"Retry jobs: {', '.join(retry['jobs'])}")
    if delta.get("job_id"):
        print(f"Delta job:  {delta['job_id']} ({delta.get('trace_status', '')})")
    print(f"Resolved by delta: {result.get('resolved_by_delta', 0)}")
    print("=" * 60)
    return 0 if result.get("success") else 1


if __name__ == "__main__":
    sys.exit(main())
