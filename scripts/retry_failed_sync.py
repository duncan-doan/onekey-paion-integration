#!/usr/bin/env python3
"""Retry pending OneKey integration failures.

Reads ts_integrationfailure rows that are Pending, retryable, under five
attempts, and whose parent integration job is no longer Running. Each parent
job gets its own retry Integration Job.

Usage:
    python scripts/retry_failed_sync.py --dry-run
    python scripts/retry_failed_sync.py --codbase WUK --limit 50
    python scripts/retry_failed_sync.py --job-id <ts_jobid>
    python scripts/retry_failed_sync.py --source-ids WUKE00001259,WUKE00009999
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dataverse import DataverseClient, DataverseConfig
from integration import create_sync_engine, load_mapping_config, setup_logging
from integration.env import load_env
from integration.job_trace import TRIGGER_BY_NAME
from integration.retry_sync import retry_pending_failures
from onekey import OneKeyClient, OneKeyConfig, OneKeySession


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--codbase", help="Limit to one OneKey codebase, such as WUK")
    parser.add_argument("--job-id", help="Limit to failures from this ts_jobid")
    parser.add_argument(
        "--source-ids",
        help="Comma-separated OneKey source IDs to retry",
    )
    parser.add_argument(
        "--phase",
        choices=(
            "accounts",
            "addresses",
            "hierarchy",
            "affiliations",
            "contacts",
            "activity_affiliations",
        ),
        help="Limit to one sync phase",
    )
    parser.add_argument("--limit", type=int, default=100, help="Maximum failure rows")
    parser.add_argument("--dry-run", action="store_true", help="List the queue and do not write")
    parser.add_argument(
        "--trigger",
        choices=sorted(TRIGGER_BY_NAME),
        default="manual",
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
    load_env()

    try:
        load_mapping_config(Path(args.config_dir) if args.config_dir else None)
    except FileNotFoundError as exc:
        logging.error("Configuration not found: %s", exc)
        return 1

    source_ids = [
        part.strip() for part in (args.source_ids or "").split(",") if part.strip()
    ]
    onekey = OneKeyClient(OneKeySession(OneKeyConfig.from_env()))
    dataverse = DataverseClient(DataverseConfig.from_env())
    engine = create_sync_engine(
        onekey,
        dataverse,
        args.config_dir,
        bulk_batch_size=args.bulk_batch_size,
    )
    engine.tracer.trigger = TRIGGER_BY_NAME[args.trigger]
    try:
        rows = None
        if args.phase or args.dry_run:
            from integration.retry_sync import fetch_pending_failures

            # The phase is part of the query, so --limit counts its rows only.
            rows = fetch_pending_failures(
                dataverse,
                codebase=args.codbase,
                job_id=args.job_id,
                source_ids=source_ids or None,
                steps=[args.phase] if args.phase else None,
                limit=args.limit,
            )
        summary = retry_pending_failures(
            engine,
            codebase=args.codbase,
            job_id=args.job_id,
            source_ids=source_ids or None,
            limit=args.limit,
            dry_run=args.dry_run,
            trigger=TRIGGER_BY_NAME[args.trigger],
            rows=rows,
        )
    finally:
        onekey.close()
        if hasattr(dataverse, "close"):
            dataverse.close()

    print("\n" + "=" * 60)
    print("RETRY SUMMARY")
    print("=" * 60)
    print(f"Success:  {summary.get('success', False)}")
    print(f"Selected: {summary.get('selected', 0)}")
    print(f"Resolved: {summary.get('resolved', 0)}")
    print(f"Pending:  {summary.get('pending', 0)}")
    print(f"Ignored:  {summary.get('ignored', 0)}")
    if summary.get("jobs"):
        print(f"Jobs:     {', '.join(summary['jobs'])}")
    for error in (summary.get("errors") or [])[:5]:
        print(f"  - {error}")
    print("=" * 60)
    return 0 if summary.get("success") else 1


if __name__ == "__main__":
    sys.exit(main())
