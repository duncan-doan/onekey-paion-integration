#!/usr/bin/env python3
"""How long a scheduled run took, for all its countries together.

Reads the ts_integrationjob rows of one timer run (their ts_name carries
run=<run_id>) and prints each job's time, the run's total time (from when the
timer queued it until its last job ended) and its work time (all job times
added up). A run still in progress shows its time so far.

Usage:
    python scripts/sync_run_report.py                       # newest run
    python scripts/sync_run_report.py --last full           # newest full-sync run
    python scripts/sync_run_report.py --last delta          # newest daily run
    python scripts/sync_run_report.py --run delta-20261012T000001Z
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dataverse import DataverseClient, DataverseConfig
from integration.env import load_env
from integration.run_report import (
    fetch_run_jobs,
    format_duration,
    latest_run_id,
    summarize_run,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--run", help="Run id, as logged by the timer (run=...)")
    group.add_argument(
        "--last",
        choices=["delta", "full"],
        help="The newest run of this kind (default: the newest run of either)",
    )
    return parser.parse_args()


def _time(moment) -> str:
    return moment.strftime("%m-%d %H:%M:%S") if moment else "-"


def main() -> int:
    args = parse_args()
    load_env()
    dataverse = DataverseClient(DataverseConfig.from_env())
    run_id = args.run or latest_run_id(dataverse, args.last)
    if not run_id:
        print("No scheduled run found in ts_integrationjob.")
        return 1
    jobs = fetch_run_jobs(dataverse, run_id)
    if not jobs:
        print(f"No jobs found for run {run_id}.")
        return 1
    summary = summarize_run(run_id, jobs)

    expected = summary["expected"]
    of = f" of {expected}" if expected is not None else ""
    print(f"Run {run_id}  queued {_time(summary['queued_at'])} UTC")
    print(
        f"Countries finished: {len(summary['finished'])}{of}  jobs: {len(summary['jobs'])}  "
        f"running: {summary['running']}  stopped: {summary['stopped']}"
    )
    so_far = "" if summary["complete"] else " so far"
    print(f"Total time: {format_duration(summary['total_s'])}{so_far} (queued to last job end)")
    print(f"Work time:  {format_duration(summary['work_s'])} (all jobs added up)")
    if not summary["complete"] and not summary["running"]:
        print(
            "Nothing is running. A country without a finished job is waiting for a "
            "retry (30 min) or a redelivery, was skipped as busy, or went to a poison queue."
        )
    print()
    print(f"{'Codebase':<9} {'Type':<12} {'State':<21} {'Started':<15} {'Ended':<15} Duration")
    for row in summary["jobs"]:
        print(
            f"{row['codebase'] or '-':<9} {row['type']:<12} {row['state']:<21} "
            f"{_time(row['started']):<15} {_time(row['ended']):<15} "
            f"{format_duration(row['duration_s'])}"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
