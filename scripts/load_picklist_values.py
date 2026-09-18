#!/usr/bin/env python3
"""Bulk upsert Dataverse choice options from config/value_mapping.csv.

Thin CLI around :mod:`integration.picklist_sync`. Dataverse has no
CreateMultiple for option sets, so this script InsertOptionValue /
UpdateOptionValue (optionally via ``$batch``).

Default targets (already present in Paion test):

=======  ========================  =================================
CSV type Dataverse choice          Kind
=======  ========================  =================================
hcotype  cr4ec_hcotype             global
specialty cr4ec_specialty          global
structuretype cr4ec_hcostructuretype global
=======  ========================  =================================

Usage:
    python scripts/load_picklist_values.py
    python scripts/load_picklist_values.py --types hcotype,specialty --dry-run
    python scripts/load_picklist_values.py --no-batch
"""

from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dataverse import DataverseClient, DataverseConfig
from integration import load_mapping_config, setup_logging
from integration.env import load_env
from integration.picklist_sync import DEFAULT_TARGETS, sync_picklists_from_config

logger = logging.getLogger(__name__)


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--types",
        default=",".join(DEFAULT_TARGETS),
        help=f"Comma-separated mapping_type values (default: {','.join(DEFAULT_TARGETS)})",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Compare only; do not Insert/Update",
    )
    parser.add_argument(
        "--no-batch",
        action="store_true",
        help="Send one Insert/Update request at a time instead of $batch",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Option operations per $batch changeset (default: 50)",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    load_env()
    setup_logging()
    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")

    config = load_mapping_config()
    types = [t.strip() for t in args.types.split(",") if t.strip()]
    for t in types:
        if t not in DEFAULT_TARGETS:
            print(f"Unknown mapping type {t!r}. Known: {', '.join(DEFAULT_TARGETS)}")
            return 2

    print("Picklist bulk upsert from value_mapping.csv")
    print(f"Types   : {', '.join(types)}")
    print(
        f"Mode    : {'DRY RUN' if args.dry_run else 'WRITE'}"
        f"{'' if args.no_batch else ' ($batch)'}"
    )

    with DataverseClient(DataverseConfig.from_env()) as dv:
        identity = dv.whoami()
        print(f"Dataverse user: {identity.get('UserId')}")
        summaries = sync_picklists_from_config(
            dv,
            config,
            types=types,
            dry_run=args.dry_run,
            use_batch=not args.no_batch,
            batch_size=args.batch_size,
        )

    print("\n" + "=" * 72)
    print("PICKLIST SUMMARY")
    print("=" * 72)
    print(
        f"{'type':16s} {'target':28s} {'csv':>5} {'dv':>5} {'+':>5} {'~':>5} "
        f"{'ok':>5} {'err':>5}"
    )
    for s in summaries:
        print(
            f"{s['mapping_type']:16s} {s['target'][:28]:28s} "
            f"{s['csv']:5d} {s['dataverse']:5d} {s['insert']:5d} {s['update']:5d} "
            f"{s['unchanged']:5d} {s['errors']:5d}"
        )
    total_err = sum(s["errors"] for s in summaries)
    total_chg = sum(s["insert"] + s["update"] for s in summaries)
    if args.dry_run:
        print(f"\nDry run complete — {total_chg} change(s) would be applied.")
    else:
        print(f"\nDone — {total_chg} change(s), {total_err} error(s).")
    return 1 if total_err else 0


if __name__ == "__main__":
    raise SystemExit(main())
