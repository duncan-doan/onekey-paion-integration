#!/usr/bin/env python3
"""Bulk upsert Dataverse choice options and code-list lookup tables.

Choice options come from ``config/value_mapping.csv`` and
``config/picklist_targets.csv``. Lists stored as tables (specialty, HCO type,
title, …) come from OneKey ``/ok/code``. Their ``ts_externalid`` is the
template in ``config/lookup_table_mapping.csv`` (for example ``WUK.TET.HOS``).

Usage:
    python scripts/load_picklist_values.py
    python scripts/load_picklist_values.py --types structuretype,language --dry-run
    python scripts/load_picklist_values.py --codebases WUK,WDE
    python scripts/load_picklist_values.py --codebases WUK --lookup-tables LK-TET --dry-run
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
from integration.lookup_sync import sync_lookup_tables
from integration.picklist_sync import sync_picklists_from_config
from onekey import OneKeyClient, OneKeyConfig, OneKeySession

logger = logging.getLogger(__name__)


def parse_args(known_types: list[str]):
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument(
        "--types",
        default=",".join(known_types) if known_types else "",
        help=(
            "Comma-separated mapping_type values "
            f"(default: {','.join(known_types) or 'all from picklist_targets.csv'})"
        ),
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
    parser.add_argument(
        "--codebases",
        default="",
        help="Comma-separated OneKey codBase values (WUK,WDE). Required to load lookup tables.",
    )
    parser.add_argument(
        "--lookup-tables",
        default="",
        help="Comma-separated lookup_id or list_code values (default: all lookup tables)",
    )
    parser.add_argument(
        "--skip-choices",
        action="store_true",
        help="Do not upsert choice options; only load lookup tables",
    )
    return parser.parse_args()


def main() -> int:
    load_env()
    setup_logging()
    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")

    config = load_mapping_config()
    target_map = config.picklist_target_map()
    if not target_map:
        print("No picklist targets in config/picklist_targets.csv")
        return 2

    args = parse_args(list(target_map.keys()))
    types = [t.strip() for t in args.types.split(",") if t.strip()]
    if not types:
        types = list(target_map.keys())
    for t in types:
        if t not in target_map:
            print(
                f"Unknown mapping type {t!r}. "
                f"Known: {', '.join(target_map)}"
            )
            return 2

    codebases = [c.strip() for c in args.codebases.split(",") if c.strip()]
    lookup_tables = [t.strip() for t in args.lookup_tables.split(",") if t.strip()]

    print("Picklist bulk upsert")
    if not args.skip_choices:
        print(f"Choices : {', '.join(types)}")
    if codebases:
        print(f"Lookups : {', '.join(lookup_tables) or 'all'} for {', '.join(codebases)}")
    else:
        print("Lookups : skipped (pass --codebases WUK to load lookup tables)")
    print(
        f"Mode    : {'DRY RUN' if args.dry_run else 'WRITE'}"
        f"{'' if args.no_batch else ' ($batch)'}"
    )

    onekey = None
    if codebases:
        onekey_config = OneKeyConfig.from_env()
        onekey = OneKeyClient(OneKeySession(onekey_config))

    with DataverseClient(DataverseConfig.from_env()) as dv:
        identity = dv.whoami()
        print(f"Dataverse user: {identity.get('UserId')}")
        summaries = []
        if not args.skip_choices:
            summaries.extend(
                sync_picklists_from_config(
                    dv,
                    config,
                    types=types,
                    dry_run=args.dry_run,
                    use_batch=not args.no_batch,
                    batch_size=args.batch_size,
                )
            )
        if codebases and onekey is not None:
            summaries.extend(
                sync_lookup_tables(
                    dv,
                    onekey,
                    config,
                    codebases,
                    dry_run=args.dry_run,
                    tables=lookup_tables or None,
                )
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
