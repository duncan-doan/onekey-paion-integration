#!/usr/bin/env python3
"""Run phased OneKey → Dataverse migration (full or delta).

Canonical phase order (contracted refreshMyOnekey IDs only):
  0. Validate + upsert picklists from value_mapping.csv
  1. HCO accounts (workplaceEid → account key)
  2. customeraddress ({workplaceEid} - {addressEid}, parentid=account GUID)
  3. HCO parentaccountid hierarchy (managerWorkplaceEid)
  4. HCO–HCO ts_affiliation (workplace.relations[], both ends contracted)
  5. HCP contacts + ts_department / ts_hospital / ts_hospitalgroup (INDIVIDUAL + main ACTIVITY)
  6. HCP–HCO ts_affiliation (all ACTIVITY packets, contracted ends)

Usage:
    python scripts/run_dynamic_sync.py --validate-only
    python scripts/run_dynamic_sync.py --dry-run --codbase WUK --max-records 10
    python scripts/run_dynamic_sync.py --codbase WUK
    python scripts/run_dynamic_sync.py --delta --date-from 2026-09-07 --date-to 2026-09-11
    python scripts/run_dynamic_sync.py --affiliations-only --codbase WUK
    python scripts/run_dynamic_sync.py --hco-only --codbase WUK
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dataverse import DataverseClient, DataverseConfig
from onekey import OneKeyClient, OneKeyConfig, OneKeySession
from integration import (
    create_sync_engine,
    load_mapping_config,
    setup_logging,
    MappingValidator,
)
from integration.job_trace import TRIGGER_BY_NAME
from integration.env import REPO_ROOT, load_env


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    parser.add_argument(
        "--delta",
        action="store_true",
        help="Run delta/CDC sync instead of full sync",
    )
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Only validate configuration, don't sync",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate and log but don't write to Dataverse",
    )

    parser.add_argument(
        "--codbase",
        default="WUK",
        help="Comma-separated OneKey codebases (default: WUK)",
    )

    parser.add_argument(
        "--date-from",
        help="Start date for delta sync (ISO: YYYY-MM-DD). Defaults to watermark or yesterday.",
    )
    parser.add_argument(
        "--date-to",
        help="End date for delta sync (ISO: YYYY-MM-DD). Defaults to today.",
    )

    parser.add_argument(
        "--max-records",
        type=int,
        help="Limit records per entity type (for testing)",
    )
    parser.add_argument(
        "--skip-address",
        action="store_true",
        help="Skip customeraddress phase",
    )
    parser.add_argument(
        "--skip-hierarchy",
        action="store_true",
        help="Skip parentaccountid hierarchy link phase",
    )
    parser.add_argument(
        "--skip-relations",
        action="store_true",
        help="Skip HCO–HCO ts_affiliation sync (workplace.relations[])",
    )
    parser.add_argument(
        "--skip-activity-affiliations",
        action="store_true",
        help="Skip HCP–HCO ts_affiliation sync (ACTIVITY packets)",
    )
    parser.add_argument(
        "--skip-picklists",
        action="store_true",
        help="Skip Phase 0 picklist upsert from value_mapping.csv",
    )
    parser.add_argument(
        "--affiliations-only",
        action="store_true",
        help=(
            "Only upsert ts_affiliation from contracted refreshMyOnekey IDs "
            "(HCO-HCO relations + HCP-HCO activities); no account/contact writes"
        ),
    )
    parser.add_argument(
        "--hco-only",
        action="store_true",
        help=(
            "Sync HCO path only (picklists, accounts, customeraddress, "
            "hierarchy, relations); skip HCP. Address/hierarchy/relations/"
            "picklists still run unless --skip-* is passed"
        ),
    )
    parser.add_argument(
        "--hcp-only",
        action="store_true",
        help="Sync only HCP/contacts (skip accounts/address/hierarchy)",
    )
    parser.add_argument(
        "--bulk-batch-size",
        type=int,
        default=None,
        help="Records per Dataverse CreateMultiple/UpdateMultiple request",
    )
    parser.add_argument(
        "--config-dir",
        help="Path to config directory with CSV mapping files",
    )
    parser.add_argument(
        "--watermark-dir",
        type=Path,
        default=REPO_ROOT / "logs",
        help="Directory for delta watermarks (default: logs/)",
    )

    parser.add_argument(
        "--trigger",
        choices=sorted(TRIGGER_BY_NAME),
        default="manual",
        help="How this job was started (stored on the integration job row)",
    )
    parser.add_argument(
        "--retry-of-job",
        help="ts_jobid of an earlier integration job this run retries",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable verbose logging"
    )
    parser.add_argument(
        "--quiet", "-q", action="store_true", help="Suppress progress logs"
    )

    return parser.parse_args()


def _watermark_path(directory: Path, codebase: str) -> Path:
    return Path(directory) / f"sync_watermark_{codebase.upper()}.json"


def load_watermark(directory: Path, codebase: str) -> str | None:
    path = _watermark_path(directory, codebase)
    if not path.is_file():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        return data.get("date_to")
    except (OSError, json.JSONDecodeError):
        return None


def save_watermark(directory: Path, codebase: str, date_to: str) -> None:
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    path = _watermark_path(directory, codebase)
    path.write_text(
        json.dumps(
            {
                "codbase": codebase.upper(),
                "date_to": date_to,
                "saved_at": datetime.now(timezone.utc).isoformat(),
            },
            indent=2,
        ),
        encoding="utf-8",
    )


class MockDataverse:
    """Minimal stand-in for dry-run without Dataverse credentials."""

    def upsert_by_field(self, *args, **kwargs):
        return ("mock-guid", True)

    def bulk_upsert_by_field(self, entity_set, match_field, records, **kwargs):
        return [
            (str(r.get(match_field)), f"mock-{i}", True)
            for i, r in enumerate(records)
        ]

    def find_one(self, *args, **kwargs):
        return None

    def find_many_by_field(self, *args, **kwargs):
        return {}

    def retrieve_multiple(self, *args, **kwargs):
        # The pre-sync schema check reads one row per table; without this
        # every dry run failed validation and stopped before any phase.
        return []

    def update_multiple(self, *args, **kwargs):
        return None

    def update(self, *args, **kwargs):
        return None

    def whoami(self):
        return {"UserId": "dry-run"}


def _print_entity(label: str, stats: dict | None) -> None:
    if not stats:
        return
    print(f"\n{label}:")
    print(f"  Processed: {stats.get('processed', 0)}")
    print(f"  Created:   {stats.get('created', 0)}")
    print(f"  Updated:   {stats.get('updated', 0)}")
    print(f"  Failed:    {stats.get('failed', 0)}")


def main():
    args = parse_args()

    log_level = (
        logging.DEBUG
        if args.verbose
        else (logging.WARNING if args.quiet else logging.INFO)
    )
    setup_logging(level=log_level)
    logger = logging.getLogger(__name__)

    load_env()

    codebases = [c.strip().upper() for c in args.codbase.split(",") if c.strip()]
    sync_addresses = not args.skip_address
    sync_hierarchy = not args.skip_hierarchy
    sync_hco_relations = not args.skip_relations
    sync_activity_affiliations = not args.skip_activity_affiliations
    sync_picklists = not args.skip_picklists

    config_dir = Path(args.config_dir) if args.config_dir else None
    try:
        config = load_mapping_config(config_dir)
        logger.info(
            "Loaded mapping configuration from %s",
            config_dir or "default location",
        )
    except FileNotFoundError as e:
        logger.error("Configuration not found: %s", e)
        return 1

    if args.validate_only:
        logger.info("Running validation only...")
        validator = MappingValidator(config)
        result = validator.validate_mapping_config()
        result.log_all()
        print(f"\nValidation: {result.summary()}")
        return 0 if result.is_valid else 1

    onekey_config = OneKeyConfig.from_env()
    logger.info(
        "Connecting to OneKey: %s (connector %s)",
        onekey_config.base_url,
        onekey_config.connector_id,
    )
    onekey = OneKeyClient(OneKeySession(onekey_config))

    if not args.dry_run:
        dataverse_config = DataverseConfig.from_env()
        logger.info("Connecting to Dataverse: %s", dataverse_config.environment_url)
        dataverse = DataverseClient(dataverse_config)
        identity = dataverse.whoami()
        logger.info("Dataverse user: %s", identity.get("UserId"))
    else:
        logger.info("DRY RUN MODE - No Dataverse writes")
        dataverse = MockDataverse()

    engine = create_sync_engine(
        onekey, dataverse, args.config_dir, bulk_batch_size=args.bulk_batch_size
    )
    engine.tracer.trigger = TRIGGER_BY_NAME[args.trigger]
    engine.tracer.retry_of_job_id = args.retry_of_job

    try:
        if args.delta:
            date_from = args.date_from
            date_to = args.date_to or datetime.now().strftime("%Y-%m-%d")
            if not date_from:
                # Prefer earliest watermark across codebases, else yesterday − 1h overlap.
                marks = [
                    load_watermark(args.watermark_dir, cb) for cb in codebases
                ]
                marks = [m for m in marks if m]
                if marks:
                    date_from = min(marks)
                    # 1-hour overlap: rewind calendar day if time present, else keep date.
                    logger.info("Using watermark date_from=%s", date_from)
                else:
                    yesterday = datetime.now() - timedelta(days=1)
                    date_from = yesterday.strftime("%Y-%m-%d")

            logger.info("Starting delta sync: %s to %s", date_from, date_to)
            result = engine.sync_delta(
                codebases=codebases,
                date_from=f"{date_from}T00:00:00Z"
                if "T" not in date_from
                else date_from,
                date_to=f"{date_to}T23:59:59Z" if "T" not in date_to else date_to,
                dry_run=args.dry_run,
                sync_addresses=sync_addresses,
                sync_hierarchy=sync_hierarchy,
                sync_hco_relations=sync_hco_relations,
                sync_activity_affiliations=sync_activity_affiliations,
                sync_picklists=sync_picklists,
            )
            if result.get("success") and not args.dry_run:
                for cb in codebases:
                    save_watermark(args.watermark_dir, cb, date_to)
        else:
            if args.affiliations_only:
                logger.info(
                    "Starting affiliations-only sync (contracted IDs) for: %s",
                    ", ".join(codebases),
                )
                result = engine.sync_affiliations_only(
                    codebases=codebases,
                    max_records=args.max_records,
                    dry_run=args.dry_run,
                )
            else:
                logger.info("Starting full sync for: %s", ", ".join(codebases))
                result = engine.sync_full(
                    codebases=codebases,
                    max_records=args.max_records,
                    dry_run=args.dry_run,
                    hco_only=args.hco_only,
                    hcp_only=args.hcp_only,
                    sync_addresses=sync_addresses,
                    sync_hierarchy=sync_hierarchy,
                    sync_hco_relations=sync_hco_relations,
                    sync_activity_affiliations=sync_activity_affiliations,
                    sync_picklists=sync_picklists,
                )

        print("\n" + "=" * 60)
        print("SYNC SUMMARY")
        print("=" * 60)
        print(f"Success: {result.get('success', False)}")
        if result.get("job_id"):
            print(f"Job ID:  {result['job_id']} ({result.get('trace_status', '')})")
        print(f"Codebases: {', '.join(codebases)}")
        if result.get("mode") == "affiliations_only":
            print("Mode: affiliations-only (contracted IDs)")
        else:
            print(
                f"Phases: picklists={'on' if sync_picklists else 'off'} "
                f"address={'on' if sync_addresses else 'off'} "
                f"hierarchy={'on' if sync_hierarchy else 'off'} "
                f"hco_relations={'on' if sync_hco_relations else 'off'} "
                f"activity_aff={'on' if sync_activity_affiliations else 'off'}"
            )

        if result.get("picklists"):
            print(f"\nPicklists: {len(result['picklists'])} type(s) processed")

        _print_entity("Accounts", result.get("accounts"))
        _print_entity("Addresses (customeraddress)", result.get("addresses"))

        hier = result.get("hierarchy") or {}
        if hier:
            print("\nHierarchy:")
            print(f"  Edges:           {hier.get('edges', 0)}")
            print(f"  Linked:          {hier.get('linked', 0)}")
            print(f"  Already linked:  {hier.get('already_linked', 0)}")
            print(f"  Missing child:   {hier.get('missing_child', 0)}")
            print(f"  Missing parent:  {hier.get('missing_parent', 0)}")
            print(f"  Failed:          {hier.get('failed', 0)}")

        _print_entity("Affiliations (HCO-HCO)", result.get("affiliations"))
        _print_entity(
            "Affiliations (HCP-HCO activity)",
            result.get("activity_affiliations"),
        )

        _print_entity("Contacts", result.get("contacts"))

        if result.get("by_process_code"):
            pc = result["by_process_code"]
            print(f"\nCDC processCode: I={pc.get('I', 0)} U={pc.get('U', 0)} R={pc.get('R', 0)}")

        gaps = result.get("picklist_gaps") or 0
        if gaps:
            print(f"\nPicklist gaps: {gaps}")
            if result.get("picklist_gap_report"):
                print(f"  Report: {result['picklist_gap_report']}")

        if result.get("errors"):
            print(f"\nErrors: {len(result['errors'])}")
            for error in result["errors"][:5]:
                print(f"  - {error}")
            if len(result["errors"]) > 5:
                print(f"  ... and {len(result['errors']) - 5} more")

        print("=" * 60)
        return 0 if result.get("success") else 1

    finally:
        onekey.close()
        if hasattr(dataverse, "close"):
            dataverse.close()


if __name__ == "__main__":
    sys.exit(main())
