#!/usr/bin/env python3
"""Run dynamic sync using CSV-based mapping configuration.

This script demonstrates the new dynamic sync engine that uses CSV files
for field mapping, validation, and comprehensive logging.

Usage:
    # Full sync with validation (dry run)
    python scripts/run_dynamic_sync.py --dry-run --codbase WUK
    
    # Full sync to Dataverse
    python scripts/run_dynamic_sync.py --codbase WUK --max-records 100
    
    # Delta sync for specific date range
    python scripts/run_dynamic_sync.py --delta --date-from 2026-09-07 --date-to 2026-09-11
    
    # Validate configuration only
    python scripts/run_dynamic_sync.py --validate-only

Examples:
    # Test with small sample
    python scripts/run_dynamic_sync.py --codbase WUK --max-records 10 --dry-run
    
    # Production full sync
    python scripts/run_dynamic_sync.py --codbase WUK,WDE,WNL --with-address
"""

import argparse
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dataverse import DataverseClient, DataverseConfig
from onekey import OneKeyClient, OneKeyConfig, OneKeySession
from integration import (
    create_sync_engine,
    load_mapping_config,
    setup_logging,
    MappingValidator,
    DataverseValidator,
)
from integration.env import load_env


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    
    # Sync mode
    parser.add_argument(
        "--delta", action="store_true",
        help="Run delta/CDC sync instead of full sync"
    )
    parser.add_argument(
        "--validate-only", action="store_true",
        help="Only validate configuration, don't sync"
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Validate and log but don't write to Dataverse"
    )
    
    # Codebase selection
    parser.add_argument(
        "--codbase", default="WUK",
        help="Comma-separated OneKey codebases (default: WUK)"
    )
    
    # Delta sync dates
    parser.add_argument(
        "--date-from",
        help="Start date for delta sync (ISO format: YYYY-MM-DD)"
    )
    parser.add_argument(
        "--date-to",
        help="End date for delta sync (ISO format: YYYY-MM-DD)"
    )
    
    # Options
    parser.add_argument(
        "--max-records", type=int,
        help="Limit records per entity type (for testing)"
    )
    parser.add_argument(
        "--with-address", action="store_true",
        help="Include address fields (may fail due to Dataverse constraints)"
    )
    parser.add_argument(
        "--hco-only", action="store_true",
        help="Sync only HCO/accounts (skip HCP/contacts)"
    )
    parser.add_argument(
        "--hcp-only", action="store_true",
        help="Sync only HCP/contacts (skip HCO/accounts)"
    )
    parser.add_argument(
        "--bulk-batch-size", type=int, default=None,
        help=(
            "Records per Dataverse CreateMultiple/UpdateMultiple request "
            "(default: DATAVERSE_BULK_BATCH_SIZE or 100; Microsoft suggests 100–1000)"
        ),
    )
    parser.add_argument(
        "--config-dir",
        help="Path to config directory with CSV mapping files"
    )
    
    # Logging
    parser.add_argument(
        "--verbose", "-v", action="store_true",
        help="Enable verbose logging"
    )
    parser.add_argument(
        "--quiet", "-q", action="store_true",
        help="Suppress progress logs"
    )
    
    return parser.parse_args()


def main():
    args = parse_args()
    
    # Setup logging
    log_level = logging.DEBUG if args.verbose else (logging.WARNING if args.quiet else logging.INFO)
    setup_logging(level=log_level)
    
    logger = logging.getLogger(__name__)
    
    # Load environment
    load_env()
    
    # Parse codebases
    codebases = [c.strip().upper() for c in args.codbase.split(",")]
    
    # Load mapping configuration
    config_dir = Path(args.config_dir) if args.config_dir else None
    try:
        config = load_mapping_config(config_dir)
        logger.info("Loaded mapping configuration from %s", config_dir or "default location")
    except FileNotFoundError as e:
        logger.error("Configuration not found: %s", e)
        return 1
    
    # Validate-only mode
    if args.validate_only:
        logger.info("Running validation only...")
        validator = MappingValidator(config)
        result = validator.validate_mapping_config()
        result.log_all()
        print(f"\nValidation: {result.summary()}")
        return 0 if result.is_valid else 1
    
    # Connect to OneKey
    onekey_config = OneKeyConfig.from_env()
    logger.info("Connecting to OneKey: %s (connector %s)", 
                onekey_config.base_url, onekey_config.connector_id)
    
    onekey = OneKeyClient(OneKeySession(onekey_config))
    
    # Connect to Dataverse (unless dry run)
    dataverse = None
    if not args.dry_run:
        dataverse_config = DataverseConfig.from_env()
        logger.info("Connecting to Dataverse: %s", dataverse_config.environment_url)
        dataverse = DataverseClient(dataverse_config)
        
        # Verify connection
        identity = dataverse.whoami()
        logger.info("Dataverse user: %s", identity.get("UserId"))
    else:
        logger.info("DRY RUN MODE - No Dataverse connection")
        # Create a mock client for dry run
        class MockDataverse:
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
        dataverse = MockDataverse()
    
    # Create sync engine
    engine = create_sync_engine(
        onekey, dataverse, args.config_dir, bulk_batch_size=args.bulk_batch_size
    )
    
    # Run sync
    try:
        if args.delta:
            # Delta sync
            date_from = args.date_from
            date_to = args.date_to
            
            # Default to yesterday if not specified
            if not date_from:
                yesterday = datetime.now() - timedelta(days=1)
                date_from = yesterday.strftime("%Y-%m-%d")
            if not date_to:
                date_to = datetime.now().strftime("%Y-%m-%d")
            
            logger.info("Starting delta sync: %s to %s", date_from, date_to)
            result = engine.sync_delta(
                codebases=codebases,
                date_from=f"{date_from}T00:00:00Z",
                date_to=f"{date_to}T23:59:59Z",
                include_address=args.with_address,
                dry_run=args.dry_run,
            )
        else:
            # Full sync
            logger.info("Starting full sync for: %s", ", ".join(codebases))
            result = engine.sync_full(
                codebases=codebases,
                include_address=args.with_address,
                max_records=args.max_records,
                dry_run=args.dry_run,
                hco_only=args.hco_only,
                hcp_only=args.hcp_only,
            )
        
        # Print summary
        print("\n" + "=" * 60)
        print("SYNC SUMMARY")
        print("=" * 60)
        print(f"Success: {result.get('success', False)}")
        print(f"Codebases: {', '.join(codebases)}")
        
        if "accounts" in result:
            acc = result["accounts"]
            print(f"\nAccounts:")
            print(f"  Processed: {acc['processed']}")
            print(f"  Created:   {acc['created']}")
            print(f"  Updated:   {acc['updated']}")
            print(f"  Failed:    {acc['failed']}")
        
        if "contacts" in result:
            con = result["contacts"]
            print(f"\nContacts:")
            print(f"  Processed: {con['processed']}")
            print(f"  Created:   {con['created']}")
            print(f"  Updated:   {con['updated']}")
            print(f"  Failed:    {con['failed']}")
        
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
