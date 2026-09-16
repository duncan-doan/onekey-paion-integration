#!/usr/bin/env python3
"""Insert HCO accounts that previously failed because officialName was empty.

Uses account.name = "noname" (via mapping default / transform fallback).
By default, targets EIDs from the last bulk HCO sync validation errors;
falls back to any WUK workplace missing both officialName and usualName.

Usage:
    python scripts/insert_hco_nameless.py
    python scripts/insert_hco_nameless.py --log logs/hco_bulk_wuk_20260915_160508.log
    python scripts/insert_hco_nameless.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import re
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dataverse import DataverseClient, DataverseConfig
from integration import create_sync_engine, setup_logging
from integration.env import load_env
from onekey import OneKeyClient, OneKeyConfig, OneKeySession
from onekey.exceptions import OneKeyRateLimitError
from onekey.parse import flatten_hco

FAILED_EID_RE = re.compile(
    r"\[VALIDATION\] HCO ([A-Z0-9]+):.*account\.name: Required field is missing"
)
DEFAULT_LOG = Path(__file__).resolve().parent.parent / "logs" / "hco_bulk_wuk_20260915_160508.log"


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--codbase", default="WUK")
    parser.add_argument(
        "--log",
        type=Path,
        default=DEFAULT_LOG,
        help="Sync log containing the validation failures",
    )
    parser.add_argument(
        "--all-nameless",
        action="store_true",
        help="Ignore log; insert every workplace missing officialName and usualName",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--bulk-batch-size", type=int, default=None)
    return parser.parse_args()


def load_failed_eids(log_path: Path) -> set[str]:
    if not log_path.exists():
        raise FileNotFoundError(f"Log not found: {log_path}")
    text = log_path.read_text(encoding="utf-8", errors="replace")
    return set(FAILED_EID_RE.findall(text))


def iter_workplaces(client: OneKeyClient, codebase: str):
    """Yield flattened workplaces, retrying briefly on OneKey 429."""
    attempts = 0
    while True:
        try:
            for record in client.iter_population(cod_base=codebase, page_size=500):
                if (record.get("entityType") or "").upper() == "WORKPLACE":
                    yield flatten_hco(record)
            return
        except OneKeyRateLimitError:
            attempts += 1
            if attempts > 6:
                raise
            delay = min(30 * attempts, 180)
            logging.warning("OneKey rate limited; sleeping %ss then retrying extract", delay)
            time.sleep(delay)


def main():
    args = parse_args()
    setup_logging(level=logging.INFO)
    logger = logging.getLogger("insert_hco_nameless")
    load_env()

    target_eids = None
    if not args.all_nameless:
        target_eids = load_failed_eids(args.log)
        logger.info("Loaded %d failed HCO EIDs from %s", len(target_eids), args.log)
        if not target_eids:
            logger.error("No failed HCO EIDs found in log")
            return 1

    onekey = OneKeyClient(OneKeySession(OneKeyConfig.from_env()))
    dataverse = None
    if not args.dry_run:
        dataverse = DataverseClient(DataverseConfig.from_env())
        logger.info("Dataverse user: %s", dataverse.whoami().get("UserId"))
    else:
        class MockDataverse:
            def bulk_upsert_by_field(self, entity_set, match_field, records, **kwargs):
                return [
                    (str(r.get(match_field)), f"mock-{i}", True)
                    for i, r in enumerate(records)
                ]
            def find_many_by_field(self, *a, **k):
                return {}
        dataverse = MockDataverse()

    engine = create_sync_engine(
        onekey, dataverse, bulk_batch_size=args.bulk_batch_size
    )
    key_field = engine.config.get_key_field("account")
    key_name = key_field.target_field if key_field else "accountnumber"

    prepared = []
    logger.info("Extracting nameless HCOs from OneKey %s...", args.codbase)
    for hco in iter_workplaces(onekey, args.codbase):
        eid = hco.get("workplaceEid")
        if target_eids is not None and eid not in target_eids:
            continue
        if target_eids is None:
            official = (hco.get("officialName") or "").strip()
            usual = (hco.get("usualName") or "").strip()
            if official or usual:
                continue

        parent_eid = hco.get("parentWorkplaceEid")
        parent_id = None
        if parent_eid:
            parent_id = engine._account_cache.get(parent_eid)
            if not parent_id and not args.dry_run:
                parent_id = engine._lookup_account(parent_eid)

        payload = engine.transformer.transform_hco(hco, False, parent_id)
        if not (payload.get("name") or "").strip():
            payload["name"] = "noname"
        prepared.append((eid, payload, hco))

    logger.info("Prepared %d nameless HCO payloads (name=noname)", len(prepared))
    if args.dry_run:
        for eid, payload, _ in prepared[:10]:
            logger.info("DRY RUN sample %s -> %s", eid, payload.get("name"))
        if len(prepared) > 10:
            logger.info("... and %d more", len(prepared) - 10)
        return 0

    if not prepared:
        logger.warning("Nothing to insert")
        return 0

    results = dataverse.bulk_upsert_by_field(
        "accounts",
        key_name,
        [p for _, p, _ in prepared],
        batch_size=engine.bulk_batch_size,
    )
    created = sum(1 for _, _, c in results if c)
    updated = sum(1 for _, _, c in results if not c)
    logger.info(
        "Done: %d processed (%d created, %d updated)",
        len(results), created, updated,
    )
    onekey.close()
    if hasattr(dataverse, "close"):
        dataverse.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
