#!/usr/bin/env python3
"""Compare OneKey HCOs to Dataverse and import any missing accounts.

Identity key is the account upsert field from mapping config
(OneKey ``workplaceEid``).

Usage:
    python scripts/import_missing_hcos.py --codbase WUK
    python scripts/import_missing_hcos.py --codbase WUK --dry-run
    python scripts/import_missing_hcos.py --codbase WUK --from-cache logs/onekey_hco_eids_WUK.json
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dataverse import DataverseClient, DataverseConfig
from integration import create_sync_engine, load_mapping_config, setup_logging
from integration.env import load_env
from onekey import OneKeyClient, OneKeyConfig, OneKeySession
from onekey.parse import flatten_hco

logger = logging.getLogger("import_missing_hcos")
DEFAULT_CACHE = (
    Path(__file__).resolve().parent.parent / "logs" / "onekey_hco_eids_WUK.json"
)


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--codbase", default="WUK")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--bulk-batch-size", type=int, default=None)
    parser.add_argument(
        "--cache",
        type=Path,
        default=DEFAULT_CACHE,
        help="JSON cache of OneKey workplaces (written after extract)",
    )
    parser.add_argument(
        "--from-cache",
        action="store_true",
        help="Skip OneKey extract; load workplaces from --cache",
    )
    parser.add_argument(
        "--max-records",
        type=int,
        help="Limit OneKey extract (testing)",
    )
    return parser.parse_args()


def extract_workplaces(client: OneKeyClient, codebase: str, max_records=None):
    """Return list of flattened OneKey workplaces."""
    rows = []
    for record in client.iter_population(
        cod_base=codebase, page_size=500, max_records=max_records
    ):
        if (record.get("entityType") or "").upper() != "WORKPLACE":
            continue
        rows.append(flatten_hco(record))
    return rows


def load_cache(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    return data.get("workplaces") or []


def save_cache(path: Path, workplaces: list[dict], codebase: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "codbase": codebase,
        "count": len(workplaces),
        "workplaces": workplaces,
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    logger.info("Cached %d workplaces to %s", len(workplaces), path)


def load_dataverse_keys(dataverse: DataverseClient, account_key: str) -> set[str]:
    """All non-null upsert-key values currently on account."""
    rows = dataverse.retrieve_multiple(
        "accounts",
        select=[account_key],
        filter=f"{account_key} ne null",
    )
    return {
        str(row[account_key]).strip()
        for row in rows
        if row.get(account_key)
    }


def main():
    args = parse_args()
    setup_logging(level=logging.INFO)
    load_env()
    account_key = load_mapping_config().require_key_field("account")

    if args.from_cache:
        if not args.cache.exists():
            logger.error("Cache not found: %s", args.cache)
            return 1
        workplaces = load_cache(args.cache)
        logger.info("Loaded %d workplaces from cache %s", len(workplaces), args.cache)
        onekey = None
    else:
        onekey = OneKeyClient(OneKeySession(OneKeyConfig.from_env()))
        logger.info("Extracting HCOs from OneKey %s...", args.codbase)
        workplaces = extract_workplaces(onekey, args.codbase, args.max_records)
        logger.info("Extracted %d HCOs from OneKey %s", len(workplaces), args.codbase)
        save_cache(args.cache, workplaces, args.codbase)

    dataverse = DataverseClient(DataverseConfig.from_env())
    logger.info("Dataverse user: %s", dataverse.whoami().get("UserId"))
    existing = load_dataverse_keys(dataverse, account_key)
    logger.info(
        "Dataverse accounts with %s: %d", account_key, len(existing)
    )

    by_eid = {}
    for hco in workplaces:
        eid = (hco.get("workplaceEid") or "").strip()
        if eid:
            by_eid[eid] = hco

    onekey_eids = set(by_eid)
    missing = sorted(onekey_eids - existing)
    extra = sorted(existing - onekey_eids)

    logger.info("OneKey HCOs:     %d", len(onekey_eids))
    logger.info("Already in DV:  %d", len(onekey_eids & existing))
    logger.info("Missing in DV:  %d", len(missing))
    logger.info("In DV not OK:   %d (not deleted)", len(extra))
    if missing[:20]:
        logger.info("Sample missing: %s", missing[:20])

    if not missing:
        logger.info("Nothing to import")
        dataverse.close()
        if onekey:
            onekey.close()
        return 0

    engine = create_sync_engine(
        onekey or OneKeyClient(OneKeySession(OneKeyConfig.from_env())),
        dataverse,
        bulk_batch_size=args.bulk_batch_size,
    )
    key_name = account_key
    prepared = []
    for eid in missing:
        hco = by_eid[eid]
        parent_id = None
        parent_eid = hco.get("parentWorkplaceEid")
        if parent_eid:
            parent_id = engine._account_cache.get(parent_eid)
            if not parent_id:
                parent_id = engine._lookup_account(parent_eid)
        payload = engine.transformer.transform_hco(hco, parent_account_id=parent_id)
        if not payload.get(key_name):
            payload[key_name] = eid
        if not (payload.get("name") or "").strip():
            payload["name"] = "noname"
        prepared.append(payload)

    if args.dry_run:
        for payload in prepared[:10]:
            logger.info(
                "DRY RUN would create %s name=%s",
                payload.get(key_name),
                payload.get("name"),
            )
        if len(prepared) > 10:
            logger.info("... and %d more", len(prepared) - 10)
        dataverse.close()
        if onekey:
            onekey.close()
        return 0

    results = dataverse.bulk_upsert_by_field(
        "accounts",
        key_name,
        prepared,
        batch_size=engine.bulk_batch_size,
    )
    created = sum(1 for _, _, c in results if c)
    updated = sum(1 for _, _, c in results if not c)
    logger.info(
        "Import done: %d missing processed (%d created, %d updated)",
        len(results),
        created,
        updated,
    )

    dataverse.close()
    if onekey:
        onekey.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
