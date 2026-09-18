#!/usr/bin/env python3
"""Link HCO parent/child hierarchy in Dataverse via bulk UpdateMultiple.

Reads workplace -> managerWorkplaceEid edges from OneKey, resolves both sides
to Dataverse account GUIDs (by cr4ec_externalid), then sets
parentaccountid@odata.bind in batches.

Usage:
    python scripts/link_hco_hierarchy.py --codbase WUK
    python scripts/link_hco_hierarchy.py --codbase WUK --dry-run
    python scripts/link_hco_hierarchy.py --codbase WUK --bulk-batch-size 100
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dataverse import DataverseClient, DataverseConfig
from integration import setup_logging
from integration.env import load_env
from integration.hierarchy import apply_updates, build_updates, edges_from_hcos, resolve_accounts
from onekey import OneKeyClient, OneKeyConfig, OneKeySession
from onekey.parse import flatten_hco

logger = logging.getLogger("link_hco_hierarchy")
EDGE_CACHE = Path(__file__).resolve().parent.parent / "logs" / "hco_hierarchy_edges_WUK.json"


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--codbase", default="WUK")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--bulk-batch-size",
        type=int,
        default=None,
        help="UpdateMultiple batch size (default: DATAVERSE_BULK_BATCH_SIZE or 100)",
    )
    parser.add_argument(
        "--skip-unchanged",
        action="store_true",
        default=True,
        help="Skip rows already linked to the correct parent (default: on)",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Rewrite parent even when already correct",
    )
    parser.add_argument(
        "--edges-cache",
        type=Path,
        default=EDGE_CACHE,
        help="JSON cache for child->parent edges (written after OneKey extract)",
    )
    parser.add_argument(
        "--from-cache",
        action="store_true",
        help="Skip OneKey extract; use --edges-cache only",
    )
    return parser.parse_args()


def iter_workplaces(client: OneKeyClient, codebase: str):
    """Yield flattened workplaces; rate-limit retries stay on the same call stack
    via OneKeySession (long 429 backoff). Do not restart the population
    transaction from scratch — that amplifies throttling.
    """
    for record in client.iter_population(cod_base=codebase, page_size=200):
        if (record.get("entityType") or "").upper() == "WORKPLACE":
            yield flatten_hco(record)


def collect_edges(client: OneKeyClient, codebase: str) -> list[tuple[str, str]]:
    """Return (child_eid, parent_eid) pairs from OneKey workplaces."""
    flattened = list(iter_workplaces(client, codebase))
    edges = edges_from_hcos(flattened)
    logger.info(
        "OneKey %s: %d workplaces, %d parent edges",
        codebase, len(flattened), len(edges),
    )
    return edges


def save_edges(path: Path, edges: list[tuple[str, str]], codebase: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "codbase": codebase,
        "edge_count": len(edges),
        "edges": [{"child": c, "parent": p} for c, p in edges],
    }
    path.write_text(json.dumps(payload), encoding="utf-8")
    logger.info("Cached %d edges to %s", len(edges), path)


def load_edges(path: Path) -> list[tuple[str, str]]:
    data = json.loads(path.read_text(encoding="utf-8"))
    edges = [(e["child"], e["parent"]) for e in data.get("edges", [])]
    logger.info("Loaded %d edges from cache %s", len(edges), path)
    return edges


def main():
    args = parse_args()
    setup_logging(level=logging.INFO)
    load_env()

    skip_unchanged = args.skip_unchanged and not args.force

    dataverse = None
    if not args.dry_run:
        dataverse = DataverseClient(DataverseConfig.from_env())
        logger.info("Dataverse user: %s", dataverse.whoami().get("UserId"))
        batch_size = args.bulk_batch_size or dataverse.config.bulk_batch_size
    else:
        batch_size = args.bulk_batch_size or 100
        logger.info("DRY RUN — no Dataverse writes")

    logger.info("Extracting HCO hierarchy edges from OneKey %s...", args.codbase)
    if args.from_cache:
        if not args.edges_cache.exists():
            logger.error("Cache not found: %s", args.edges_cache)
            return 1
        edges = load_edges(args.edges_cache)
    else:
        onekey = OneKeyClient(OneKeySession(OneKeyConfig.from_env()))
        edges = collect_edges(onekey, args.codbase)
        onekey.close()
        if edges:
            save_edges(args.edges_cache, edges, args.codbase)

    if not edges:
        logger.info("No parent edges found; nothing to link")
        return 0

    eids = sorted({eid for pair in edges for eid in pair})
    logger.info("Resolving %d cr4ec_externalid values in Dataverse...", len(eids))

    if args.dry_run:
        # Still need live lookups to report readiness unless mocked.
        dataverse = DataverseClient(DataverseConfig.from_env())
        logger.info("Dataverse user: %s", dataverse.whoami().get("UserId"))

    accounts = resolve_accounts(dataverse, eids)
    logger.info("Resolved %d/%d accounts", len(accounts), len(eids))

    updates, stats = build_updates(edges, accounts, skip_unchanged=skip_unchanged)
    logger.info(
        "Link plan: ready=%d already_linked=%d missing_child=%d "
        "missing_parent=%d (unique parents missing=%d) self_ref=%d",
        stats["ready"],
        stats["already_linked"],
        stats["missing_child"],
        stats["missing_parent"],
        stats["missing_parent_unique"],
        stats["self_ref"],
    )

    if args.dry_run:
        for row in updates[:10]:
            logger.info(
                "DRY RUN would set %s -> %s",
                row["cr4ec_externalid"],
                row["parentaccountid@odata.bind"],
            )
        if len(updates) > 10:
            logger.info("... and %d more", len(updates) - 10)
        dataverse.close()
        return 0

    if not updates:
        logger.info("Nothing to update")
        dataverse.close()
        return 0

    logger.info(
        "Applying %d parent links via UpdateMultiple (batch size %d)...",
        len(updates), batch_size,
    )
    updated, failed = apply_updates(dataverse, updates, batch_size)
    logger.info("Done: %d linked, %d failed", updated, failed)
    dataverse.close()
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
