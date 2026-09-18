"""HCO parentaccountid linking helpers (managerWorkplaceEid → parent)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from dataverse.client import chunked
from dataverse.exceptions import DataverseHTTPError

if TYPE_CHECKING:
    from dataverse import DataverseClient

logger = logging.getLogger(__name__)


def edges_from_hcos(hcos: list[dict]) -> list[tuple[str, str]]:
    """Return (child_eid, parent_eid) from flattened HCO rows."""
    edges = []
    for hco in hcos:
        child = hco.get("workplaceEid")
        parent = hco.get("parentWorkplaceEid")
        if not child or not parent:
            continue
        if child == parent:
            continue
        edges.append((str(child), str(parent)))
    return edges


def resolve_accounts(
    dataverse: "DataverseClient", eids: list[str]
) -> dict[str, dict]:
    return dataverse.find_many_by_field(
        "accounts",
        "cr4ec_externalid",
        eids,
        select=["cr4ec_externalid", "accountid", "_parentaccountid_value"],
        chunk_size=50,
    )


def build_updates(
    edges: list[tuple[str, str]],
    accounts: dict[str, dict],
    skip_unchanged: bool = True,
) -> tuple[list[dict], dict]:
    stats = {
        "edges": len(edges),
        "ready": 0,
        "missing_child": 0,
        "missing_parent": 0,
        "already_linked": 0,
        "self_ref": 0,
    }
    updates = []
    missing_parents = set()

    for child_eid, parent_eid in edges:
        child = accounts.get(child_eid)
        parent = accounts.get(parent_eid)
        if not child or not child.get("accountid"):
            stats["missing_child"] += 1
            continue
        if not parent or not parent.get("accountid"):
            stats["missing_parent"] += 1
            missing_parents.add(parent_eid)
            continue

        child_id = child["accountid"]
        parent_id = parent["accountid"]
        if child_id == parent_id:
            stats["self_ref"] += 1
            continue

        current_parent = child.get("_parentaccountid_value")
        if (
            skip_unchanged
            and current_parent
            and str(current_parent).lower() == str(parent_id).lower()
        ):
            stats["already_linked"] += 1
            continue

        updates.append(
            {
                "accountid": child_id,
                "cr4ec_externalid": child_eid,
                "parentaccountid@odata.bind": f"/accounts({parent_id})",
            }
        )
        stats["ready"] += 1

    stats["missing_parent_unique"] = len(missing_parents)
    return updates, stats


def apply_updates(
    dataverse: "DataverseClient",
    updates: list[dict],
    batch_size: int,
) -> tuple[int, int]:
    updated = failed = 0
    total = len(updates)
    for i, batch in enumerate(chunked(updates, batch_size), 1):
        payloads = [
            {
                "accountid": row["accountid"],
                "parentaccountid@odata.bind": row["parentaccountid@odata.bind"],
            }
            for row in batch
        ]
        try:
            dataverse.update_multiple("accounts", payloads)
            updated += len(batch)
            logger.info(
                "Hierarchy UpdateMultiple batch %d: linked %d/%d (running %d)",
                i,
                len(batch),
                total,
                updated,
            )
        except DataverseHTTPError as exc:
            logger.warning(
                "Hierarchy batch %d failed (%s); falling back to PATCH",
                i,
                exc,
            )
            for row in batch:
                try:
                    dataverse.update(
                        "accounts",
                        row["accountid"],
                        {
                            "parentaccountid@odata.bind": row[
                                "parentaccountid@odata.bind"
                            ],
                        },
                    )
                    updated += 1
                except DataverseHTTPError as single_exc:
                    failed += 1
                    logger.error(
                        "Failed to link %s: %s",
                        row.get("cr4ec_externalid"),
                        single_exc,
                    )
    return updated, failed
