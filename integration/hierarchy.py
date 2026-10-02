"""HCO parentaccountid linking helpers (managerWorkplaceEid → parent)."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from dataverse.client import chunked
from dataverse.exceptions import DataverseHTTPError

if TYPE_CHECKING:
    from dataverse import DataverseClient

logger = logging.getLogger(__name__)


def edges_from_hcos(
    hcos: list[dict],
    allowed_workplace_eids: set[str] | None = None,
) -> list[tuple[str, str]]:
    """Return (child_eid, parent_eid) from flattened HCO rows.

    When ``allowed_workplace_eids`` is provided (contracted population from
    ``refreshMyOnekey``), skip edges whose parent (or child) is not in that
    set — do not chase out-of-contract parents via search.
    """
    edges = []
    for hco in hcos:
        child = hco.get("workplaceEid")
        parent = hco.get("parentWorkplaceEid")
        if not child or not parent:
            continue
        if child == parent:
            continue
        child_s, parent_s = str(child), str(parent)
        if allowed_workplace_eids is not None and (
            child_s not in allowed_workplace_eids
            or parent_s not in allowed_workplace_eids
        ):
            continue
        edges.append((child_s, parent_s))
    return edges


def resolve_accounts(
    dataverse: "DataverseClient",
    eids: list[str],
    match_field: str,
) -> dict[str, dict]:
    """Map upsert-key value → account row (includes current parent)."""
    return dataverse.find_many_by_field(
        "accounts",
        match_field,
        eids,
        select=[match_field, "accountid", "_parentaccountid_value"],
        chunk_size=50,
    )


def build_updates(
    edges: list[tuple[str, str]],
    accounts: dict[str, dict],
    match_field: str,
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
    missing_child_eids: list[str] = []
    missing_parent_eids: list[str] = []

    for child_eid, parent_eid in edges:
        child = accounts.get(child_eid)
        parent = accounts.get(parent_eid)
        if not child or not child.get("accountid"):
            stats["missing_child"] += 1
            missing_child_eids.append(child_eid)
            continue
        if not parent or not parent.get("accountid"):
            stats["missing_parent"] += 1
            missing_parent_eids.append(child_eid)
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
                match_field: child_eid,
                "parentaccountid@odata.bind": f"/accounts({parent_id})",
            }
        )
        stats["ready"] += 1

    stats["missing_parent_unique"] = len(missing_parents)
    stats["missing_child_eids"] = missing_child_eids
    stats["missing_parent_eids"] = missing_parent_eids
    return updates, stats


def apply_updates(
    dataverse: "DataverseClient",
    updates: list[dict],
    batch_size: int,
    match_field: str | None = None,
    failures: list | None = None,
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
                "Hierarchy progress: batch=%d linked=%d done=%d total=%d",
                i,
                len(batch),
                updated,
                total,
            )
        except DataverseHTTPError as exc:
            logger.warning(
                "UpdateMultiple failed for hierarchy batch %d, linking one by one: "
                "records=%d status=%s error=%r",
                i,
                len(batch),
                exc.status_code,
                str(exc)[:300],
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
                    label = (
                        row.get(match_field)
                        if match_field
                        else row.get("accountid")
                    )
                    logger.error(
                        "Account %s failed (relate parent): error=%r",
                        label,
                        str(single_exc)[:500],
                    )
                    if failures is not None:
                        failures.append((str(label) if label else "", single_exc))
    return updated, failed
