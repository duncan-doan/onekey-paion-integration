"""Contracted OneKey ID helpers (``/pop/refreshMyOnekey`` population only).

Search (``/ok/search`` / ``get_by_id``) can return entities outside the
connector subscription. Migration and sync must process only EIDs present in
the population packets — never enrich missing related IDs via search.
"""

from __future__ import annotations

from typing import Iterable


def workplace_eid_from_record(record: dict) -> str | None:
    """Extract workplaceEid from a population or search WORKPLACE row."""
    workplace = record.get("workplace") or record
    eid = workplace.get("workplaceEid") or record.get("onekeyEid")
    return str(eid) if eid else None


def individual_eid_from_record(record: dict) -> str | None:
    individual = record.get("individual") or record
    eid = individual.get("individualEid") or record.get("onekeyEid")
    return str(eid) if eid else None


def activity_eid_from_record(record: dict) -> str | None:
    activity = record.get("activity") or record
    eid = activity.get("activityEid") or record.get("onekeyEid")
    return str(eid) if eid else None


def index_population_eids(records: Iterable[dict]) -> dict[str, set[str]]:
    """Build contracted EID sets keyed by entity type from population rows.

    Returns:
        ``{"WORKPLACE": {...}, "INDIVIDUAL": {...}, "ACTIVITY": {...}}``
    """
    out: dict[str, set[str]] = {
        "WORKPLACE": set(),
        "INDIVIDUAL": set(),
        "ACTIVITY": set(),
    }
    for record in records:
        entity_type = (record.get("entityType") or "").upper()
        if entity_type == "WORKPLACE":
            eid = workplace_eid_from_record(record)
            if eid:
                out["WORKPLACE"].add(eid)
        elif entity_type == "INDIVIDUAL":
            eid = individual_eid_from_record(record)
            if eid:
                out["INDIVIDUAL"].add(eid)
        elif entity_type == "ACTIVITY":
            eid = activity_eid_from_record(record)
            if eid:
                out["ACTIVITY"].add(eid)
    return out


def collect_contracted_workplace_eids(records: Iterable[dict]) -> set[str]:
    """Return workplace EIDs seen in population WORKPLACE packets."""
    return index_population_eids(records)["WORKPLACE"]


def both_in_contract(a: str | None, b: str | None, allowed: set[str] | None) -> bool:
    """True when both EIDs are allowed (or no allowlist is enforced)."""
    if allowed is None:
        return bool(a and b)
    return bool(a and b and a in allowed and b in allowed)
