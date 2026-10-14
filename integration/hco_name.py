"""Account name for a workplace that has neither official nor usual name.

The name is workplace type, then postal city and code, then one HCP who
works there. Several HCPs resolve to one: a main activity wins, and a
remaining tie uses the alphabetically first name.
"""

from __future__ import annotations


def individual_name(individual: dict | None) -> str:
    """``firstName lastName``, omitting whichever part is blank."""
    individual = individual or {}
    first = str(individual.get("firstName") or "").strip()
    last = str(individual.get("lastName") or "").strip()
    return " ".join(part for part in (first, last) if part)


def hcp_name_by_workplace(individuals, activities) -> dict[str, str]:
    """One HCP display name per workplace EID."""
    by_eid: dict[str, dict] = {}
    for record in individuals or []:
        individual = record.get("individual") or record
        eid = str(individual.get("individualEid") or "").strip()
        if eid:
            by_eid[eid] = individual

    chosen: dict[str, tuple] = {}
    for record in activities or []:
        activity = record.get("activity") or {}
        workplace = record.get("workplace") or {}
        workplace_eid = str(
            workplace.get("workplaceEid") or activity.get("workplaceEid") or ""
        ).strip()
        if not workplace_eid:
            continue
        nested = record.get("individual") or {}
        individual_eid = str(
            nested.get("individualEid") or activity.get("individualEid") or ""
        ).strip()
        individual = nested if individual_name(nested) else by_eid.get(individual_eid, {})
        name = individual_name(individual)
        if not name:
            continue
        rank = (
            0 if activity.get("isMainActivity") else 1,
            name.casefold(),
            individual_eid,
        )
        current = chosen.get(workplace_eid)
        if current is None or rank < current[0]:
            chosen[workplace_eid] = (rank, name)
    return {eid: name for eid, (rank, name) in chosen.items()}


def nameless_account_name(source: dict, limit: int = 160) -> str:
    """Build the account name, or ``noname`` when every part is missing."""
    workplace_type = str(source.get("typeLabel") or source.get("typeCode") or "").strip()
    city = str(source.get("address_city") or "").strip()
    postal = str(source.get("address_postalCode") or "").strip()
    place = " ".join(part for part in (city, postal) if part)
    hcp = str(source.get("hcpName") or "").strip()
    built = " ".join(part for part in (workplace_type, place, hcp) if part)
    return (built or "noname")[:limit]
