"""HCP–HCO Activity → ts_affiliation helpers (AFF-* mappings)."""

from __future__ import annotations

from typing import Any


def _tih_role_codes(activity: dict) -> list[str]:
    """Collect role codes from activity.ada TIH_* entries (order by rank/key)."""
    ada = activity.get("ada") or {}
    entries: list[tuple[int, str, str]] = []
    for key, value in ada.items():
        if not isinstance(value, dict):
            continue
        list_code = (value.get("listCode") or key.split(",", 1)[0] or "").upper()
        if not list_code.startswith("TIH"):
            continue
        code = value.get("code")
        if not code:
            continue
        rank = value.get("rank")
        try:
            sort_rank = int(rank) if rank is not None else 99
        except (TypeError, ValueError):
            sort_rank = 99
        entries.append((sort_rank, str(key), str(code)))
    entries.sort()
    return [code for _, _, code in entries]


def flatten_activity_for_affiliation(record: dict) -> dict[str, Any]:
    """Flatten one ACTIVITY search/population row for AFF-* transforms.

    Returns keys expected by ``entity_mapping.csv`` AFF rows plus ``roleCodes``
    for multi-select ``ts_role``.
    """
    activity = record.get("activity") or {}
    individual = record.get("individual") or {}
    workplace = record.get("workplace") or {}

    activity_eid = activity.get("activityEid") or record.get("onekeyEid")
    individual_eid = individual.get("individualEid") or activity.get("individualEid")
    workplace_eid = workplace.get("workplaceEid") or activity.get("workplaceEid")
    role_codes = _tih_role_codes(activity)

    # Populate AFF-006..015 source_field names (role, ROLE_2, …)
    role_fields: dict[str, str] = {}
    if role_codes:
        role_fields["role"] = role_codes[0]
        for index, code in enumerate(role_codes[1:10], start=2):
            role_fields[f"ROLE_{index}"] = code

    first = individual.get("firstName") or ""
    last = individual.get("lastName") or ""
    person = f"{first} {last}".strip() or individual_eid or ""
    workplace_name = (
        workplace.get("usualName")
        or workplace.get("officialName")
        or workplace_eid
        or ""
    )
    display = f"{person} @ {workplace_name}".strip(" @") or activity_eid

    return {
        "activityEid": activity_eid,
        "individualEid": individual_eid,
        "workplaceEid": workplace_eid,
        "isMainActivity": activity.get("isMainActivity"),
        "workingStatusCode": activity.get("workingStatusCode"),
        "stateCode": activity.get("stateCode") or activity.get("statusCode"),
        "statusCode": activity.get("statusCode"),
        "activityStartDate": activity.get("activityStartDate"),
        "activityEndDate": activity.get("activityEndDate"),
        "roleCodes": role_codes,
        "ts_name": display,
        "firstName": first,
        "lastName": last,
        "workplaceName": workplace_name,
        **role_fields,
    }
