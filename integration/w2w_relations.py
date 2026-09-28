"""HCO–HCO workplace relation helpers (OneKey relations[] → ts_affiliation).

OneKey stores each HCO–HCO link twice (once per workplace). Each copy becomes
its own affiliation row, pointing from the workplace that owns the
``relations[]`` entry to the related one, so a link seen from both sides
yields two rows with two keys. ``isParent`` still records which side is the
parent org (``childWorkplaceEid`` / ``parentWorkplaceEid``).
"""

from __future__ import annotations

from typing import Any

WORKPLACE_TO_WORKPLACE_LINK = "workplaceToWorkplaceLink"

# MAI ("Has for main workplace") repeats every HIE edge with the same child,
# parent and direction, so syncing it would add a second, non-primary
# ts_affiliation row for each hierarchy link.
#
# Verified for WUK only (2026-09-29): all 1,086 HIE links in the contracted
# population had an identical MAI link. In the complete relations, 90 of
# 24,246 parent->child links differed, all with children outside the
# population; one department has HIE and MAI pointing at different parents.
# HIE and MAI are separate OneKey links and can diverge in a later phase or
# another codebase (e.g. WDE). Before adding a codebase, or before each new
# phase, pull all its workplaces and re-run the HIE/MAI comparison; if they
# differ, MAI must be synced rather than ignored.
IGNORED_LINK_CODES = frozenset({"MAI"})


def normalize_hco_hco_edge(
    workplace_eid: str,
    relation: dict,
) -> tuple[str, str, str] | None:
    """Return canonical (child_eid, parent_eid, link_code) or None if invalid.

    Both sides of a link are accepted and produce the same tuple. ``isParent``
    true means the related workplace (``onekeyEid``) is the parent of
    ``workplace_eid``; false means it is the child. A relation whose
    ``isParent`` is missing or not a boolean is skipped rather than guessed,
    as are links in :data:`IGNORED_LINK_CODES`.

    Args:
        workplace_eid: The workplace that owns this ``relations[]`` entry.
        relation: One element from ``workplace.relations``.
    """
    if (relation.get("typeCode") or "") != WORKPLACE_TO_WORKPLACE_LINK:
        return None

    related = relation.get("onekeyEid")
    link_code = relation.get("linkCode")
    if not workplace_eid or not related or not link_code:
        return None
    if link_code in IGNORED_LINK_CODES:
        return None
    if workplace_eid == related:
        return None

    if relation.get("isParent"):
        child, parent = workplace_eid, related
    else:
        child, parent = related, workplace_eid

    return str(child), str(parent), str(link_code)


def affiliation_external_id(from_eid: str, to_eid: str, link_code: str) -> str:
    """Stable upsert key: from – to – linkCode.

    ``from_eid`` is the workplace that owns the ``relations[]`` entry, so the
    two sides of one OneKey link get different keys. On the child's side this
    is the same child – parent – linkCode key used before, so existing rows
    still match.
    """
    return f"{from_eid} - {to_eid} - {link_code}"


def _workplace_eid(record: dict) -> str | None:
    workplace = record.get("workplace") or record
    return workplace.get("workplaceEid") or record.get("onekeyEid")


def explode_w2w_relations(raw_workplaces: list[dict]) -> list[dict]:
    """Explode ``workplace.relations[]`` into normalized W2W rows."""
    edges: list[dict] = []

    for record in raw_workplaces:
        workplace = record.get("workplace") or record
        workplace_eid = workplace.get("workplaceEid") or record.get("onekeyEid")
        if not workplace_eid:
            continue

        for relation in workplace.get("relations") or []:
            normalized = normalize_hco_hco_edge(workplace_eid, relation)
            if not normalized:
                continue
            child_eid, parent_eid, link_code = normalized
            from_eid = str(workplace_eid)
            to_eid = str(relation["onekeyEid"])
            edges.append(
                {
                    "fromWorkplaceEid": from_eid,
                    "toWorkplaceEid": to_eid,
                    "childWorkplaceEid": child_eid,
                    "parentWorkplaceEid": parent_eid,
                    "linkCode": link_code,
                    "ts_externalid": affiliation_external_id(
                        from_eid, to_eid, link_code
                    ),
                    "beginValidDate": relation.get("beginValidDate"),
                    "endValidDate": relation.get("endValidDate"),
                    "codBase": record.get("codBase") or workplace.get("codBase"),
                    "country": record.get("country") or workplace.get("country"),
                }
            )

    return edges


def dedupe_w2w_relations(edges: list[dict]) -> list[dict]:
    """Keep one row per from–to–linkCode key.

    The two sides of a OneKey link have different keys, so both are kept;
    this only drops a side seen more than once, such as the same workplace
    appearing twice in a batch. Prefer the entry that carries validity dates;
    otherwise keep the first seen.
    """
    best: dict[str, dict] = {}

    for edge in edges:
        key = edge.get("ts_externalid") or affiliation_external_id(
            edge["fromWorkplaceEid"],
            edge["toWorkplaceEid"],
            edge["linkCode"],
        )
        existing = best.get(key)
        if existing is None:
            best[key] = edge
            continue
        if not existing.get("beginValidDate") and edge.get("beginValidDate"):
            best[key] = edge

    return list(best.values())


def filter_w2w_to_contracted(
    edges: list[dict],
    allowed_workplace_eids: set[str],
) -> list[dict]:
    """Drop edges where either side is outside the contracted population."""
    kept = []
    for edge in edges:
        child = edge.get("childWorkplaceEid")
        parent = edge.get("parentWorkplaceEid")
        if (
            child
            and parent
            and child in allowed_workplace_eids
            and parent in allowed_workplace_eids
        ):
            kept.append(edge)
    return kept


def collect_w2w_relations(
    raw_workplaces: list[dict],
    allowed_workplace_eids: set[str] | None = None,
) -> list[dict]:
    """Explode and dedupe workplace-to-workplace links.

    When ``allowed_workplace_eids`` is set (IDs from ``refreshMyOnekey``),
    edges that reference a workplace outside the population are dropped —
    those related IDs must not be fetched via search / ``get_by_id``.
    """
    edges = dedupe_w2w_relations(explode_w2w_relations(raw_workplaces))
    if allowed_workplace_eids is None:
        return edges
    return filter_w2w_to_contracted(edges, allowed_workplace_eids)
