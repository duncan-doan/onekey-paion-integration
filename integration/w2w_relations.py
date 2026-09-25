"""HCO–HCO workplace relation helpers (OneKey relations[] → ts_affiliation).

OneKey stores each HCO–HCO link twice (once per workplace). ``isParent`` marks
which side is the parent org. These helpers normalize to child/parent and
dedupe before upserting a single Dataverse affiliation row per link.
"""

from __future__ import annotations

from typing import Any

WORKPLACE_TO_WORKPLACE_LINK = "workplaceToWorkplaceLink"


def normalize_hco_hco_edge(
    workplace_eid: str,
    relation: dict,
) -> tuple[str, str, str] | None:
    """Return canonical (child_eid, parent_eid, link_code) or None if invalid.

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
    if workplace_eid == related:
        return None

    if relation.get("isParent"):
        child, parent = workplace_eid, related
    else:
        child, parent = related, workplace_eid

    return str(child), str(parent), str(link_code)


def canonical_external_id(child_eid: str, parent_eid: str, link_code: str) -> str:
    """Stable upsert key: child – parent – linkCode."""
    return f"{child_eid} - {parent_eid} - {link_code}"


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
            edges.append(
                {
                    "childWorkplaceEid": child_eid,
                    "parentWorkplaceEid": parent_eid,
                    "linkCode": link_code,
                    "ts_externalid": canonical_external_id(
                        child_eid, parent_eid, link_code
                    ),
                    "beginValidDate": relation.get("beginValidDate"),
                    "endValidDate": relation.get("endValidDate"),
                    "codBase": record.get("codBase") or workplace.get("codBase"),
                    "country": record.get("country") or workplace.get("country"),
                }
            )

    return edges


def dedupe_w2w_relations(edges: list[dict]) -> list[dict]:
    """Keep one row per canonical child–parent–linkCode key.

    When both OneKey directions are present, prefer the entry that carries
    validity dates; otherwise keep the first seen.
    """
    best: dict[str, dict] = {}

    for edge in edges:
        key = edge.get("ts_externalid") or canonical_external_id(
            edge["childWorkplaceEid"],
            edge["parentWorkplaceEid"],
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
