#!/usr/bin/env python3
"""Export the contracted OneKey population as Dataverse-shaped CSV files.

Three steps, each reusable on its own:

  pull     Walk /pop/refreshMyOnekey for a codebase and cache the raw packets
           (output/export/<CB>/raw_population.jsonl). Slow; run once.
  build    Transform the cached packets with the same mapping config and
           TransformEngine the sync uses, and write one CSV per Dataverse
           table. Picklist columns hold the Dataverse value the sync would
           send, with <column>__ok_code / __ok_label / __ok_local_label
           companion columns. Lookup columns hold the related record's
           OneKey ID (its alternate key) instead of a GUID.
  compare  Compare two or more built codebases and write diff CSVs to
           output/export/compare/.

Usage:
    python scripts/export_dv_csv.py pull --codbase WUK,WDE
    python scripts/export_dv_csv.py build --codbase WUK,WDE
    python scripts/export_dv_csv.py compare --codbase WUK,WDE
    python scripts/export_dv_csv.py all --codbase WUK,WDE    # pull + build + compare
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from integration.env import REPO_ROOT, load_env  # noqa: E402

logger = logging.getLogger("export_dv_csv")

DEFAULT_OUT = REPO_ROOT / "output" / "export"
RAW_FILE = "raw_population.jsonl"


# =============================================================================
# pull
# =============================================================================
def pull(codebase: str, out_dir: Path, page_size: int) -> Path:
    """Cache every population record for ``codebase`` as JSON lines."""
    from onekey import OneKeyClient, OneKeyConfig, OneKeySession

    target = out_dir / codebase / RAW_FILE
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_suffix(".jsonl.partial")

    counts: dict[str, int] = {}
    started = time.monotonic()
    config = OneKeyConfig.from_env()
    with OneKeyClient(OneKeySession(config)) as client, partial.open(
        "w", encoding="utf-8"
    ) as handle:
        for record in client.iter_population(cod_base=codebase, page_size=page_size):
            handle.write(json.dumps(record, ensure_ascii=False) + "\n")
            entity_type = (record.get("entityType") or "?").upper()
            counts[entity_type] = counts.get(entity_type, 0) + 1

    partial.replace(target)
    manifest = {
        "codbase": codebase,
        "pulled_at": datetime.now(timezone.utc).isoformat(),
        "onekey_base_url": config.base_url,
        "connector_id": config.connector_id,
        "seconds": round(time.monotonic() - started, 1),
        "counts": counts,
    }
    (target.parent / "pull_manifest.json").write_text(
        json.dumps(manifest, indent=2), encoding="utf-8"
    )
    logger.info("Pulled %s: %s in %.0fs", codebase, counts, manifest["seconds"])
    return target


# =============================================================================
# build
# =============================================================================
PICKLIST_TYPES = {"picklist", "multi-select picklist", "state"}
COMPANIONS = ("__ok_code", "__ok_label", "__ok_local_label")
MAPPED_ADDRESS_SLOTS = {"P,1", "P,2", "P,3", "P,4"}  # ADR-* covers P_1..P_4


def load_raw(codebase: str, out_dir: Path) -> tuple[dict, dict, list, dict]:
    """Read the cached packets into (workplaces, individuals, activities, stats)."""
    from integration.contracted_ids import (
        individual_eid_from_record,
        workplace_eid_from_record,
    )

    workplaces: dict[str, dict] = {}
    individuals: dict[str, dict] = {}
    activities: list[dict] = []
    stats = {"duplicate_workplaces": 0, "duplicate_individuals": 0}
    path = out_dir / codebase / RAW_FILE
    if not path.exists():
        raise SystemExit(f"No cached population for {codebase}: run the pull step first ({path})")
    with path.open(encoding="utf-8") as handle:
        for line in handle:
            record = json.loads(line)
            entity_type = (record.get("entityType") or "").upper()
            if entity_type == "WORKPLACE":
                eid = workplace_eid_from_record(record)
                if eid in workplaces:
                    stats["duplicate_workplaces"] += 1
                workplaces[eid] = record
            elif entity_type == "INDIVIDUAL":
                eid = individual_eid_from_record(record)
                if eid in individuals:
                    stats["duplicate_individuals"] += 1
                individuals[eid] = record
            elif entity_type == "ACTIVITY":
                activities.append(record)
    return workplaces, individuals, activities, stats


def _coded(obj: dict, list_code: str) -> list[tuple]:
    """(code, corporate label, local label) for an ada list, in sync slot order.

    SP follows ``specialty_slots`` (listCode, rank); TIH follows
    ``_tih_role_codes`` (rank, key).
    """
    ada = obj.get("ada") or obj.get("qualifications") or {}
    entries = []
    for key, value in ada.items():
        if not isinstance(value, dict) or not value.get("code"):
            continue
        code_list = (value.get("listCode") or key.split(",", 1)[0] or "").upper()
        if not code_list.startswith(list_code.upper()):
            continue
        try:
            rank = int(value.get("rank"))
        except (TypeError, ValueError):
            rank = 99 if list_code.upper() == "TIH" else 0
        entries.append((rank, key, value))
    entries.sort(key=lambda e: (e[0], e[1]))
    return [
        (v.get("code"), v.get("codeCorporateLabel"), v.get("codeLabel") or v.get("label"))
        for _, _, v in entries
    ]


def _scalar_code(obj: dict, key: str) -> tuple:
    """(code, corporate label, local label) for a scalar code field."""
    code = obj.get(key)
    label_key = key
    if code in (None, "") and key == "stateCode":
        code, label_key = obj.get("statusCode"), "statusCode"
    if label_key.endswith("Code"):
        base = label_key[: -len("Code")]
    elif label_key.endswith("EID"):
        base = label_key[: -len("EID")]
    else:
        base = label_key
    corporate = obj.get(base + "CorporateLabel") or obj.get(label_key + "CorporateLabel")
    local = obj.get(base + "Label") or obj.get(label_key + "Label")
    return code, corporate, local


def ok_source(path: str, objs: dict) -> tuple | None:
    """Resolve an entity_mapping source_field_path to its OneKey code and labels.

    ``objs`` holds the raw ``workplace`` / ``individual`` / ``activity`` object
    and, for address and relation rows, the ``address`` entry or ``relation``.
    """
    parts = (path or "").split(".")
    root, rest = parts[0], parts[1:]
    if root == "workplace" and rest and rest[0] == "workplaceAddresses":
        obj, rest = objs.get("address"), rest[2:]
    elif root == "workplace" and rest and rest[0].startswith("relations"):
        obj, rest = objs.get("relation"), rest[1:]
    else:
        obj = objs.get(root)
    if not obj or not rest:
        return None
    if rest[0] == "ada" and len(rest) >= 2:
        list_code, _, index = rest[1].rpartition("_")
        entries = _coded(obj, list_code)
        i = int(index) - 1 if index.isdigit() else -1
        return entries[i] if 0 <= i < len(entries) else None
    return _scalar_code(obj, rest[0])


class EntityCsv:
    """Collects rows for one Dataverse table in entity_mapping.csv column order."""

    def __init__(self, config, entity: str, mapping_filter=None):
        self.entity = entity
        self.mappings = [
            fm for fm in config.get_field_mappings(entity)
            if mapping_filter is None or mapping_filter(fm)
        ]
        self.columns: list[str] = []
        self.picklists: dict[str, list] = {}
        self.lookups: set[str] = set()
        for fm in self.mappings:
            if fm.target_field not in self.columns:
                self.columns.append(fm.target_field)
            if fm.is_lookup:
                self.lookups.add(fm.target_field)
            if (fm.target_field_type or "").lower() in PICKLIST_TYPES:
                self.picklists.setdefault(fm.target_field, []).append(fm)
        self.extras: list[str] = []
        self.rows: list[dict] = []

    def add(self, payload: dict, objs: dict, lookups: dict, meta: dict,
            picklists: dict | None = None) -> dict:
        picklists = self.picklists if picklists is None else picklists
        row: dict = {}
        for col in self.columns:
            row[col] = lookups.get(col) if col in self.lookups else payload.get(col)
            if col in picklists:
                found = [ok_source(fm.source_field_path, objs) for fm in picklists[col]]
                found = [f for f in found if f and f[0] not in (None, "")]
                for suffix, i in zip(COMPANIONS, range(3)):
                    row[col + suffix] = ";".join(str(f[i] or "") for f in found)
        for key, value in payload.items():
            if key in row or "@odata" in key:
                continue
            if key not in self.extras:
                self.extras.append(key)
            row[key] = value
        row.update(lookups)
        row.update(meta)
        self.rows.append(row)
        return row

    def header(self) -> list[str]:
        cols: list[str] = []
        for col in self.columns:
            cols.append(col)
            if col in self.picklists:
                cols.extend(col + s for s in COMPANIONS)
        cols.extend(c for c in self.extras if c not in cols)
        seen = set(cols)
        for row in self.rows:
            for key in row:
                if key not in seen:
                    cols.append(key)
                    seen.add(key)
        return cols

    def write(self, path: Path) -> None:
        write_csv(path, self.header(), self.rows)


def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    import csv

    path.parent.mkdir(parents=True, exist_ok=True)
    # utf-8-sig so Excel shows umlauts correctly.
    with path.open("w", newline="", encoding="utf-8-sig") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: _cell(v) for k, v in row.items()})


def _cell(value):
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (list, dict)):
        return json.dumps(value, ensure_ascii=False)
    return value


def _issues(result) -> str:
    return "; ".join(f"{e.field}: {e.message}" for e in result.errors)


def _primary_address_key(workplace: dict) -> str | None:
    """Key of the entry ``primary_address`` picks (the one the sync writes)."""
    addresses = workplace.get("workplaceAddresses") or {}
    if not isinstance(addresses, dict) or not addresses:
        return None

    def sort_key(item):
        key, value = item
        type_code = value.get("typeCode") or key.split(",", 1)[0]
        return (0 if type_code == "P" else 1, value.get("rank") or 0)

    return sorted(addresses.items(), key=sort_key)[0][0]


def _chain_depths(parents: dict[str, set[str]]) -> dict[str, tuple[int, list[str]]]:
    """Longest root→node chain for every node in a child→parents graph."""
    memo: dict[str, tuple[int, list[str]]] = {}

    def visit(node: str, trail: frozenset) -> tuple[int, list[str]]:
        if node in memo:
            return memo[node]
        best = (1, [node])
        for parent in sorted(parents.get(node, ())):
            if parent in trail or parent == node:
                continue  # cycle guard
            depth, chain = visit(parent, trail | {node})
            if depth + 1 > best[0]:
                best = (depth + 1, chain + [node])
        memo[node] = best
        return best

    nodes = set(parents) | {p for ps in parents.values() for p in ps}
    return {node: visit(node, frozenset()) for node in sorted(nodes)}


def build(codebase: str, out_dir: Path) -> dict:
    """Transform the cached population into Dataverse-shaped CSVs."""
    from integration import create_transform_engine, load_mapping_config
    from integration.activity_affiliation import flatten_activity_for_affiliation
    from integration.hierarchy import edges_from_hcos
    from integration.validation import MappingValidator
    from integration.w2w_relations import collect_w2w_relations, normalize_hco_hco_edge
    from onekey.parse import compose_hcp_result, flatten_address_entry, flatten_hco, flatten_hcp

    base = out_dir / codebase
    config = load_mapping_config()
    transformer = create_transform_engine(config)
    validator = MappingValidator(config)
    workplaces, individuals, activities, raw_stats = load_raw(codebase, out_dir)
    contracted_wp = set(workplaces)
    contracted_ind = set(individuals)
    names = {}

    # --- account ----------------------------------------------------------
    accounts = EntityCsv(config, "account")
    flat_hcos = []
    for eid, record in workplaces.items():
        flat = flatten_hco(record)
        flat_hcos.append(flat)
        workplace = record.get("workplace") or {}
        names[eid] = workplace.get("officialName") or workplace.get("usualName") or ""
        check = validator.validate_source_record(flat, "account")
        manager = flat.get("parentWorkplaceEid")
        in_contract = bool(manager and manager in contracted_wp and manager != eid)
        accounts.add(
            transformer.transform_hco(flat),
            {"workplace": workplace},
            {"parentaccountid": manager if in_contract else None},
            {
                "parentaccountid__ok_eid": manager,
                "parentaccountid__in_contract": in_contract if manager else None,
                "__valid": check.is_valid,
                "__issues": _issues(check),
            },
        )
    accounts.write(base / "account.csv")

    # --- customeraddress (every workplaceAddresses entry) ------------------
    # transform_address applies the P_1 slot mappings to each entry; do the same
    # for the companion columns so P_2..P_4 do not repeat them.
    addresses = EntityCsv(
        config, "customeraddress",
        lambda fm: "workplaceAddresses." not in (fm.source_field_path or "")
        or "workplaceAddresses.P_1." in fm.source_field_path,
    )
    address_counts = []
    seen_keys: dict[str, int] = {}
    for flat in flat_hcos:
        eid = flat["workplaceEid"]
        workplace = workplaces[eid].get("workplace") or {}
        entries = workplace.get("workplaceAddresses") or {}
        entries = entries if isinstance(entries, dict) else {}
        primary_key = _primary_address_key(workplace)
        address_eids = []
        for key, entry in entries.items():
            fields = flatten_address_entry(entry)
            address_eids.append(fields.get("addressEid"))
            source = {**flat, **{f"address_{k}": v for k, v in fields.items()}}
            payload = transformer.transform_address(source)
            if not payload:
                continue
            ext = payload.get("ts_externalid")
            seen_keys[ext] = seen_keys.get(ext, 0) + 1
            addresses.add(
                payload,
                {"workplace": workplace, "address": entry},
                {"parentid": eid},
                {
                    "__ok_slot": key,
                    "__is_primary": key == primary_key,
                    "__in_mapped_slots": key in MAPPED_ADDRESS_SLOTS,
                },
            )
        address_counts.append({
            "workplaceEid": eid,
            "name": names.get(eid),
            "address_count": len(entries),
            "distinct_address_eids": len({a for a in address_eids if a}),
            "slots": ";".join(sorted(entries)),
        })
    for row in addresses.rows:
        row["__duplicate_key"] = seen_keys.get(row.get("ts_externalid"), 0) > 1
    addresses.write(base / "customeraddress.csv")
    write_csv(base / "hco_address_counts.csv",
              ["workplaceEid", "name", "address_count", "distinct_address_eids", "slots"],
              address_counts)

    # --- contact (one per individual, main activity preferred) -------------
    main_activity: dict[str, dict] = {}
    for record in activities:
        activity = record.get("activity") or {}
        ind = activity.get("individualEid")
        if not ind:
            continue
        existing = main_activity.get(ind)
        if existing is None or (
            activity.get("isMainActivity")
            and not (existing.get("activity") or {}).get("isMainActivity")
        ):
            main_activity[ind] = record

    contacts = EntityCsv(config, "contact")
    skipped = {"individual_not_in_population": 0, "workplace_not_in_population": 0,
               "no_workplace": 0}
    for ind, activity_record in main_activity.items():
        activity = activity_record.get("activity") or {}
        wp_eid = activity.get("workplaceEid")
        if ind not in individuals:
            skipped["individual_not_in_population"] += 1
            continue
        if not wp_eid:
            skipped["no_workplace"] += 1
            continue
        if wp_eid not in workplaces:
            skipped["workplace_not_in_population"] += 1
            continue
        composed = compose_hcp_result(individuals[ind], activity_record, workplaces[wp_eid])
        flat = flatten_hcp(composed)
        check = validator.validate_source_record(flat, "contact")
        contacts.add(
            transformer.transform_hcp(flat),
            {"individual": composed["individual"], "activity": activity,
             "workplace": composed["workplace"]},
            {"ts_department": wp_eid, "ts_hospital": flat.get("hospitalWorkplaceEid")},
            {"__main_activity_eid": activity.get("activityEid"),
             "__valid": check.is_valid, "__issues": _issues(check)},
        )
    contacts.write(base / "contact.csv")
    contact_eids = {row.get("ts_externalid") for row in contacts.rows}

    # --- ts_affiliation: HCO-HCO (W2W) and HCP-HCO (AFF) --------------------
    w2w = EntityCsv(config, "ts_affiliation", lambda fm: fm.mapping_id.startswith("W2W-"))
    aff = EntityCsv(config, "ts_affiliation", lambda fm: fm.mapping_id.startswith("AFF-"))
    affiliations = EntityCsv(config, "ts_affiliation",
                             lambda fm: fm.mapping_id.startswith(("W2W-", "AFF-")))
    raw_by_eid = {eid: (rec.get("workplace") or {}) for eid, rec in workplaces.items()}
    edges = collect_w2w_relations(list(workplaces.values()), allowed_workplace_eids=contracted_wp)
    all_edges = collect_w2w_relations(list(workplaces.values()))
    for edge in edges:
        owner = raw_by_eid.get(edge["fromWorkplaceEid"]) or {}
        relation = next(
            (r for r in owner.get("relations") or []
             if r.get("onekeyEid") == edge["toWorkplaceEid"]
             and r.get("linkCode") == edge["linkCode"]),
            None,
        )
        affiliations.add(
            transformer.transform_w2w_affiliation(edge),
            {"workplace": owner, "relation": relation},
            {"ts_fromid": edge["fromWorkplaceEid"], "ts_toid": edge["toWorkplaceEid"]},
            {"ts_fromid__type": "account", "__source": "W2W (HCO-HCO)"},
            picklists=w2w.picklists,
        )
    aff_skipped = 0
    aff_no_contact = 0
    for record in activities:
        flat = flatten_activity_for_affiliation(record)
        if not (flat.get("activityEid") and flat.get("individualEid") and flat.get("workplaceEid")):
            aff_skipped += 1
            continue
        if flat["workplaceEid"] not in contracted_wp or flat["individualEid"] not in contracted_ind:
            aff_skipped += 1
            continue
        has_contact = flat["individualEid"] in contact_eids
        aff_no_contact += 0 if has_contact else 1
        affiliations.add(
            transformer.transform_activity_affiliation(flat),
            {"activity": record.get("activity") or {}},
            {"ts_fromid": flat["individualEid"], "ts_toid": flat["workplaceEid"]},
            {"ts_fromid__type": "contact", "__source": "AFF (HCP-HCO)",
             "__contact_in_export": has_contact},
            picklists=aff.picklists,
        )
    affiliations.write(base / "ts_affiliation.csv")

    # --- hierarchy depth ----------------------------------------------------
    manager_parents: dict[str, set[str]] = {}
    for child, parent in edges_from_hcos(flat_hcos, allowed_workplace_eids=contracted_wp):
        manager_parents.setdefault(child, set()).add(parent)
    link_parents: dict[str, dict[str, set[str]]] = {}
    edge_links: dict[tuple[str, str], set[str]] = {}
    for child, parents in manager_parents.items():
        for parent in parents:
            edge_links.setdefault((child, parent), set()).add("manager")
    for eid, record in workplaces.items():
        for relation in (record.get("workplace") or {}).get("relations") or []:
            normalized = normalize_hco_hco_edge(eid, relation)
            if not normalized:
                continue
            child, parent, link = normalized
            if child in contracted_wp and parent in contracted_wp:
                link_parents.setdefault(link, {}).setdefault(child, set()).add(parent)
                edge_links.setdefault((child, parent), set()).add(link)

    hierarchy_rows = []
    depth_summary = {}
    any_link: dict[str, set[str]] = {}
    for graph in link_parents.values():
        for child, parents in graph.items():
            any_link.setdefault(child, set()).update(parents)
    for source, graph in [("managerWorkplaceEid", manager_parents)] + [
        (f"relations:{link}", g) for link, g in sorted(link_parents.items())
    ] + [("relations:any link", any_link)]:
        depths = _chain_depths(graph)
        histogram: dict[int, int] = {}
        for node, (depth, chain) in depths.items():
            histogram[depth] = histogram.get(depth, 0) + 1
            has_children = any(node in ps for ps in graph.values())
            if not has_children:  # one row per leaf
                hierarchy_rows.append({
                    "source": source, "leaf_eid": node, "levels": depth,
                    "chain_eids": " > ".join(chain),
                    "chain_links": " > ".join(
                        "/".join(sorted(edge_links.get((chain[i + 1], chain[i]), set()) - {"manager"}))
                        for i in range(len(chain) - 1)
                    ),
                    "chain_names": " > ".join(names.get(e, "?") for e in chain),
                })
        depth_summary[source] = {
            "max_levels": max(histogram) if histogram else 0,
            "nodes_by_level": dict(sorted(histogram.items())),
        }
    hierarchy_rows.sort(key=lambda r: (r["source"], -r["levels"], r["leaf_eid"]))
    write_csv(base / "hco_hierarchy_chains.csv",
              ["source", "leaf_eid", "levels", "chain_eids", "chain_links", "chain_names"],
              hierarchy_rows)

    manager_out_of_contract = sum(
        1 for h in flat_hcos
        if h.get("parentWorkplaceEid") and h["parentWorkplaceEid"] not in contracted_wp
    )
    summary = {
        "codbase": codebase,
        "onekey": {
            "WORKPLACE": len(workplaces),
            "INDIVIDUAL": len(individuals),
            "ACTIVITY": len(activities),
            **raw_stats,
        },
        "dataverse_rows": {
            "account": len(accounts.rows),
            "account_invalid": sum(1 for r in accounts.rows if not r["__valid"]),
            "contact": len(contacts.rows),
            "contact_invalid": sum(1 for r in contacts.rows if not r["__valid"]),
            "customeraddress_all": len(addresses.rows),
            "customeraddress_primary_only": sum(1 for r in addresses.rows if r["__is_primary"]),
            "customeraddress_in_mapped_slots": sum(1 for r in addresses.rows if r["__in_mapped_slots"]),
            "customeraddress_duplicate_keys": sum(1 for r in addresses.rows if r["__duplicate_key"]),
            "ts_affiliation_hco_hco": len(edges),
            "ts_affiliation_hco_hco_dropped_out_of_contract": len(all_edges) - len(edges),
            "ts_affiliation_hcp_hco": len(affiliations.rows) - len(edges),
            "ts_affiliation_hcp_hco_no_contact": aff_no_contact,
            "ts_affiliation_hcp_hco_skipped": aff_skipped,
        },
        "contacts_skipped": {
            **skipped,
            "individuals_without_activity": len(set(individuals) - set(main_activity)),
        },
        "addresses_per_hco": {
            str(n): sum(1 for r in address_counts if r["address_count"] == n)
            for n in sorted({r["address_count"] for r in address_counts})
        },
        "hierarchy": {
            "manager_out_of_contract": manager_out_of_contract,
            **depth_summary,
        },
    }
    (base / "summary.json").write_text(json.dumps(summary, indent=2, ensure_ascii=False),
                                       encoding="utf-8")
    logger.info("Built %s: %s", codebase, summary["dataverse_rows"])
    return summary


# =============================================================================
# compare
# =============================================================================
TABLES = ("account", "contact", "customeraddress", "ts_affiliation")


def read_csv(path: Path) -> list[dict]:
    import csv

    with path.open(encoding="utf-8-sig", newline="") as handle:
        return list(csv.DictReader(handle))


def _family(column: str) -> str:
    """Group ts_specialty1..5 into one picklist family."""
    import re

    return re.sub(r"\d+$", "*", column) if column.startswith("ts_specialty") else column


def compare(codebases: list[str], out_dir: Path) -> None:
    """Compare built codebases and write diff CSVs to <out_dir>/compare/."""
    from collections import Counter

    from integration import create_transform_engine, load_mapping_config

    cmp_dir = out_dir / "compare"
    config = load_mapping_config()
    transformer = create_transform_engine(config)
    summaries = {
        cb: json.loads((out_dir / cb / "summary.json").read_text(encoding="utf-8"))
        for cb in codebases
    }
    tables = {cb: {t: read_csv(out_dir / cb / f"{t}.csv") for t in TABLES} for cb in codebases}

    # 1. Record counts ------------------------------------------------------
    count_rows = []
    first = summaries[codebases[0]]
    for section in ("onekey", "dataverse_rows", "contacts_skipped"):
        for metric in first[section]:
            count_rows.append({"section": section, "metric": metric,
                               **{cb: summaries[cb][section].get(metric) for cb in codebases}})
    for n in sorted({int(k) for cb in codebases for k in summaries[cb]["addresses_per_hco"]}):
        count_rows.append({"section": "addresses_per_hco", "metric": f"{n} address(es)",
                           **{cb: summaries[cb]["addresses_per_hco"].get(str(n), 0)
                              for cb in codebases}})
    sources = sorted({k for cb in codebases for k in summaries[cb]["hierarchy"]
                      if isinstance(summaries[cb]["hierarchy"][k], dict)})
    for source in sources:
        count_rows.append({"section": "hierarchy_max_levels", "metric": source,
                           **{cb: (summaries[cb]["hierarchy"].get(source) or {}).get("max_levels", 0)
                              for cb in codebases}})
    count_rows.append({"section": "hierarchy", "metric": "manager_out_of_contract",
                       **{cb: summaries[cb]["hierarchy"]["manager_out_of_contract"]
                          for cb in codebases}})
    write_csv(cmp_dir / "record_counts.csv", ["section", "metric", *codebases], count_rows)

    # 2. HCOs with more than 2 addresses ---------------------------------------
    many = []
    for cb in codebases:
        for row in read_csv(out_dir / cb / "hco_address_counts.csv"):
            if int(row["address_count"]) > 2:
                many.append({"codbase": cb, **row})
    many.sort(key=lambda r: (r["codbase"], -int(r["address_count"])))
    write_csv(cmp_dir / "hco_more_than_2_addresses.csv",
              ["codbase", "workplaceEid", "name", "address_count", "distinct_address_eids", "slots"],
              many)

    # 3. Hierarchy chains with 3+ levels ---------------------------------------
    deep = []
    for cb in codebases:
        for row in read_csv(out_dir / cb / "hco_hierarchy_chains.csv"):
            if int(row["levels"]) >= 3:
                deep.append({"codbase": cb, **row})
    write_csv(cmp_dir / "hco_3plus_level_chains.csv",
              ["codbase", "source", "leaf_eid", "levels", "chain_eids", "chain_links",
               "chain_names"], deep)

    # 4. Picklist codes per codebase ------------------------------------------
    picklist_fms = {}
    for table in TABLES:
        for fm in config.get_field_mappings(table):
            if (fm.target_field_type or "").lower() in PICKLIST_TYPES:
                picklist_fms.setdefault((table, fm.target_field), fm)

    stats: dict[tuple, dict] = {}
    for cb in codebases:
        for table, rows in tables[cb].items():
            if not rows:
                continue
            columns = [c for c in rows[0] if c + "__ok_code" in rows[0]]
            for row in rows:
                for col in columns:
                    codes = [c for c in (row.get(col + "__ok_code") or "").split(";")]
                    corp = (row.get(col + "__ok_label") or "").split(";")
                    local = (row.get(col + "__ok_local_label") or "").split(";")
                    for i, code in enumerate(codes):
                        if not code:
                            continue
                        key = (table, _family(col), code)
                        entry = stats.setdefault(key, {}).setdefault(
                            cb, {"count": 0, "corp": Counter(), "local": Counter()})
                        entry["count"] += 1
                        entry["corp"][corp[i] if i < len(corp) else ""] += 1
                        entry["local"][local[i] if i < len(local) else ""] += 1
                        stats[key].setdefault("_column", col)

    picklist_rows = []
    for (table, family, code), per_cb in sorted(stats.items()):
        column = per_cb.pop("_column")
        fm = picklist_fms.get((table, column))
        dv_value = transformer._apply_transform(code, fm, {}) if fm else None
        present = [cb for cb in codebases if cb in per_cb]
        corp_sets = {cb: {l for l in per_cb[cb]["corp"] if l} for cb in present}
        if len(present) == 1:
            status = f"only in {present[0]}"
        elif any(not s for s in corp_sets.values()):
            status = "no corporate label"
        elif len({frozenset(s) for s in corp_sets.values()}) > 1:
            status = "label differs"
        else:
            status = "same"
        row = {"table": table, "column": family, "ok_code": code, "status": status,
               "dv_value": dv_value, "dv_mapped": dv_value not in (None, "")}
        for cb in codebases:
            entry = per_cb.get(cb)
            row[f"{cb}_count"] = entry["count"] if entry else 0
            row[f"{cb}_label"] = " | ".join(l for l, _ in entry["corp"].most_common() if l) if entry else ""
            row[f"{cb}_local_label"] = " | ".join(l for l, _ in entry["local"].most_common() if l) if entry else ""
        picklist_rows.append(row)
    pk_cols = ["table", "column", "ok_code", "status", "dv_value", "dv_mapped"]
    for cb in codebases:
        pk_cols += [f"{cb}_count", f"{cb}_label", f"{cb}_local_label"]
    write_csv(cmp_dir / "picklist_codes.csv", pk_cols, picklist_rows)
    write_csv(cmp_dir / "picklist_label_differences.csv", pk_cols,
              [r for r in picklist_rows if r["status"] == "label differs"])
    write_csv(cmp_dir / "picklist_codes_single_codebase.csv", pk_cols,
              [r for r in picklist_rows if r["status"].startswith("only in")])

    # 5. Same record ID in more than one export --------------------------------
    same_rows = []
    overlap_counts = {}
    for table in TABLES:
        keyed = {cb: {r.get("ts_externalid"): r for r in tables[cb][table] if r.get("ts_externalid")}
                 for cb in codebases}
        for i, a in enumerate(codebases):
            for b in codebases[i + 1:]:
                shared = sorted(set(keyed[a]) & set(keyed[b]))
                overlap_counts[f"{table} {a}/{b}"] = len(shared)
                for ext in shared:
                    ra, rb = keyed[a][ext], keyed[b][ext]
                    for col in ra:
                        if col.startswith("__") or ra.get(col) == rb.get(col):
                            continue
                        same_rows.append({"table": table, "ts_externalid": ext, "column": col,
                                          "codbase_a": a, "value_a": ra.get(col),
                                          "codbase_b": b, "value_b": rb.get(col)})
    write_csv(cmp_dir / "same_record_differences.csv",
              ["table", "ts_externalid", "column", "codbase_a", "value_a", "codbase_b", "value_b"],
              same_rows)

    # Console summary --------------------------------------------------------
    width = max(len(r["metric"]) for r in count_rows) + 2
    print(f"\n{'metric':<{width}}" + "".join(f"{cb:>12}" for cb in codebases))
    for row in count_rows:
        print(f"{row['metric']:<{width}}" + "".join(f"{str(row[cb]):>12}" for cb in codebases))
    print(f"\nHCOs with >2 addresses: "
          + ", ".join(f"{cb}={sum(1 for r in many if r['codbase'] == cb)}" for cb in codebases))
    print("Chains with 3+ levels (leaf rows): "
          + ", ".join(f"{cb}={sum(1 for r in deep if r['codbase'] == cb)}" for cb in codebases))
    status_counts = Counter((r["table"], r["column"], r["status"]) for r in picklist_rows)
    print("\nPicklist codes by status:")
    for (table, column, status), n in sorted(status_counts.items()):
        print(f"  {table}.{column:<22} {status:<20} {n}")
    print("\nSame ts_externalid in more than one export:",
          ", ".join(f"{k}={v}" for k, v in overlap_counts.items()))
    print(f"\nWrote comparison files to {cmp_dir}")


# =============================================================================
# CLI
# =============================================================================
def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("step", choices=["pull", "build", "compare", "all"])
    parser.add_argument("--codbase", default="WUK,WDE",
                        help="Comma-separated codebases (default: WUK,WDE)")
    parser.add_argument("--out-dir", type=Path, default=DEFAULT_OUT,
                        help="Output root (default: output/export)")
    parser.add_argument("--page-size", type=int, default=1000,
                        help="Population packet size, 100-2000 (default: 1000)")
    parser.add_argument("-v", "--verbose", action="store_true")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    load_env()
    codebases = [c.strip().upper() for c in args.codbase.split(",") if c.strip()]

    if args.step in ("pull", "all"):
        for codebase in codebases:
            pull(codebase, args.out_dir, args.page_size)
    if args.step in ("build", "all"):
        for codebase in codebases:
            build(codebase, args.out_dir)
    if args.step in ("compare", "all"):
        compare(codebases, args.out_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
