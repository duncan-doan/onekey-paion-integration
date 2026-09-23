#!/usr/bin/env python3
"""Export one top HCO and its full hierarchy for Dataverse verification.

Walks the management tree (HIE + MAI workplace links) from a root Health Board,
fetches every HCO in that tree, dedupes HCO–HCO relations, searches HCP
activities per workplace, and writes:

  output/org/<root_eid>/
    hco_summary.csv
    hcp_summary.csv
    hco_relations_summary.csv
    manifest.json
    full_response.json

Usage:
    python scripts/export_org_hierarchy.py
    python scripts/export_org_hierarchy.py --root WUKE00001260 --codbase WUK
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from integration.env import REPO_ROOT, load_env
from integration.w2w_relations import collect_w2w_relations
from onekey import ACTIVITY, WORKPLACE, EXACT, OneKeyClient, OneKeyConfig, OneKeySession, search_field
from onekey.parse import flatten_hco, flatten_hcp

logger = logging.getLogger("export_org_hierarchy")

HIER_LINKS = frozenset({"HIE", "MAI"})
DEFAULT_ROOT = "WUKE00001260"  # Betsi Cadwaladr University Health Board (WUK)


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", default=DEFAULT_ROOT, help="Root workplace EID")
    parser.add_argument("--codbase", default="WUK")
    parser.add_argument(
        "--out-dir",
        type=Path,
        help="Output directory (default: output/org/<root>)",
    )
    parser.add_argument(
        "--hier-links",
        default="HIE,MAI",
        help="Comma-separated linkCodes for hierarchy BFS (default: HIE,MAI)",
    )
    parser.add_argument(
        "--fetch-delay",
        type=float,
        default=0.8,
        help="Seconds between OneKey id lookups (default: 0.8)",
    )
    parser.add_argument(
        "--hcp-delay",
        type=float,
        default=0.5,
        help="Seconds between HCP workplace searches (default: 0.5)",
    )
    parser.add_argument(
        "--max-hcp-per-workplace",
        type=int,
        default=1000,
        help="Max activities returned per workplace search",
    )
    parser.add_argument("-v", action="store_true")
    return parser.parse_args()


def workplace_eid(record: dict) -> str:
    wp = record.get("workplace") or record
    return wp.get("workplaceEid") or record.get("onekeyEid") or ""


def workplace_name(record: dict) -> str:
    wp = record.get("workplace") or record
    return wp.get("officialName") or wp.get("usualName") or ""


def child_eids_for_hierarchy(
    workplace_eid: str,
    relations: list[dict],
    hier_links: set[str],
) -> set[str]:
    from integration.w2w_relations import normalize_hco_hco_edge

    children: set[str] = set()
    for rel in relations or []:
        if rel.get("linkCode") not in hier_links:
            continue
        normalized = normalize_hco_hco_edge(workplace_eid, rel)
        if normalized:
            children.add(normalized[0])
    return children


def fetch_workplace(client: OneKeyClient, eid: str, cod_base: str) -> dict | None:
    return client.get_by_id(eid, WORKPLACE, cod_base=cod_base)


def walk_hierarchy(
    client: OneKeyClient,
    root_eid: str,
    cod_base: str,
    hier_links: set[str],
    fetch_delay: float,
) -> dict[str, dict]:
    queue = [root_eid]
    seen: set[str] = set()
    workplaces: dict[str, dict] = {}

    while queue:
        eid = queue.pop(0)
        if eid in seen:
            continue
        seen.add(eid)

        record = fetch_workplace(client, eid, cod_base)
        if not record:
            logger.warning("No record for workplace %s", eid)
            continue

        key = workplace_eid(record) or eid
        workplaces[key] = record
        wp = record.get("workplace") or {}
        logger.info(
            "HCO %3d %s — %s (manager=%s)",
            len(workplaces),
            key,
            wp.get("usualName") or wp.get("officialName"),
            wp.get("managerWorkplaceEid") or "-",
        )

        for child in child_eids_for_hierarchy(key, wp.get("relations"), hier_links):
            if child not in seen:
                queue.append(child)

        if fetch_delay > 0:
            time.sleep(fetch_delay)

    return workplaces


def search_hcps_at_workplace(
    client: OneKeyClient,
    workplace_eid: str,
    cod_base: str,
    max_results: int,
) -> list[dict]:
    fields = [
        search_field("activity.workplaceEid", workplace_eid, EXACT),
        search_field("activity.statusCode", "3", EXACT),
    ]
    return client.search(
        ACTIVITY,
        fields,
        cod_base=cod_base,
        result_size=max_results,
        is_in_contract=True,
    )


def role_label(activity: dict) -> str:
    ada = activity.get("ada") or {}
    for key in sorted(ada.keys()):
        entry = ada[key]
        if isinstance(entry, dict) and (entry.get("listCode") or "").startswith("TIH"):
            return entry.get("codeLabel") or entry.get("code") or ""
    return ""


def write_csv(path: Path, columns: list[str], rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def build_summaries(
    root_eid: str,
    workplaces: dict[str, dict],
    affiliations: list[dict],
    hcp_records: list[dict],
) -> tuple[list[dict], list[dict], list[dict]]:
    hco_rows = []
    for eid in sorted(workplaces):
        rec = workplaces[eid]
        flat = flatten_hco(rec)
        wp = rec.get("workplace") or {}
        hco_rows.append(
            {
                "workplaceEid": eid,
                "officialName": flat.get("officialName") or flat.get("usualName"),
                "usualName": flat.get("usualName"),
                "typeCode": flat.get("typeCode"),
                "typeLabel": flat.get("typeLabel"),
                "structureTypeCode": wp.get("structureTypeCode"),
                "managerWorkplaceEid": flat.get("parentWorkplaceEid"),
                "managerWorkplaceName": wp.get("managerWorkplaceOfficialName")
                or wp.get("managerWorkplaceUsualName"),
                "statusCode": flat.get("statusCode"),
                "activityLocationCode": flat.get("activityLocationCode"),
                "address_city": flat.get("address_city"),
                "address_postalCode": flat.get("address_postalCode"),
                "isRoot": eid == root_eid,
                "dataverse_account_key": eid,
                "dataverse_parentaccountid_lookup": flat.get("parentWorkplaceEid") or "",
            }
        )

    hcp_rows = []
    seen_individuals: set[str] = set()
    for rec in hcp_records:
        flat = flatten_hcp(rec)
        activity = rec.get("activity") or {}
        individual = rec.get("individual") or {}
        ind_eid = flat.get("individualEid") or ""
        hcp_rows.append(
            {
                "individualEid": ind_eid,
                "activityEid": flat.get("activityEid"),
                "workplaceEid": flat.get("workplaceEid"),
                "workplaceName": flat.get("workplaceName"),
                "firstName": flat.get("firstName"),
                "lastName": flat.get("lastName"),
                "title": individual.get("titleCode"),
                "typeLabel": flat.get("typeLabel"),
                "specialty1": (individual.get("ada") or {}).get("SP,1", {}).get("codeLabel")
                if isinstance((individual.get("ada") or {}).get("SP,1"), dict)
                else "",
                "roleAtWorkplace": role_label(activity),
                "isMainActivity": activity.get("isMainActivity"),
                "workingStatusCode": activity.get("workingStatusCode"),
                "activityStartDate": activity.get("activityStartDate"),
                "activityEndDate": activity.get("activityEndDate"),
                "statusCode": activity.get("statusCode"),
                "dataverse_contact_key": ind_eid,
                "dataverse_parentcustomerid_lookup": flat.get("workplaceEid") or "",
                "dataverse_affiliation_key": flat.get("activityEid"),
            }
        )
        seen_individuals.add(ind_eid)

    aff_rows = []
    for edge in affiliations:
        child = edge.get("childWorkplaceEid")
        parent = edge.get("parentWorkplaceEid")
        if child not in workplaces and parent not in workplaces:
            continue
        aff_rows.append(
            {
                "ts_externalid": edge.get("ts_externalid"),
                "childWorkplaceEid": child,
                "childName": workplace_name(workplaces.get(child) or {}),
                "parentWorkplaceEid": parent,
                "parentName": workplace_name(workplaces.get(parent) or {}),
                "linkCode": edge.get("linkCode"),
                "beginValidDate": edge.get("beginValidDate"),
                "endValidDate": edge.get("endValidDate"),
                "dataverse_ts_fromid_lookup": child,
                "dataverse_ts_toid_lookup": parent,
            }
        )

    return hco_rows, hcp_rows, aff_rows


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.v else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )
    load_env()

    root_eid = args.root.strip().upper()
    cod_base = args.codbase.strip().upper()
    hier_links = {x.strip().upper() for x in args.hier_links.split(",") if x.strip()}
    out_dir = args.out_dir or (
        REPO_ROOT.parent / "output" / "org" / root_eid
    )
    out_dir = Path(out_dir)

    config = OneKeyConfig.from_env()
    logger.info(
        "Export root=%s codBase=%s hierLinks=%s → %s",
        root_eid,
        cod_base,
        sorted(hier_links),
        out_dir,
    )

    with OneKeyClient(OneKeySession(config)) as client:
        client.session.test_connection()

        workplaces = walk_hierarchy(
            client,
            root_eid,
            cod_base,
            hier_links,
            args.fetch_delay,
        )
        if root_eid not in workplaces:
            logger.error("Root workplace %s could not be fetched", root_eid)
            return 1

        root_name = workplace_name(workplaces[root_eid])
        affiliations = collect_w2w_relations(list(workplaces.values()))

        hcp_records: list[dict] = []
        for i, eid in enumerate(sorted(workplaces), 1):
            batch = search_hcps_at_workplace(
                client,
                eid,
                cod_base,
                args.max_hcp_per_workplace,
            )
            if batch:
                logger.info("HCP search %3d/%d %s → %d activities", i, len(workplaces), eid, len(batch))
            hcp_records.extend(batch)
            if args.hcp_delay > 0:
                time.sleep(args.hcp_delay)

    hco_rows, hcp_rows, aff_rows = build_summaries(
        root_eid, workplaces, affiliations, hcp_records
    )

    unique_individuals = len({r["individualEid"] for r in hcp_rows if r.get("individualEid")})

    write_csv(
        out_dir / "hco_summary.csv",
        list(hco_rows[0].keys()) if hco_rows else ["workplaceEid"],
        hco_rows,
    )
    write_csv(
        out_dir / "hcp_summary.csv",
        list(hcp_rows[0].keys()) if hcp_rows else ["individualEid"],
        hcp_rows,
    )
    write_csv(
        out_dir / "hco_relations_summary.csv",
        list(aff_rows[0].keys()) if aff_rows else ["ts_externalid"],
        aff_rows,
    )

    manifest = {
        "exportedAt": datetime.now(timezone.utc).isoformat(),
        "rootWorkplaceEid": root_eid,
        "rootName": root_name,
        "codBase": cod_base,
        "hierarchyLinkCodes": sorted(hier_links),
        "counts": {
            "hcos": len(hco_rows),
            "hcpActivities": len(hcp_rows),
            "uniqueHcps": unique_individuals,
            "hcoHcoAffiliations": len(aff_rows),
            "hcoHcoAffiliationsAllWorkplaces": len(affiliations),
        },
        "files": {
            "hco_summary": "hco_summary.csv",
            "hcp_summary": "hcp_summary.csv",
            "hco_relations_summary": "hco_relations_summary.csv",
            "full_response": "full_response.json",
        },
        "dataverseMappingNotes": {
            "account": "ts_externalid / accountnumber = workplaceEid",
            "contact": "ts_externalid = individualEid (one per person)",
            "contact_parent": "parentcustomerid → account via workplaceEid",
            "ts_affiliation_hco_hco": "ts_externalid = {child} - {parent} - {linkCode}",
            "ts_affiliation_hcp_hco": "ts_externalid = activityEid (future AFF sync)",
        },
    }
    (out_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )

    full_payload = {
        **manifest,
        "workplaces": list(workplaces.values()),
        "affiliationsNormalized": affiliations,
        "hcpActivities": hcp_records,
    }
    (out_dir / "full_response.json").write_text(
        json.dumps(full_payload, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )

    logger.info(
        "Done: %d HCOs, %d HCP activities (%d people), %d relations → %s",
        len(hco_rows),
        len(hcp_rows),
        unique_individuals,
        len(aff_rows),
        out_dir,
    )
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
