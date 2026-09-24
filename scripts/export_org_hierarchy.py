#!/usr/bin/env python3
"""Export one top HCO and its related tree for Dataverse verification/load.

1. Walks the management tree (default HIE + MAI) from a root Health Board
2. Optionally fetches every related workplace referenced by any linkCode
3. Searches HCP activities per workplace (default: not limited to in-contract)
4. Dedupes HCO–HCO relations and writes activity/affiliation summaries
5. Optionally upserts accounts, contacts, and ts_affiliation into Dataverse

  output/org/<root_eid>/
    hco_summary.csv
    hcp_summary.csv
    activity_summary.csv
    hco_relations_summary.csv
    manifest.json
    full_response.json

Usage:
    python scripts/export_org_hierarchy.py
    python scripts/export_org_hierarchy.py --root WUKE00001260 --codbase WUK
    python scripts/export_org_hierarchy.py --root WUKE00001260 --fetch-related all --load-dataverse
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
    parser.add_argument(
        "--fetch-related",
        choices=("hierarchy", "all"),
        default="hierarchy",
        help="hierarchy=HIE/MAI tree only (default); all=also fetch workplaces linked by any linkCode (slow; rate-limits)",
    )
    parser.add_argument(
        "--in-contract-only",
        action="store_true",
        help="Restrict HCP search to isInContract=true (default: all activities)",
    )
    parser.add_argument(
        "--load-dataverse",
        action="store_true",
        help="After export, upsert HCOs, HCPs, and HCO–HCO affiliations to Dataverse",
    )
    parser.add_argument(
        "--dry-run-load",
        action="store_true",
        help="With --load-dataverse, transform only (no writes)",
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


def related_eids_from_workplaces(workplaces: dict[str, dict]) -> set[str]:
    """Collect every onekeyEid referenced by workplace.relations[]."""
    found: set[str] = set()
    for record in workplaces.values():
        wp = record.get("workplace") or {}
        for rel in wp.get("relations") or []:
            eid = rel.get("onekeyEid")
            if eid:
                found.add(str(eid))
    return found


def fetch_missing_workplaces(
    client: OneKeyClient,
    workplaces: dict[str, dict],
    missing: set[str],
    cod_base: str,
    fetch_delay: float,
) -> int:
    """Fetch workplaces not yet in ``workplaces``; return count added."""
    added = 0
    for eid in sorted(missing):
        if eid in workplaces:
            continue
        record = fetch_workplace(client, eid, cod_base)
        if not record:
            logger.warning("No record for related workplace %s", eid)
            continue
        key = workplace_eid(record) or eid
        workplaces[key] = record
        added += 1
        wp = record.get("workplace") or {}
        logger.info(
            "Related HCO %3d %s — %s",
            added,
            key,
            wp.get("usualName") or wp.get("officialName"),
        )
        if fetch_delay > 0:
            time.sleep(fetch_delay)
    return added


def search_hcps_at_workplace(
    client: OneKeyClient,
    workplace_eid: str,
    cod_base: str,
    max_results: int,
    in_contract_only: bool = False,
) -> list[dict]:
    fields = [
        search_field("activity.workplaceEid", workplace_eid, EXACT),
        search_field("activity.statusCode", "3", EXACT),
    ]
    kwargs = {
        "cod_base": cod_base,
        "result_size": max_results,
    }
    if in_contract_only:
        kwargs["is_in_contract"] = True
    return client.search(ACTIVITY, fields, **kwargs)


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
) -> tuple[list[dict], list[dict], list[dict], list[dict]]:
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
                "relationCount": len(wp.get("relations") or []),
                "dataverse_account_key": eid,
                "dataverse_parentaccountid_lookup": flat.get("parentWorkplaceEid") or "",
            }
        )

    hcp_rows = []
    activity_rows = []
    for rec in hcp_records:
        flat = flatten_hcp(rec)
        activity = rec.get("activity") or {}
        individual = rec.get("individual") or {}
        ind_eid = flat.get("individualEid") or ""
        act_eid = flat.get("activityEid") or ""
        wkp_eid = flat.get("workplaceEid") or ""
        row = {
            "individualEid": ind_eid,
            "activityEid": act_eid,
            "workplaceEid": wkp_eid,
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
            "dataverse_parentcustomerid_lookup": wkp_eid,
            "dataverse_affiliation_key": act_eid,
            "dataverse_ts_fromid_lookup": ind_eid,
            "dataverse_ts_toid_lookup": wkp_eid,
        }
        hcp_rows.append(row)
        activity_rows.append(row)

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

    return hco_rows, hcp_rows, aff_rows, activity_rows


def load_org_into_dataverse(
    workplaces: dict[str, dict],
    hcp_records: list[dict],
    affiliations: list[dict],
    dry_run: bool = False,
) -> dict:
    """Upsert HCO accounts, HCP contacts, and HCO–HCO affiliations for this org."""
    from dataverse import DataverseClient, DataverseConfig
    from integration.dynamic_sync import DynamicSyncEngine
    from integration.sync_logger import SyncLogger

    dv = DataverseClient(DataverseConfig.from_env())
    # OneKey client unused for this offline load path
    engine = DynamicSyncEngine(
        onekey_client=None,  # type: ignore[arg-type]
        dataverse_client=dv,
        sync_logger=SyncLogger(),
    )

    flattened_hcos = [flatten_hco(r) for r in workplaces.values()]
    hco_stats, _, _ = engine._sync_hcos(
        codebase="WUK",
        dry_run=dry_run,
        flattened=flattened_hcos,
    )

    # Hierarchy from managerWorkplaceEid
    hier_stats = engine._sync_hierarchy(flattened_hcos, dry_run=dry_run)

    # Prefer main activity per individual for contact.parentcustomerid
    by_individual: dict[str, dict] = {}
    for rec in hcp_records:
        activity = rec.get("activity") or {}
        individual = rec.get("individual") or {}
        ind_eid = individual.get("individualEid") or activity.get("individualEid")
        if not ind_eid:
            continue
        existing = by_individual.get(ind_eid)
        if existing is None:
            by_individual[ind_eid] = rec
            continue
        existing_act = existing.get("activity") or {}
        if activity.get("isMainActivity") and not existing_act.get("isMainActivity"):
            by_individual[ind_eid] = rec

    composed = list(by_individual.values())
    hcp_stats = engine._sync_hcps(
        codebase="WUK",
        dry_run=dry_run,
        composed=composed,
    )

    activity_aff_stats = engine._sync_activity_affiliations(
        hcp_records,
        dry_run=dry_run,
    )

    aff_stats = engine._sync_hco_relations(
        list(workplaces.values()),
        dry_run=dry_run,
    )

    return {
        "accounts": hco_stats,
        "hierarchy": hier_stats,
        "contacts": hcp_stats,
        "activityAffiliations": activity_aff_stats,
        "affiliations": aff_stats,
        "affiliationEdgesPrepared": len(affiliations),
        "activityRecords": len(hcp_records),
    }


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

        related_added = 0
        if args.fetch_related == "all":
            missing = related_eids_from_workplaces(workplaces) - set(workplaces)
            logger.info(
                "Fetching %d related workplaces referenced by relations[]…",
                len(missing),
            )
            related_added = fetch_missing_workplaces(
                client,
                workplaces,
                missing,
                cod_base,
                args.fetch_delay,
            )

        root_name = workplace_name(workplaces[root_eid])
        affiliations = collect_w2w_relations(list(workplaces.values()))

        hcp_records: list[dict] = []
        for i, eid in enumerate(sorted(workplaces), 1):
            batch = search_hcps_at_workplace(
                client,
                eid,
                cod_base,
                args.max_hcp_per_workplace,
                in_contract_only=args.in_contract_only,
            )
            if batch:
                logger.info(
                    "HCP search %3d/%d %s → %d activities",
                    i,
                    len(workplaces),
                    eid,
                    len(batch),
                )
            hcp_records.extend(batch)
            if args.hcp_delay > 0:
                time.sleep(args.hcp_delay)

    hco_rows, hcp_rows, aff_rows, activity_rows = build_summaries(
        root_eid, workplaces, affiliations, hcp_records
    )

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
        out_dir / "activity_summary.csv",
        list(activity_rows[0].keys()) if activity_rows else ["activityEid"],
        activity_rows,
    )
    write_csv(
        out_dir / "hco_relations_summary.csv",
        list(aff_rows[0].keys()) if aff_rows else ["ts_externalid"],
        aff_rows,
    )

    unique_individuals = len(
        {r["individualEid"] for r in hcp_rows if r.get("individualEid")}
    )
    manifest = {
        "exportedAt": datetime.now(timezone.utc).isoformat(),
        "rootWorkplaceEid": root_eid,
        "rootName": root_name,
        "codBase": cod_base,
        "hierarchyLinkCodes": sorted(hier_links),
        "fetchRelated": args.fetch_related,
        "inContractOnly": args.in_contract_only,
        "counts": {
            "hcos": len(hco_rows),
            "relatedHcosFetched": related_added,
            "hcpActivities": len(hcp_rows),
            "uniqueHcps": unique_individuals,
            "hcoHcoAffiliations": len(aff_rows),
            "hcoHcoAffiliationsAllWorkplaces": len(affiliations),
        },
        "files": {
            "hco_summary": "hco_summary.csv",
            "hcp_summary": "hcp_summary.csv",
            "activity_summary": "activity_summary.csv",
            "hco_relations_summary": "hco_relations_summary.csv",
            "full_response": "full_response.json",
        },
        "dataverseLoad": None,
        "dataverseMappingNotes": {
            "account": "ts_externalid / accountnumber = workplaceEid",
            "contact": "ts_externalid = individualEid (one per person; prefers isMainActivity)",
            "contact_parent": "parentcustomerid → account via workplaceEid",
            "ts_affiliation_hco_hco": "ts_externalid = {child} - {parent} - {linkCode}",
            "ts_affiliation_hcp_hco": "ts_externalid = activityEid (AFF-* mapping; activity_summary.csv)",
        },
    }
    full_payload = {
        **manifest,
        "workplaces": list(workplaces.values()),
        "affiliationsNormalized": affiliations,
        "hcpActivities": hcp_records,
    }
    (out_dir / "manifest.json").write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
    )
    (out_dir / "full_response.json").write_text(
        json.dumps(full_payload, indent=2, ensure_ascii=False, default=str),
        encoding="utf-8",
    )
    logger.info(
        "Wrote export package: %d HCOs, %d activities (%d people), %d relations → %s",
        len(hco_rows),
        len(hcp_rows),
        unique_individuals,
        len(aff_rows),
        out_dir,
    )

    load_result = None
    if args.load_dataverse:
        logger.info("Loading org package into Dataverse (dry_run=%s)…", args.dry_run_load)
        try:
            load_result = load_org_into_dataverse(
                workplaces,
                hcp_records,
                affiliations,
                dry_run=args.dry_run_load,
            )
            logger.info("Dataverse load result: %s", json.dumps(load_result, default=str))
        except Exception:
            logger.exception("Dataverse load failed; export files were already saved")
            raise
        manifest["dataverseLoad"] = load_result
        (out_dir / "manifest.json").write_text(
            json.dumps(manifest, indent=2, ensure_ascii=False), encoding="utf-8"
        )

    logger.info(
        "Done: %d HCOs (+%d related), %d HCP activities (%d people), %d relations → %s",
        len(hco_rows),
        related_added,
        len(hcp_rows),
        unique_individuals,
        len(aff_rows),
        out_dir,
    )
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
