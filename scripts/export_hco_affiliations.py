#!/usr/bin/env python3
"""Export OneKey HCO–HCO affiliations to CSV for Dataverse comparison.

Uses the same normalization (:func:`integration.w2w_relations.collect_w2w_relations`)
and W2W transform (:func:`integration.transform.TransformEngine.transform_w2w_affiliation`)
as sync, so every column matches what would be upserted into ``ts_affiliation``.

Lookup binds (``ts_fromid`` / ``ts_toid``) are exported as OneKey workplace EIDs;
Dataverse GUIDs are resolved only at sync time.

Usage:
    python scripts/export_hco_affiliations.py \\
        --input ../output/hco/hco_300.json \\
        --out ../output/hco/hco_affiliations_upsert.csv

    python scripts/export_hco_affiliations.py --codbase WUK --out ../output/hco/hco_affiliations_wuk.csv
"""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from integration.dynamic_mapping import load_mapping_config
from integration.transform import create_transform_engine
from integration.w2w_relations import collect_w2w_relations

logger = logging.getLogger("export_hco_affiliations")

# Columns in upsert / comparison order (Dataverse fields first, then OneKey source).
CSV_COLUMNS = [
    "ts_externalid",
    "ts_name",
    "ts_fromid_lookup",
    "ts_toid_lookup",
    "ts_compoundkey",
    "ts_linktype",
    "linkCode",
    "linkCode_label",
    "ts_isprimary",
    "ts_isparent",
    "ts_startdate",
    "ts_enddate",
    "fromWorkplaceEid",
    "toWorkplaceEid",
    "childWorkplaceEid",
    "parentWorkplaceEid",
    "codBase",
    "country",
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        type=Path,
        help="OneKey HCO JSON file (``results`` array or export wrapper)",
    )
    parser.add_argument(
        "--codbase",
        help="Fetch contracted workplaces from refreshMyOnekey instead of --input",
    )
    parser.add_argument(
        "--out",
        type=Path,
        help="Output CSV path (default: output/hco/hco_affiliations_<stamp>.csv)",
    )
    parser.add_argument(
        "--contracted-only",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="Drop edges whose child/parent is outside the input population (default: true)",
    )
    parser.add_argument(
        "--include-raw",
        action="store_true",
        help="Also write sidecar JSON with normalized edges + payloads",
    )
    parser.add_argument("-v", action="store_true")
    return parser.parse_args()


def load_workplaces_from_json(path: Path) -> list[dict]:
    data = json.loads(path.read_text(encoding="utf-8"))
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        for key in ("results", "workplaces", "hcpActivities"):
            if key in data and isinstance(data[key], list):
                records = data[key]
                if key == "workplaces":
                    return records
                return [r for r in records if (r.get("entityType") or "").upper() == "WORKPLACE" or r.get("workplace")]
        raise ValueError(f"No workplace list found in {path}")
    raise ValueError(f"Unsupported JSON root in {path}")


def load_workplaces_from_api(cod_base: str) -> list[dict]:
    from integration.env import load_env
    from onekey import OneKeyClient, OneKeyConfig, OneKeySession

    load_env()
    config = OneKeyConfig.from_env()
    records: list[dict] = []
    with OneKeyClient(OneKeySession(config)) as client:
        for record in client.iter_population(cod_base=cod_base, page_size=500):
            if (record.get("entityType") or "").upper() != "WORKPLACE":
                continue
            records.append(record)
    logger.info("Fetched %d workplaces from refreshMyOnekey (%s)", len(records), cod_base)
    return records


def workplace_eids(workplaces: list[dict]) -> set[str]:
    eids: set[str] = set()
    for record in workplaces:
        wp = record.get("workplace") or record
        eid = wp.get("workplaceEid") or record.get("onekeyEid")
        if eid:
            eids.add(str(eid))
    return eids


def linkcode_label(config, link_code: str | None) -> str:
    if not link_code:
        return ""
    code = str(link_code)
    for vm in config.value_mappings:
        if vm.mapping_type.lower().replace(" ", "") == "linkcode" and vm.source_code == code:
            return vm.target_label or vm.source_value or code
    return code


def build_export_row(edge: dict, payload: dict, config) -> dict:
    child = edge.get("childWorkplaceEid") or ""
    parent = edge.get("parentWorkplaceEid") or ""
    compound = f"{child} - {parent}" if child and parent else (payload.get("ts_compoundkey") or "")

    link_code = edge.get("linkCode") or ""
    return {
        "ts_externalid": payload.get("ts_externalid") or edge.get("ts_externalid") or "",
        "ts_name": payload.get("ts_name") or "",
        "ts_fromid_lookup": edge.get("fromWorkplaceEid") or "",
        "ts_toid_lookup": edge.get("toWorkplaceEid") or "",
        "ts_compoundkey": compound or "",
        "ts_linktype": payload.get("ts_linktype") or "",
        "linkCode": link_code,
        "linkCode_label": linkcode_label(config, link_code),
        "ts_isprimary": payload.get("ts_isprimary", ""),
        "ts_isparent": payload.get("ts_isparent", ""),
        "ts_startdate": payload.get("ts_startdate") or edge.get("beginValidDate") or "",
        "ts_enddate": payload.get("ts_enddate") or edge.get("endValidDate") or "",
        "fromWorkplaceEid": edge.get("fromWorkplaceEid") or "",
        "toWorkplaceEid": edge.get("toWorkplaceEid") or "",
        "childWorkplaceEid": child,
        "parentWorkplaceEid": parent,
        "codBase": edge.get("codBase") or "",
        "country": edge.get("country") or "",
    }


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=CSV_COLUMNS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)


def default_out_path(input_path: Path | None, cod_base: str | None) -> Path:
    repo_root = Path(__file__).resolve().parent.parent
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    if input_path:
        stem = input_path.stem
        return repo_root.parent / "output" / "hco" / f"{stem}_affiliations_upsert.csv"
    code = (cod_base or "export").lower()
    return repo_root.parent / "output" / "hco" / f"hco_affiliations_{code}_{stamp}.csv"


def main() -> int:
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.v else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if not args.input and not args.codbase:
        logger.error("Provide --input <json> or --codbase <code>")
        return 1
    if args.input and args.codbase:
        logger.error("Use either --input or --codbase, not both")
        return 1

    if args.input:
        workplaces = load_workplaces_from_json(args.input)
        source_label = str(args.input)
    else:
        workplaces = load_workplaces_from_api(args.codbase.strip().upper())
        source_label = f"refreshMyOnekey:{args.codbase.strip().upper()}"

    if not workplaces:
        logger.error("No workplace records found in %s", source_label)
        return 1

    allowed = workplace_eids(workplaces) if args.contracted_only else None
    edges = collect_w2w_relations(workplaces, allowed_workplace_eids=allowed)

    config = load_mapping_config()
    engine = create_transform_engine(config)

    rows: list[dict] = []
    sidecar: list[dict] = []
    for edge in edges:
        payload = engine.transform_w2w_affiliation(edge)
        row = build_export_row(edge, payload, config)
        rows.append(row)
        if args.include_raw:
            sidecar.append({"edge": edge, "payload": payload, "csv": row})

    rows.sort(key=lambda r: (r["ts_externalid"], r["fromWorkplaceEid"]))

    out_path = args.out or default_out_path(args.input, args.codbase)
    write_csv(out_path, rows)

    manifest = {
        "exportedAt": datetime.now(timezone.utc).isoformat(),
        "source": source_label,
        "workplaceCount": len(workplaces),
        "affiliationCount": len(rows),
        "contractedOnly": args.contracted_only,
        "columns": CSV_COLUMNS,
        "notes": {
            "upsertKey": "ts_externalid",
            "lookups": "ts_fromid_lookup / ts_toid_lookup are account.ts_externalid (= workplaceEid)",
            "skippedLinkCodes": sorted(getattr(
                __import__("integration.w2w_relations", fromlist=["IGNORED_LINK_CODES"]),
                "IGNORED_LINK_CODES",
                set(),
            )),
        },
        "outputCsv": str(out_path),
    }

    manifest_path = out_path.with_suffix(".manifest.json")
    manifest_path.write_text(
        json.dumps(manifest, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )

    if args.include_raw:
        raw_path = out_path.with_suffix(".raw.json")
        raw_path.write_text(
            json.dumps(sidecar, indent=2, ensure_ascii=False, default=str),
            encoding="utf-8",
        )
        manifest["rawJson"] = str(raw_path)

    logger.info(
        "Wrote %d affiliation row(s) from %d workplace(s) → %s",
        len(rows),
        len(workplaces),
        out_path,
    )
    print(json.dumps(manifest, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
