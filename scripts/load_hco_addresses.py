#!/usr/bin/env python3
"""Load customeraddress rows for HCOs (sample or full population).

Uses the composite upsert key from ``entity_mapping.csv``
(``{workplaceEid} - {addressEid}`` on the customeraddress key field)
and binds each address to its Account (Dataverse 1-n). Shared OneKey addresses
become duplicate rows when multiple HCOs share the same addressEid.

Modes:
  (default) Sample accounts already in Dataverse and re-fetch workplaces
  --from-onekey   Sample via OneKey search
  --eids ...      Explicit workplaceEid list
  --full          Iterate OneKey population for the codebase (full migration)

Usage:
    python scripts/load_hco_addresses.py --limit 10
    python scripts/load_hco_addresses.py --limit 10 --codbase WUK --dry-run
    python scripts/load_hco_addresses.py --full --codbase WUK --max-records 100
    python scripts/load_hco_addresses.py --eids WUKF01484310,WUKH00015583
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from dataverse import DataverseClient, DataverseConfig
from dataverse.exceptions import DataverseHTTPError
from integration import create_transform_engine, load_mapping_config, setup_logging
from integration.env import load_env
from onekey import WORKPLACE, OneKeyClient, OneKeyConfig, OneKeySession
from onekey.parse import flatten_hco

logger = logging.getLogger(__name__)

ADDRESS_ENTITY = "customeraddresses"
# Set in main() from MappingConfig.require_key_field(...)
ADDRESS_KEY = ""
ACCOUNT_KEY = ""


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument("--limit", type=int, default=10, help="HCO count (default: 10)")
    parser.add_argument(
        "--max-records",
        type=int,
        default=None,
        help="With --full: cap population workplaces (default: unlimited)",
    )
    parser.add_argument("--codbase", default="WUK", help="OneKey codebase (default: WUK)")
    parser.add_argument(
        "--eids",
        help="Comma-separated workplaceEid values to load (skips sampling)",
    )
    parser.add_argument(
        "--from-onekey",
        action="store_true",
        help="Sample workplaces from OneKey search instead of existing Dataverse accounts",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Load addresses for all workplaces in OneKey population (migration mode)",
    )
    parser.add_argument(
        "--bulk-batch-size",
        type=int,
        default=None,
        help="Bulk upsert batch size when using --full (default: client config)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print payloads only; do not write to Dataverse",
    )
    return parser.parse_args()


def accounts_from_dataverse(dv: DataverseClient, codbase: str, limit: int) -> list[dict]:
    """Return accounts that look like this codebase (eid prefix)."""
    prefix = codbase.upper()
    rows = dv.retrieve_multiple(
        "accounts",
        select=["accountid", ACCOUNT_KEY, "name"],
        filter=f"startswith({ACCOUNT_KEY},'{prefix}') and {ACCOUNT_KEY} ne null",
        top=limit * 5,
        order_by=f"{ACCOUNT_KEY} asc",
    )
    return rows[: limit * 5]


def workplaces_from_onekey(ok: OneKeyClient, codbase: str, limit: int) -> list[dict]:
    """Search a thin HCO sample, then re-fetch by id for full address detail."""
    thin = ok.search_hco(cod_base=codbase, name="Hospital", result_size=max(limit * 3, 20))
    out = []
    for raw in thin:
        workplace = raw.get("workplace") or {}
        eid = workplace.get("workplaceEid")
        if not eid:
            continue
        full = ok.get_by_id(eid, WORKPLACE, cod_base=codbase)
        if not full:
            continue
        out.append(full)
        if len(out) >= limit * 3:
            break
    return out


def workplaces_from_population(
    ok: OneKeyClient, codbase: str, max_records: int | None
) -> list[dict]:
    """Collect WORKPLACE rows from population (includes address detail)."""
    out = []
    for record in ok.iter_population(cod_base=codbase, max_records=max_records):
        if (record.get("entityType") or "").upper() != "WORKPLACE":
            continue
        out.append(record)
    return out


def main() -> int:
    global ADDRESS_KEY, ACCOUNT_KEY
    args = parse_args()
    load_env()
    setup_logging()
    logging.basicConfig(level=logging.INFO, format="%(levelname)-7s %(message)s")

    config = load_mapping_config()
    ADDRESS_KEY = config.require_key_field("customeraddress")
    ACCOUNT_KEY = config.require_key_field("account")
    transformer = create_transform_engine(config)
    onekey_cfg = OneKeyConfig.from_env()
    dv_cfg = DataverseConfig.from_env()

    prepared: list[tuple[str, dict, dict]] = []  # (workplace_eid, hco, payload)
    skipped = []

    with OneKeyClient(OneKeySession(onekey_cfg)) as ok, DataverseClient(dv_cfg) as dv:
        target_eids = [
            e.strip() for e in (args.eids or "").split(",") if e.strip()
        ]
        if target_eids:
            args.limit = len(target_eids)

        print(f"OneKey    : {onekey_cfg.base_url}")
        print(f"Dataverse : {dv_cfg.environment_url}")
        if args.full:
            print(
                f"Mode      : FULL population ({args.codbase})"
                + (f" max={args.max_records}" if args.max_records else "")
            )
        else:
            print(f"Limit     : {args.limit} HCO addresses ({args.codbase})")
        if target_eids:
            print(f"EIDs      : {', '.join(target_eids)}")
        if args.dry_run:
            print("Write     : DRY RUN\n")
        else:
            print()

        if args.full:
            raw_workplaces = workplaces_from_population(
                ok, args.codbase, args.max_records
            )
            hcos_by_eid = {}
            for raw in raw_workplaces:
                hco = flatten_hco(raw)
                eid = hco.get("workplaceEid")
                if eid:
                    hcos_by_eid[eid] = hco
            eids = list(hcos_by_eid.keys())
            accounts = dv.find_many_by_field(
                "accounts",
                ACCOUNT_KEY,
                eids,
                select=["accountid", ACCOUNT_KEY, "name"],
            )
            candidates = []
            for eid, hco in hcos_by_eid.items():
                acc = accounts.get(eid)
                if not acc:
                    skipped.append((eid, "account not in Dataverse"))
                    continue
                candidates.append((eid, hco, acc))
        elif target_eids:
            accounts = dv.find_many_by_field(
                "accounts",
                ACCOUNT_KEY,
                target_eids,
                select=["accountid", ACCOUNT_KEY, "name"],
            )
            candidates = []
            for eid in target_eids:
                acc = accounts.get(eid)
                if not acc:
                    skipped.append((eid, "account not in Dataverse"))
                    continue
                try:
                    raw = ok.get_by_id(eid, WORKPLACE, cod_base=args.codbase)
                except Exception as exc:  # noqa: BLE001
                    skipped.append((eid, f"OneKey fetch failed: {exc}"))
                    continue
                if not raw:
                    skipped.append((eid, "not found in OneKey"))
                    continue
                candidates.append((eid, flatten_hco(raw), acc))
        elif args.from_onekey:
            raw_workplaces = workplaces_from_onekey(ok, args.codbase, args.limit)
            eids = []
            hcos_by_eid = {}
            for raw in raw_workplaces:
                hco = flatten_hco(raw)
                eid = hco.get("workplaceEid")
                if not eid:
                    continue
                eids.append(eid)
                hcos_by_eid[eid] = hco
            accounts = dv.find_many_by_field(
                "accounts", ACCOUNT_KEY, eids, select=["accountid", ACCOUNT_KEY, "name"]
            )
            candidates = []
            for eid, hco in hcos_by_eid.items():
                acc = accounts.get(eid)
                if not acc:
                    skipped.append((eid, "account not in Dataverse"))
                    continue
                candidates.append((eid, hco, acc))
        else:
            account_rows = accounts_from_dataverse(dv, args.codbase, args.limit)
            candidates = []
            for acc in account_rows:
                eid = acc.get(ACCOUNT_KEY)
                if not eid:
                    continue
                try:
                    raw = ok.get_by_id(eid, WORKPLACE, cod_base=args.codbase)
                except Exception as exc:  # noqa: BLE001
                    skipped.append((eid, f"OneKey fetch failed: {exc}"))
                    continue
                if not raw:
                    skipped.append((eid, "not found in OneKey"))
                    continue
                candidates.append((eid, flatten_hco(raw), acc))

        limit = None if args.full else args.limit
        for eid, hco, acc in candidates:
            if limit is not None and len(prepared) >= limit:
                break
            address_eid = hco.get("address_addressEid") or hco.get("addressEid")
            if not address_eid:
                skipped.append((eid, "no addressEid on workplace"))
                continue
            account_id = acc.get("accountid")
            payload = transformer.transform_address(hco, parent_account_id=account_id)
            if not payload:
                skipped.append((eid, "transform returned None"))
                continue
            prepared.append((eid, hco, payload))

        print(f"Prepared  : {len(prepared)} address payloads")
        if skipped:
            print(f"Skipped   : {len(skipped)}")
            for eid, reason in skipped[:15]:
                print(f"  - {eid}: {reason}")
            if len(skipped) > 15:
                print(f"  … +{len(skipped) - 15} more")
        print()

        results = []
        for eid, hco, payload in prepared:
            key = payload.get(ADDRESS_KEY)
            address_eid = hco.get("address_addressEid") or hco.get("addressEid")
            if not args.full or len(results) < 20:
                print("-" * 72)
                print(f"HCO {eid}  ({hco.get('officialName') or hco.get('usualName') or '—'})")
                print(f"  addressEid : {address_eid}")
                print(f"  composite  : {key}")
                print(f"  street/city: {payload.get('line1')} / {payload.get('city')}")
            results.append({
                "workplace_eid": eid,
                "hco_name": hco.get("officialName") or hco.get("usualName") or "",
                "address_eid": address_eid,
                "composite_key": key,
                "city": payload.get("city"),
                "postalcode": payload.get("postalcode"),
                "line1": payload.get("line1"),
                "accountid": (payload.get("parentid_account@odata.bind") or "")
                .removeprefix("/accounts(")
                .rstrip(")"),
                "status": "dry-run" if args.dry_run else "pending",
                "record_id": None,
                "error": None,
            })

        if args.dry_run or not prepared:
            _print_validation_summary(results, skipped)
            return 0

        print("\nWriting to Dataverse…")
        created = updated = failed = 0

        if args.full and hasattr(dv, "bulk_upsert_by_field"):
            batch_size = args.bulk_batch_size or dv.config.bulk_batch_size
            try:
                bulk = dv.bulk_upsert_by_field(
                    ADDRESS_ENTITY,
                    ADDRESS_KEY,
                    [p for _, _, p in prepared],
                    batch_size=batch_size,
                )
                for row, (eid, hco, payload), (key, record_id, was_created) in zip(
                    results, prepared, bulk
                ):
                    if was_created:
                        created += 1
                        row["status"] = "created"
                    else:
                        updated += 1
                        row["status"] = "updated"
                    row["record_id"] = record_id
            except DataverseHTTPError as exc:
                logger.exception("Bulk address upsert failed: %s", exc)
                failed = len(prepared)
                for row in results:
                    row["status"] = "failed"
                    row["error"] = str(exc)
        else:
            for row, (eid, hco, payload) in zip(results, prepared):
                key = payload[ADDRESS_KEY]
                try:
                    record_id, was_created = dv.upsert_by_field(
                        ADDRESS_ENTITY, ADDRESS_KEY, key, payload
                    )
                    if was_created:
                        created += 1
                        row["status"] = "created"
                    else:
                        updated += 1
                        row["status"] = "updated"
                    row["record_id"] = record_id
                    print(f"  OK {row['status']}: {key} -> {record_id}")
                except DataverseHTTPError as exc:
                    failed += 1
                    row["status"] = "failed"
                    row["error"] = str(exc)
                    print(f"  FAIL {key}: {exc}")
                    logger.exception("Address upsert failed for %s", key)

        print(
            f"\nDone: {created} created, {updated} updated, {failed} failed "
            f"(of {len(prepared)})"
        )
        _print_validation_summary(results, skipped)
        return 1 if failed else 0


def _print_validation_summary(results: list[dict], skipped: list[tuple[str, str]]) -> None:
    print("\n" + "=" * 96)
    print("ADDRESS VALIDATION SUMMARY")
    print("=" * 96)
    print(
        f"{'#':>2} {'status':8s} {'workplaceEid':14s} {'addressEid':14s} "
        f"{'composite key':32s} {'city':16s} {'postcode':10s}"
    )
    show = results[:50]
    for i, row in enumerate(show, start=1):
        print(
            f"{i:2d} {row['status']:8s} {row['workplace_eid'][:14]:14s} "
            f"{str(row['address_eid'])[:14]:14s} {str(row['composite_key'])[:32]:32s} "
            f"{str(row.get('city') or '—')[:16]:16s} {str(row.get('postalcode') or '—')[:10]:10s}"
        )
    if len(results) > 50:
        print(f"  … +{len(results) - 50} more rows")

    by_addr: dict[str, list[str]] = {}
    for row in results:
        ae = row.get("address_eid")
        if not ae:
            continue
        by_addr.setdefault(str(ae), []).append(row["workplace_eid"])
    shared = {k: v for k, v in by_addr.items() if len(v) > 1}

    print()
    print(f"Rows written/prepared : {len(results)}")
    print(f"Distinct addressEid   : {len(by_addr)}")
    print(f"Shared addressEid     : {len(shared)} "
          f"(same OneKey address on multiple HCOs → duplicate DV rows)")
    if shared:
        for ae, hcos in list(shared.items())[:8]:
            print(f"  {ae}: {', '.join(hcos)}")
    keys = [r["composite_key"] for r in results if r.get("composite_key")]
    print(f"Distinct composite keys: {len(set(keys))} "
          f"(must equal row count for correct 1-n keys)")
    if skipped:
        print(f"Skipped HCO candidates : {len(skipped)}")
    print("=" * 96)


if __name__ == "__main__":
    raise SystemExit(main())
