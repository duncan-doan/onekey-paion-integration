#!/usr/bin/env python3
"""Step 1 - retrieve HCP and HCO data from IQVIA OneKey.

Prints what it finds and can save it as JSON for step 2 to load.

Examples:
    python scripts/retrieve_onekey.py --type hcp --last-name Schmidt
    python scripts/retrieve_onekey.py --type hco --name Klinik --limit 20
    python scripts/retrieve_onekey.py --type both --limit 5 --out data/sample.json
    python scripts/retrieve_onekey.py --test          # just check the credentials

Territory defaults to WDE (Germany); see docs/reference for the full codBase list.
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from integration import load_env  # noqa: E402
from onekey import (  # noqa: E402
    CONTAINS,
    EXACT,
    PHONETIC,
    STARTS_WITH,
    OneKeyClient,
    OneKeyConfig,
    OneKeyError,
    OneKeySession,
    flatten_hco,
    flatten_hcp,
)

METHODS = {"exact": EXACT, "contains": CONTAINS,
           "starts-with": STARTS_WITH, "phonetic": PHONETIC}


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--type", choices=["hcp", "hco", "both"], default="both",
                        help="what to retrieve (default: both)")
    parser.add_argument("--codbase", default="WDE",
                        help="OneKey territory code (default: WDE = Germany)")
    parser.add_argument("--limit", type=int, default=10,
                        help="max records per type (default: 10)")
    parser.add_argument("--method", choices=sorted(METHODS), default="starts-with",
                        help="match strategy (default: starts-with)")
    parser.add_argument("--last-name", default="S",
                        help="HCP surname criterion (default: S)")
    parser.add_argument("--first-name", help="HCP given name criterion")
    parser.add_argument("--specialty", help="specialty code, e.g. AR")
    parser.add_argument("--name", default="Klinik",
                        help="HCO name criterion (default: Klinik)")
    parser.add_argument("--city", help="city criterion")
    parser.add_argument("--include-inactive", action="store_true",
                        help="include records OneKey does not mark valid")
    parser.add_argument("--out", metavar="FILE",
                        help="write the results to this JSON file")
    parser.add_argument("--raw", action="store_true",
                        help="save the unflattened OneKey payloads")
    parser.add_argument("--test", action="store_true",
                        help="verify the credentials and exit")
    return parser.parse_args()


def show(title, rows, columns):
    print(f"\n{title} ({len(rows)})")
    print("-" * 100)
    if not rows:
        print("  no matches")
        return
    for row in rows:
        cells = []
        for key, width in columns:
            value = row.get(key)
            cells.append(f"{(str(value) if value else '-')[:width]:{width}}")
        print("  " + "  ".join(cells))


def main():
    args = parse_args()
    load_env()

    config = OneKeyConfig.from_env()
    with OneKeyClient(OneKeySession(config)) as client:
        print(f"OneKey: {config.base_url} (connector {config.connector_id})")

        if args.test:
            client.session.test_connection()
            print("Credentials accepted.")
            return 0

        method = METHODS[args.method]
        only_valid = not args.include_inactive
        output = {"codBase": args.codbase, "hcp": [], "hco": []}

        if args.type in ("hcp", "both"):
            raw = client.search_hcp(
                cod_base=args.codbase, last_name=args.last_name,
                first_name=args.first_name, specialty=args.specialty, city=args.city,
                method=method, only_valid=only_valid, result_size=args.limit,
            )
            output["hcp"] = raw if args.raw else [flatten_hcp(r) for r in raw]
            show("HCP - healthcare professionals", [flatten_hcp(r) for r in raw],
                 [("individualEid", 14), ("lastName", 20), ("firstName", 16),
                  ("specialtyLabel", 24), ("workplaceName", 20)])

        if args.type in ("hco", "both"):
            raw = client.search_hco(
                cod_base=args.codbase, name=args.name, city=args.city,
                method=CONTAINS, only_valid=only_valid, result_size=args.limit,
            )
            output["hco"] = raw if args.raw else [flatten_hco(r) for r in raw]
            show("HCO - healthcare organisations", [flatten_hco(r) for r in raw],
                 [("workplaceEid", 14), ("officialName", 40), ("typeLabel", 24),
                  ("address_city", 18)])

        if args.out:
            directory = os.path.dirname(os.path.abspath(args.out))
            os.makedirs(directory, exist_ok=True)
            with open(args.out, "w", encoding="utf-8") as handle:
                json.dump(output, handle, indent=2, ensure_ascii=False)
            print(f"\nSaved {len(output['hcp'])} HCP and {len(output['hco'])} HCO "
                  f"to {args.out}")
            print("Load it into Dataverse with:")
            print(f"   python scripts/dataverse_records.py upsert --in {args.out}")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except OneKeyError as exc:
        print(f"\nOneKey error: {exc}", file=sys.stderr)
        sys.exit(1)
