#!/usr/bin/env python3
"""Step 3 - the whole flow: retrieve from OneKey and upsert into Dataverse.

Does in one pass what steps 1 and 2 do separately. Use this for a real run; use
the separate scripts when you want to inspect or edit the data in between.

    OneKey workplace  (HCO) -> Dataverse account   keyed via mapping config
    OneKey individual (HCP) -> Dataverse contact   keyed via mapping config

Examples:
    python scripts/run_full_sync.py --dry-run
    python scripts/run_full_sync.py --hco 5 --hcp 5
    python scripts/run_full_sync.py --hco 10 --hcp 25 --codbase WFR

Records are matched on their OneKey id, so re-running updates rather than
duplicates. Addresses are not written; see integration/mapping.py.
"""

import argparse
import json
import logging
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dataverse import DataverseClient, DataverseConfig, DataverseError  # noqa: E402
from integration import hcp_to_contact, hco_to_account, load_env, sync_sample  # noqa: E402
from onekey import OneKeyClient, OneKeyConfig, OneKeyError, OneKeySession  # noqa: E402
from onekey.parse import flatten_hcp, flatten_hco  # noqa: E402


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--hco", type=int, default=5, help="accounts (default: 5)")
    parser.add_argument("--hcp", type=int, default=5, help="contacts (default: 5)")
    parser.add_argument("--codbase", default="WDE",
                        help="OneKey territory (default: WDE = Germany)")
    parser.add_argument("--with-address", action="store_true",
                        help="also write address columns (currently rejected by "
                             "the org; see integration/mapping.py)")
    parser.add_argument("--dry-run", action="store_true",
                        help="show the payloads without writing to Dataverse")
    parser.add_argument("--quiet", action="store_true", help="suppress progress logs")
    return parser.parse_args()


def main():
    args = parse_args()
    load_env()
    logging.basicConfig(
        level=logging.WARNING if args.quiet else logging.INFO,
        format="%(levelname)-7s %(message)s",
    )

    onekey_config = OneKeyConfig.from_env()
    with OneKeyClient(OneKeySession(onekey_config)) as onekey:
        print(f"OneKey    : {onekey_config.base_url} "
              f"(connector {onekey_config.connector_id})")

        if args.dry_run:
            print("\nDRY RUN - nothing will be written to Dataverse\n")
            raw = onekey.search_hcp(cod_base=args.codbase, last_name="S",
                                    result_size=args.hcp)
            for result in raw[: args.hcp]:
                hco = flatten_hco({
                    "onekeyEid": (result.get("workplace") or {}).get("workplaceEid"),
                    "codBase": result.get("codBase"),
                    "country": result.get("country"),
                    "workplace": result.get("workplace") or {},
                })
                print("-" * 78)
                print("account:", json.dumps(
                    hco_to_account(hco, include_address=args.with_address),
                    indent=2, ensure_ascii=False))
                print("contact:", json.dumps(
                    hcp_to_contact(flatten_hcp(result),
                                   include_address=args.with_address),
                    indent=2, ensure_ascii=False))
            return 0

        dataverse_config = DataverseConfig.from_env()
        with DataverseClient(dataverse_config) as dataverse:
            identity = dataverse.whoami()
            print(f"Dataverse : {dataverse_config.environment_url}")
            print(f"            user {identity['UserId']}\n")

            summary = sync_sample(
                onekey, dataverse,
                cod_base=args.codbase, hco_count=args.hco, hcp_count=args.hcp,
                include_address=args.with_address,
            )

            accounts, contacts = summary["accounts"], summary["contacts"]
            print(f"\nAccounts / HCO: {len(accounts)} "
                  f"({summary['accountsCreated']} created, "
                  f"{len(accounts) - summary['accountsCreated']} updated)")
            for account in accounts:
                print(f"   {account['onekeyEid']:14} {account['name']}")

            print(f"\nContacts / HCP: {len(contacts)} "
                  f"({summary['contactsCreated']} created, "
                  f"{len(contacts) - summary['contactsCreated']} updated)")
            linked = 0
            for contact in contacts:
                linked += bool(contact.get("accountId"))
                print(f"   {contact['onekeyEid']:14} {contact['name']}")
            print(f"\n{linked}/{len(contacts)} contacts linked to an account")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (OneKeyError, DataverseError) as exc:
        print(f"\nError: {exc}", file=sys.stderr)
        sys.exit(1)
