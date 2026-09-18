#!/usr/bin/env python3
"""Step 2 - read and write HCO/HCP records in Dynamics 365 Dataverse.

    HCO -> account    keyed on cr4ec_externalid
    HCP -> contact    keyed on externaluseridentifier

Commands:
    list      show accounts and contacts already in the environment
    upsert    load records from a JSON file produced by step 1
    delete    remove records whose OneKey id matches a prefix
    whoami    verify the OAuth2 token and print the calling user

Examples:
    python scripts/dataverse_records.py whoami
    python scripts/dataverse_records.py list
    python scripts/dataverse_records.py upsert --in data/sample.json
    python scripts/dataverse_records.py delete --prefix WDE --dry-run

Addresses are not written; see integration/mapping.py for why.
"""

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from dataverse import DataverseClient, DataverseConfig, DataverseError  # noqa: E402
from integration import (  # noqa: E402
    ACCOUNT_KEY_FIELD,
    CONTACT_KEY_FIELD,
    load_env,
    upsert_account,
    upsert_contact,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    sub = parser.add_subparsers(dest="command", required=True)

    sub.add_parser("whoami", help="verify the token and print the calling user")

    lister = sub.add_parser("list", help="show records already in Dataverse")
    lister.add_argument("--type", choices=["hco", "hcp", "both"], default="both")
    lister.add_argument("--limit", type=int, default=50)

    upserter = sub.add_parser("upsert", help="load records from step 1")
    upserter.add_argument("--in", dest="infile", required=True, metavar="FILE",
                          help="JSON file written by scripts/retrieve_onekey.py")
    upserter.add_argument("--type", choices=["hco", "hcp", "both"], default="both")
    upserter.add_argument("--with-address", action="store_true",
                          help="also write address columns (currently rejected "
                               "by the org; see integration/mapping.py)")
    upserter.add_argument("--dry-run", action="store_true",
                          help="show what would be written without writing")

    deleter = sub.add_parser("delete", help="remove OneKey-sourced records")
    deleter.add_argument("--prefix", default="WDE",
                         help="OneKey id prefix to match (default: WDE)")
    deleter.add_argument("--type", choices=["hco", "hcp", "both"], default="both")
    deleter.add_argument("--dry-run", action="store_true")
    return parser.parse_args()


def cmd_whoami(client):
    identity = client.whoami()
    print(f"Dataverse : {client.config.environment_url}")
    print(f"User      : {identity['UserId']}")
    print(f"Business  : {identity['BusinessUnitId']}")
    print(f"Org       : {identity['OrganizationId']}")
    return 0


def cmd_list(client, args):
    if args.type in ("hco", "both"):
        rows = client.retrieve_multiple(
            "accounts", select=["name", ACCOUNT_KEY_FIELD], order_by="name",
            max_records=args.limit)
        print(f"\nAccounts / HCO ({len(rows)})")
        print("-" * 74)
        for row in rows:
            print(f"  {(row.get(ACCOUNT_KEY_FIELD) or '-'):16} {row.get('name')}")

    if args.type in ("hcp", "both"):
        rows = client.retrieve_multiple(
            "contacts", select=["fullname", CONTACT_KEY_FIELD, "jobtitle"],
            expand="parentcustomerid_account($select=name)",
            order_by="lastname", max_records=args.limit)
        print(f"\nContacts / HCP ({len(rows)})")
        print("-" * 74)
        for row in rows:
            account = (row.get("parentcustomerid_account") or {}).get("name") or "-"
            print(f"  {(row.get(CONTACT_KEY_FIELD) or '-'):16} "
                  f"{(row.get('fullname') or '')[:26]:28} -> {account[:24]}")
    return 0


def cmd_upsert(client, args):
    with open(args.infile, encoding="utf-8") as handle:
        payload = json.load(handle)

    hcos = payload.get("hco", []) if args.type in ("hco", "both") else []
    hcps = payload.get("hcp", []) if args.type in ("hcp", "both") else []
    print(f"Loaded {len(hcos)} HCO and {len(hcps)} HCP from {args.infile}")

    if args.dry_run:
        from integration import hco_to_account, hcp_to_contact
        for hco in hcos:
            print("\naccount:", json.dumps(
                hco_to_account(hco, include_address=args.with_address),
                indent=2, ensure_ascii=False))
        for hcp in hcps:
            print("\ncontact:", json.dumps(
                hcp_to_contact(hcp, include_address=args.with_address),
                indent=2, ensure_ascii=False))
        print("\nDry run - nothing written.")
        return 0

    # Accounts first, so each contact can be linked to its workplace as it lands.
    account_ids, created_accounts = {}, 0
    for hco in hcos:
        account_id, created = upsert_account(
            client, hco, include_address=args.with_address)
        account_ids[hco.get("workplaceEid")] = account_id
        created_accounts += created
        print(f"  {'created' if created else 'updated'} account "
              f"{hco.get('workplaceEid')}  {hco.get('officialName')}")

    created_contacts = 0
    for hcp in hcps:
        # A contact links to its own workplace only when that HCO is in this batch.
        account_id = account_ids.get(hcp.get("workplaceEid"))
        contact_id, created = upsert_contact(
            client, hcp, account_id=account_id, include_address=args.with_address)
        created_contacts += created
        link = "linked" if account_id else "unlinked"
        print(f"  {'created' if created else 'updated'} contact "
              f"{hcp.get('individualEid')}  "
              f"{hcp.get('firstName') or ''} {hcp.get('lastName') or ''} ({link})")

    print(f"\nAccounts: {len(hcos)} ({created_accounts} created, "
          f"{len(hcos) - created_accounts} updated)")
    print(f"Contacts: {len(hcps)} ({created_contacts} created, "
          f"{len(hcps) - created_contacts} updated)")
    return 0


def cmd_delete(client, args):
    targets = []
    if args.type in ("hco", "both"):
        targets.append(("accounts", ACCOUNT_KEY_FIELD, "name"))
    if args.type in ("hcp", "both"):
        targets.append(("contacts", CONTACT_KEY_FIELD, "fullname"))

    for entity_set, key_field, label_field in targets:
        rows = client.retrieve_multiple(
            entity_set, select=[key_field, label_field],
            filter=f"startswith({key_field},'{args.prefix}')")
        print(f"\n{entity_set}: {len(rows)} match {args.prefix}*")
        id_field = entity_set[:-1] + "id"
        for row in rows:
            print(f"   {row.get(key_field)}  {row.get(label_field)}")
            if not args.dry_run:
                client.delete(entity_set, row[id_field])
    print("\nDry run - nothing deleted." if args.dry_run else "\nDeleted.")
    return 0


def main():
    args = parse_args()
    load_env()

    with DataverseClient(DataverseConfig.from_env()) as client:
        if args.command == "whoami":
            return cmd_whoami(client)
        if args.command == "list":
            return cmd_list(client, args)
        if args.command == "upsert":
            return cmd_upsert(client, args)
        return cmd_delete(client, args)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except DataverseError as exc:
        print(f"\nDataverse error: {exc}", file=sys.stderr)
        sys.exit(1)
