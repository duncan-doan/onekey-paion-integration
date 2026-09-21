"""Move OneKey HCP/HCO data into Dynamics 365 Dataverse.

Accounts are written before contacts so every contact can be linked to its
workplace as it is created, rather than needing a second pass to fix up
relationships.
"""

import logging

from .mapping import (
    ACCOUNT_KEY_FIELD,
    CONTACT_KEY_FIELD,
    hco_to_account,
    hcp_to_contact,
)
from onekey.parse import flatten_hcp, flatten_hco

logger = logging.getLogger(__name__)


def upsert_account(dataverse, hco, include_address=False):
    """Create or update one ``account`` from a flattened OneKey HCO.

    Matches on the account upsert key from mapping config (OneKey
    ``workplaceEid``), so re-running a sync updates rather than duplicates.

    Returns:
        tuple[str, bool]: account id, and True when it was created.
    """
    record = hco_to_account(hco, include_address=include_address)
    key = record.get(ACCOUNT_KEY_FIELD)
    if not key:
        return dataverse.create("accounts", record), True
    return dataverse.upsert_by_field("accounts", ACCOUNT_KEY_FIELD, key, record)


def upsert_contact(dataverse, hcp, account_id=None, include_address=False):
    """Create or update one ``contact`` from a flattened OneKey HCP.

    Matches on the contact upsert key from mapping config (OneKey
    ``individualEid``).

    Note:
        A OneKey individual practising at several workplaces produces one
        ACTIVITY row per workplace but a single ``individualEid``. Matching on
        it therefore collapses them to one contact, whose parent account is
        whichever activity was processed last. Key on ``activityEid`` instead
        if you want one contact per practice location.

    Returns:
        tuple[str, bool]: contact id, and True when it was created.
    """
    record = hcp_to_contact(hcp, account_id=account_id, include_address=include_address)
    key = record.get(CONTACT_KEY_FIELD)
    if not key:
        return dataverse.create("contacts", record), True
    return dataverse.upsert_by_field("contacts", CONTACT_KEY_FIELD, key, record)


def sync_sample(onekey_client, dataverse, cod_base="WDE", hco_count=5, hcp_count=10,
                search_pool=1000, include_address=False):
    """Load a small, fully linked sample of OneKey data into Dataverse.

    Rather than pulling HCOs and HCPs independently -- which yields contacts
    whose workplaces are not among the accounts -- this searches HCPs first,
    groups them by workplace, and keeps only the workplaces that can supply the
    requested number of people. Every contact created is therefore linked to an
    account that also exists.

    Args:
        onekey_client: A connected :class:`onekey.OneKeyClient`.
        dataverse: A connected :class:`dataverse.DataverseClient`.
        cod_base: OneKey territory, e.g. ``WDE``.
        hco_count: Accounts to create.
        hcp_count: Contacts to create.
        search_pool: How many OneKey activities to consider when grouping.
            Needs to be generous: most OneKey workplaces have a single member
            of staff in any given slice, so a small pool yields workplaces that
            cannot supply ``hcp_count`` people between them. The connector caps
            a single search at 1000 results.
        include_address: Off by default -- only identity, classification and
            the account/contact link are written. The target org rejects
            address writes; see :mod:`mapping`.

    Returns:
        dict: summary counts and the records written.
    """
    logger.info("Searching OneKey for up to %d activities in %s", search_pool, cod_base)
    raw = onekey_client.search_hcp(
        cod_base=cod_base, last_name="S", method="STARTS_WITH", result_size=search_pool
    )
    people = [flatten_hcp(r) for r in raw]

    # Group people by the workplace they practise at.
    by_workplace = {}
    for person in people:
        workplace_eid = person.get("workplaceEid")
        if workplace_eid and person.get("individualEid"):
            by_workplace.setdefault(workplace_eid, []).append(person)

    # Prefer workplaces with the most staff, so a few accounts cover the people,
    # but keep only one workplace per name. OneKey reuses generic department
    # names ("Verwaltung", "Apotheke") across thousands of sites, and picking
    # purely by headcount yields a set of accounts that all look identical.
    ranked = sorted(by_workplace.items(), key=lambda kv: len(kv[1]), reverse=True)

    chosen, seen_names = [], set()
    for workplace_eid, staff in ranked:
        name = (_workplace_branch(raw, workplace_eid).get("officialName") or "").strip()
        key = name.casefold()
        if key and key in seen_names:
            continue
        seen_names.add(key)
        chosen.append((workplace_eid, staff))
        if len(chosen) == hco_count:
            break

    # Fall back to the plain ranking if de-duplicating left too few.
    if len(chosen) < hco_count:
        for item in ranked:
            if item not in chosen:
                chosen.append(item)
            if len(chosen) == hco_count:
                break
    if not chosen:
        raise RuntimeError(f"OneKey returned no usable activities for {cod_base}")

    # Take people round-robin across the chosen workplaces so the sample is not
    # all drawn from a single account.
    selected_people, index = [], 0
    while len(selected_people) < hcp_count:
        added = False
        for _, staff in chosen:
            if index < len(staff):
                selected_people.append(staff[index])
                added = True
                if len(selected_people) == hcp_count:
                    break
        if not added:
            break
        index += 1

    # Accounts first, so each contact can be linked as it is created.
    account_ids, accounts_written = {}, []
    for workplace_eid, staff in chosen:
        hco = flatten_hco(
            {
                "onekeyEid": workplace_eid,
                "codBase": staff[0].get("codBase"),
                "country": staff[0].get("country"),
                # The workplace branch of an ACTIVITY result carries the same
                # shape flatten_hco expects from a WORKPLACE search.
                "workplace": _workplace_branch(raw, workplace_eid),
            }
        )
        account_id, created = upsert_account(
            dataverse, hco, include_address=include_address
        )
        account_ids[workplace_eid] = account_id
        accounts_written.append(
            {"id": account_id, "created": created,
             "name": hco.get("officialName"), "onekeyEid": workplace_eid}
        )
        logger.info("%s account %s (%s)", "Created" if created else "Updated",
                    hco.get("officialName"), workplace_eid)

    contacts_written = []
    for person in selected_people:
        contact_id, created = upsert_contact(
            dataverse, person,
            account_id=account_ids.get(person.get("workplaceEid")),
            include_address=include_address,
        )
        contacts_written.append(
            {"id": contact_id, "created": created,
             "name": f"{person.get('firstName') or ''} {person.get('lastName') or ''}".strip(),
             "onekeyEid": person.get("individualEid"),
             "accountId": account_ids.get(person.get("workplaceEid"))}
        )
        logger.info("%s contact %s (%s)", "Created" if created else "Updated",
                    contacts_written[-1]["name"], person.get("individualEid"))

    return {
        "accounts": accounts_written,
        "contacts": contacts_written,
        "accountsCreated": sum(1 for a in accounts_written if a["created"]),
        "contactsCreated": sum(1 for c in contacts_written if c["created"]),
    }


def _workplace_branch(raw_results, workplace_eid):
    """Find the raw workplace object for a workplace id among ACTIVITY results."""
    for result in raw_results:
        workplace = result.get("workplace") or {}
        if workplace.get("workplaceEid") == workplace_eid:
            return workplace
    return {}
