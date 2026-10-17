"""Which customeraddress row each OneKey address of an account goes to.

OneKey lists a workplace's addresses as P (main), M (mailing) and S
(secondary) links; onekey.parse.workplace_addresses orders them. They
become the account's address rows 1, 2, 3, ... in that order, and a missing
type moves the next one up, so the numbers run from 1 without a gap. Row 1
and row 2 also feed the account's Address 1 and Address 2 fields.

Dataverse may already hold rows for the account: the empty rows 1 and 2 it
creates with every account (unless the environment has "Disable empty
address record creation" on) and the rows earlier syncs wrote. The plan
reuses the row that already has each number, so no number is held twice,
and lists the writes that give each row its address's key before the
upsert, which matches on the key, fills it.
"""

from __future__ import annotations

from dataclasses import dataclass, field


@dataclass
class AddressPlan:
    """The writes that put one account's addresses on their numbered rows.

    Run in this order: ``deletes`` and ``blanks``, then ``clears``, then
    ``claims``; then upsert each address with ``numbers[key]`` as its
    addressnumber.
    """

    # Rows this sync wrote that no position keeps: deleted.
    deletes: list[str] = field(default_factory=list)
    # The same for rows 1 and 2, which back the account's Address 1 and 2
    # fields: emptied instead, since Dataverse refuses to delete them
    # unless "Enable deletion of address records" is on.
    blanks: list[str] = field(default_factory=list)
    # Rows whose key another position needs before they take a new one.
    clears: list[str] = field(default_factory=list)
    # Row id -> the key it takes, so the upsert fills that row.
    claims: dict[str, str] = field(default_factory=dict)
    # Key -> the addressnumber its upsert writes.
    numbers: dict[str, int] = field(default_factory=dict)
    # Row id -> the key it holds now, for the deleted and cleared rows.
    holding: dict[str, str] = field(default_factory=dict)

    @property
    def writes(self) -> int:
        return len(self.deletes) + len(self.blanks) + len(self.clears) + len(self.claims)


def plan_address_rows(rows: list[dict], keys: list[str], owned_prefix: str) -> AddressPlan:
    """Plan one account's address rows.

    ``rows`` are the account's customeraddress rows as ``{"id", "number",
    "key"}``. ``keys`` are the keys of its OneKey addresses, in address-number
    order. ``owned_prefix`` starts every key this sync writes for the account
    (``"{workplaceEid} - "``): only rows with such a key are ever deleted, so
    rows a user added by hand are left alone.

    Position k takes row k when the account has one (if two rows share the
    number, the one that already holds the key): that row keeps its key or
    is claimed for this one. Without a row k, the upsert writes the address
    with addressnumber k; that moves a row holding the key at a number past
    the last position, or creates the row. A row with an owned key that no
    position keeps is deleted, or emptied if it is row 1 or 2: its address
    left OneKey, or its key now belongs on another row. A claimed row whose
    current key another position needs gives it up first, since the key
    column is unique.
    """
    plan = AddressPlan()
    count = len(keys)
    by_id = {row["id"]: row for row in rows}
    by_number: dict[int, list[dict]] = {}
    by_key: dict[str, dict] = {}
    for row in rows:
        number = row.get("number")
        if isinstance(number, int):
            by_number.setdefault(number, []).append(row)
        if row.get("key"):
            by_key.setdefault(row["key"], row)

    kept: set[str] = set()
    for position, key in enumerate(keys, 1):
        plan.numbers[key] = position
        holder = by_key.get(key)
        numbered = by_number.get(position, [])
        target = holder if holder in numbered else next(iter(numbered), None)
        if target is not None:
            kept.add(target["id"])
            if target is not holder:
                plan.claims[target["id"]] = key
        elif holder is not None and not 1 <= (holder.get("number") or 0) <= count:
            # Past the last position: the upsert renumbers it in place.
            kept.add(holder["id"])

    wanted = set(keys)
    for row in rows:
        if row["id"] in kept:
            continue
        key = row.get("key")
        if key and str(key).startswith(owned_prefix):
            (plan.blanks if row.get("number") in (1, 2) else plan.deletes).append(row["id"])
            plan.holding[row["id"]] = key
    for row_id, key in plan.claims.items():
        current = by_id[row_id].get("key")
        if current and current != key and current in wanted:
            plan.clears.append(row_id)
            plan.holding[row_id] = current
    return plan


def address_id(source: dict) -> str | None:
    """The OneKey address id of a transform source, as TransformEngine reads it."""
    value = source.get("addressEid") or source.get("address_addressEid")
    if value is None:
        return None
    return str(value).strip() or None


def address_sources(hco: dict) -> list[dict]:
    """One transform source per address of a flattened workplace, in address-number order.

    flatten_hco lists every address under ``addresses``. Each source is the
    workplace row with that address's ``address_*`` fields, the main
    address's ones replaced. A row without the list (built by hand, or by
    another flattener) stands for its main address only.
    """
    entries = hco.get("addresses")
    if not isinstance(entries, list):
        return [hco]
    base = {
        name: value
        for name, value in hco.items()
        if not name.startswith("address_") and name not in ("addresses", "addressEid")
    }
    sources = []
    for entry in entries:
        source = {**base, **{f"address_{name}": value for name, value in entry.items()}}
        # The transform reads these two without the address_ prefix.
        source["addressEid"] = entry.get("addressEid")
        source["buildingLabel"] = entry.get("buildingLabel")
        sources.append(source)
    return sources


def workplace_batches(items: list[tuple], size: int):
    """Split ``(key, payload, workplace_eid)`` items into batches of whole workplaces.

    A workplace's addresses are planned together (plan_address_rows), so a
    batch closes only between two workplaces: it can pass ``size`` by the
    rest of the last workplace's addresses.
    """
    size = max(1, int(size))
    batch: list[tuple] = []
    for item in items:
        if len(batch) >= size and item[2] != batch[-1][2]:
            yield batch
            batch = []
        batch.append(item)
    if batch:
        yield batch
