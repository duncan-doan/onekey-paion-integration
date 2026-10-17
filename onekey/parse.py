"""Flatten OneKey's nested search results into plain rows.

OneKey responses are deeply nested and use compound dictionary keys rather than
lists: qualifications arrive as ``{"SP,1": {...}}`` where the key is
``<listCode>,<rank>``, and addresses as ``{"P,1": {...}}`` where the key is
``<typeCode>,<rank>``. These helpers pull out the fields an integration
normally wants, leaving the raw payload untouched for anything else.
"""

from __future__ import annotations


def _rank(key, value):
    """Read a rank from the entry or its compound map key."""
    rank = value.get("rank")
    if rank is None:
        rank = str(key).replace("_", ",").partition(",")[2]
    try:
        return int(rank)
    except (TypeError, ValueError):
        return 0


def _coded_values(container):
    """Normalise a OneKey coded-value map into a list ordered by rank.

    Handles both the ``ada`` and ``qualifications`` containers, which share a
    shape. Returns entries like ``{"listCode": "SP", "rank": 1, "code": "1S",
    "label": "PKA", "corporateLabel": "..."}``.
    """
    if not isinstance(container, dict):
        return []

    entries = []
    for key, value in container.items():
        if not isinstance(value, dict):
            continue
        list_code = value.get("listCode")
        if list_code is None and "," in str(key):
            list_code = str(key).split(",", 1)[0]
        entries.append(
            {
                "listCode": list_code,
                "rank": _rank(key, value),
                "code": value.get("code"),
                "label": value.get("codeLabel") or value.get("label"),
                "corporateLabel": value.get("codeCorporateLabel"),
            }
        )
    return sorted(entries, key=lambda e: (e["listCode"] or "", e["rank"]))


def first_ada_code(entity, list_code: str) -> str | None:
    """Return the first coded value on ``entity.ada`` for ``list_code``."""
    container = (entity or {}).get("ada") or {}
    matches = [
        entry
        for entry in _coded_values(container)
        if (entry.get("listCode") or "").upper() == list_code.upper() and entry.get("code")
    ]
    if not matches:
        return None
    return matches[0]["code"]


def specialties(entity):
    """Return the ``SP`` (specialty) coded values for an individual or workplace."""
    for name in ("ada", "qualifications"):
        matches = [e for e in _coded_values(entity.get(name)) if e["listCode"] == "SP"]
        if matches:
            return matches
    return []


def specialty_slots(entity, slots: int = 5) -> dict:
    """Flatten up to ``slots`` SP codes/labels as specialtyCode1..N / specialtyLabel1..N.

    Also sets ``specialtyCode`` / ``specialtyLabel`` aliases for slot 1.
    """
    specs = specialties(entity) or []
    out = {}
    for i in range(1, slots + 1):
        sp = specs[i - 1] if i - 1 < len(specs) else {}
        out[f"specialtyCode{i}"] = sp.get("code")
        out[f"specialtyLabel{i}"] = sp.get("corporateLabel") or sp.get("label")
    out["specialtyCode"] = out.get("specialtyCode1")
    out["specialtyLabel"] = out.get("specialtyLabel1")
    return out


def _telephone_slots(entity, slots: int = 2) -> dict:
    """Flatten workplace/individual telephones as telephone1..N.

    OneKey keys are typically ``1,1`` / ``1,2`` (type,rank). Also accepts
    underscore forms used in mapping paths (``1_1``).
    """
    phones = (entity or {}).get("telephones") or {}
    out = {"telephone": None, **{f"telephone{i}": None for i in range(1, slots + 1)}}
    if not isinstance(phones, dict) or not phones:
        return out

    ordered = sorted(
        ((key, value) for key, value in phones.items() if isinstance(value, dict)),
        key=lambda item: _rank(*item),
    )
    for i, (_, entry) in enumerate(ordered[:slots], start=1):
        out[f"telephone{i}"] = (
            entry.get("callNumberForSearch")
            or entry.get("callNumber")
            or entry.get("number")
        )
    # Alias first phone as ``telephone`` for older mappings
    out["telephone"] = out.get("telephone1")
    return out


def _denied_accesses(entity) -> dict:
    """Flatten deniedAccesses flags used by entity_mapping (noPhone, …)."""
    denied = (entity or {}).get("deniedAccesses") or {}
    if not isinstance(denied, dict):
        denied = {}
    return {
        "noPhone": denied.get("noPhone"),
        "noEmail": denied.get("noEmail"),
        "noMail": denied.get("noMail"),
        "noFax": denied.get("noFax"),
        "noVisit": denied.get("noVisit"),
    }


_ADDRESS_FIELDS = (
    "addressEid",
    "buildingLabel",
    "postalBox",
    "street",
    "postalCode",
    "city",
    "county",
    "district",
    "country",
    "addressType",
)


def _subdivision_label(town, *type_codes):
    """Return first matching subdivision localized label (e.g. COUNTY, DISTRICT)."""
    subs = (town or {}).get("subdivisions") or {}
    if not isinstance(subs, dict):
        return None
    wanted = {c.upper() for c in type_codes}
    for entry in subs.values():
        if not isinstance(entry, dict):
            continue
        code = (entry.get("typeCode") or entry.get("subdivisionType") or "").upper()
        if code in wanted:
            label = (
                entry.get("longLocalizedLabel")
                or entry.get("longLabel")
                or entry.get("shortLocalizedLabel")
            )
            if label:
                return label
    return None


def primary_address(workplace):
    """Return the flattened primary address of a workplace.

    Prefers the address of type ``P`` (principal) at the lowest rank, falling
    back to whichever address is ranked first. Always returns the same keys,
    valued None when the entity carries no address, so that flattened rows keep
    a stable column set for CSV or dataframe export.

    UK OneKey often omits ``addressLongLabel`` and only sends
    ``addressShortLabel`` (street) plus ``buildingLabel`` on the workplace
    address entry.
    """
    addresses = (workplace or {}).get("workplaceAddresses") or {}
    if not isinstance(addresses, dict) or not addresses:
        return {field: None for field in _ADDRESS_FIELDS}

    def sort_key(item):
        key, value = item
        type_code = value.get("typeCode") or str(key).split(",", 1)[0]
        return (0 if type_code == "P" else 1, _rank(key, value))

    candidates = ((key, value) for key, value in addresses.items() if isinstance(value, dict))
    _, entry = min(candidates, key=sort_key, default=(None, {}))
    return flatten_address_entry(entry)


def flatten_address_entry(entry):
    """Flatten one ``workplaceAddresses`` entry into the address field set."""
    empty = {field: None for field in _ADDRESS_FIELDS}
    entry = entry or {}
    address = entry.get("address") or {}
    town = address.get("postalTownReference") or {}

    street = (
        address.get("addressLongLabel")
        or address.get("addressShortLabel")
        or address.get("extensionLabel")
    )

    return {
        **empty,
        "addressEid": address.get("addressEid"),
        "buildingLabel": entry.get("buildingLabel") or address.get("buildingLabel"),
        "postalBox": entry.get("postalBoxLabel") or address.get("postalBoxLabel"),
        "street": street,
        "postalCode": address.get("longPostalCode") or address.get("postalCode"),
        "city": town.get("villageLabel") or town.get("dispatchLabel"),
        "county": _subdivision_label(town, "COUNTY", "SUB.3"),
        "district": _subdivision_label(town, "DISTRICT", "SUB.5"),
        "country": town.get("country"),
        "addressType": entry.get("typeCode"),
    }


def compose_hcp_result(individual_record=None, activity_record=None, workplace_record=None):
    """Build a search-shaped ACTIVITY result from population entity rows.

    ``/pop/refreshMyOnekey`` returns WORKPLACE, INDIVIDUAL, and ACTIVITY as
    separate rows. ACTIVITY rows carry only the ``activity`` object (with
    ``individualEid`` / ``workplaceEid`` links); person and workplace detail
    live on the other entity types. Search (``/ok/search``) embeds all three.
    This helper reassembles the nested shape ``flatten_hcp`` expects.
    """
    activity_record = activity_record or {}
    individual_record = individual_record or {}
    workplace_record = workplace_record or {}
    activity = activity_record.get("activity") or {}
    individual = individual_record.get("individual") or {}
    workplace = workplace_record.get("workplace") or {}

    return {
        "onekeyEid": activity_record.get("onekeyEid") or activity.get("activityEid"),
        "entityType": "ACTIVITY",
        "country": (
            individual_record.get("country")
            or activity_record.get("country")
            or workplace_record.get("country")
        ),
        "codBase": (
            individual_record.get("codBase")
            or activity_record.get("codBase")
            or workplace_record.get("codBase")
        ),
        "isInContract": activity_record.get("isInContract"),
        "isPrivacyLaw": (
            individual_record.get("isPrivacyLaw")
            if individual_record.get("isPrivacyLaw") is not None
            else activity_record.get("isPrivacyLaw")
        ),
        "activity": activity,
        "individual": individual,
        "workplace": workplace,
    }


def flatten_hcp(result):
    """Flatten one ACTIVITY result into a healthcare-professional row.

    An activity ties an individual to a workplace, so the row carries the
    person, their role there, and that workplace's address. The same individual
    appears once per workplace they practise at, each with its own
    ``activityEid``.

    Population ACTIVITY rows may omit nested ``individual`` / ``workplace``;
    EIDs then fall back to fields on ``activity``.
    """
    individual = result.get("individual") or {}
    activity = result.get("activity") or {}
    workplace = result.get("workplace") or {}
    status = individual.get("statusCode") or activity.get("statusCode")

    return {
        "onekeyEid": result.get("onekeyEid"),
        "activityEid": activity.get("activityEid"),
        "individualEid": individual.get("individualEid") or activity.get("individualEid"),
        "workplaceEid": workplace.get("workplaceEid") or activity.get("workplaceEid"),
        "firstName": individual.get("firstName"),
        "lastName": individual.get("lastName"),
        "gender": individual.get("genderLabel"),
        "genderCode": individual.get("genderCode"),
        "title": individual.get("titleCode"),
        "typeCode": individual.get("typeCode"),
        "typeLabel": individual.get("typeCorporateLabel") or individual.get("typeLabel"),
        **specialty_slots(individual),
        "statusCode": status,
        # entity_mapping.csv uses stateCode; OneKey sends statusCode
        "stateCode": individual.get("stateCode") or status,
        "preferredLanguageEID": individual.get("preferredLanguageEID"),
        "isValid": status == "3",
        "usualName": workplace.get("usualName"),
        "workplaceName": workplace.get("officialName") or workplace.get("usualName"),
        "country": result.get("country"),
        "codBase": result.get("codBase"),
        "isInContract": result.get("isInContract"),
        "isPrivacyLaw": result.get("isPrivacyLaw"),
        **{f"address_{k}": v for k, v in primary_address(workplace).items()},
    }


def flatten_hco(result):
    """Flatten one WORKPLACE result into a healthcare-organisation row."""
    workplace = result.get("workplace") or result
    status = workplace.get("statusCode")

    return {
        "onekeyEid": result.get("onekeyEid") or workplace.get("workplaceEid"),
        "workplaceEid": workplace.get("workplaceEid"),
        "officialName": workplace.get("officialName"),
        "usualName": workplace.get("usualName"),
        "typeCode": workplace.get("typeCode"),
        "typeLabel": workplace.get("typeCorporateLabel") or workplace.get("typeLabel"),
        "activityLocationCode": workplace.get("activityLocationCode"),
        "legalCategoryCode": first_ada_code(workplace, "CAJ"),
        "structureTypeCode": workplace.get("structureTypeCode"),
        **specialty_slots(workplace),
        "statusCode": status,
        # entity_mapping.csv uses stateCode; OneKey workplace uses statusCode
        "stateCode": workplace.get("stateCode") or status,
        "isValid": status == "3",
        "parentWorkplaceEid": workplace.get("managerWorkplaceEid"),
        "parentWorkplaceName": workplace.get("managerWorkplaceOfficialName"),
        "country": result.get("country"),
        "codBase": result.get("codBase"),
        "isPrivacyLaw": (
            workplace.get("isPrivacyLaw")
            if workplace.get("isPrivacyLaw") is not None
            else result.get("isPrivacyLaw")
        ),
        **_telephone_slots(workplace),
        **_denied_accesses(workplace),
        **{f"address_{k}": v for k, v in primary_address(workplace).items()},
    }
