"""Flatten OneKey's nested search results into plain rows.

OneKey responses are deeply nested and use compound dictionary keys rather than
lists: qualifications arrive as ``{"SP,1": {...}}`` where the key is
``<listCode>,<rank>``, and addresses as ``{"P,1": {...}}`` where the key is
``<typeCode>,<rank>``. These helpers pull out the fields an integration
normally wants, leaving the raw payload untouched for anything else.
"""


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
        rank = value.get("rank")
        if list_code is None and "," in key:
            list_code = key.split(",", 1)[0]
        entries.append(
            {
                "listCode": list_code,
                "rank": rank if rank is not None else 0,
                "code": value.get("code"),
                "label": value.get("codeLabel") or value.get("label"),
                "corporateLabel": value.get("codeCorporateLabel"),
            }
        )
    return sorted(entries, key=lambda e: (e["listCode"] or "", e["rank"]))


def specialties(entity):
    """Return the ``SP`` (specialty) coded values for an individual or workplace."""
    container = entity.get("ada") or entity.get("qualifications") or {}
    return [e for e in _coded_values(container) if e["listCode"] == "SP"]


_ADDRESS_FIELDS = (
    "addressEid",
    "buildingLabel",
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
    empty = {field: None for field in _ADDRESS_FIELDS}

    addresses = (workplace or {}).get("workplaceAddresses") or {}
    if not isinstance(addresses, dict) or not addresses:
        return empty

    def sort_key(item):
        key, value = item
        type_code = value.get("typeCode") or key.split(",", 1)[0]
        return (0 if type_code == "P" else 1, value.get("rank") or 0)

    _, entry = sorted(addresses.items(), key=sort_key)[0]
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
    specialty = (specialties(individual) or [{}])[0]

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
        "specialtyCode": specialty.get("code"),
        "specialtyLabel": specialty.get("corporateLabel") or specialty.get("label"),
        "statusCode": individual.get("statusCode") or activity.get("statusCode"),
        "isValid": (individual.get("statusCode") or activity.get("statusCode")) == "3",
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
    specialty = (specialties(workplace) or [{}])[0]

    return {
        "onekeyEid": result.get("onekeyEid") or workplace.get("workplaceEid"),
        "workplaceEid": workplace.get("workplaceEid"),
        "officialName": workplace.get("officialName"),
        "usualName": workplace.get("usualName"),
        "typeCode": workplace.get("typeCode"),
        "typeLabel": workplace.get("typeCorporateLabel") or workplace.get("typeLabel"),
        "activityLocationCode": workplace.get("activityLocationCode"),
        "specialtyCode": specialty.get("code"),
        "specialtyLabel": specialty.get("corporateLabel") or specialty.get("label"),
        "statusCode": workplace.get("statusCode"),
        "isValid": workplace.get("statusCode") == "3",
        "parentWorkplaceEid": workplace.get("managerWorkplaceEid"),
        "parentWorkplaceName": workplace.get("managerWorkplaceOfficialName"),
        "country": result.get("country"),
        "codBase": result.get("codBase"),
        "isPrivacyLaw": workplace.get("isPrivacyLaw"),
        **{f"address_{k}": v for k, v in primary_address(workplace).items()},
    }
