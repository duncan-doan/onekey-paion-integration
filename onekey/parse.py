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
    "street",
    "postalCode",
    "city",
    "country",
    "addressType",
)


def primary_address(workplace):
    """Return the flattened primary address of a workplace.

    Prefers the address of type ``P`` (principal) at the lowest rank, falling
    back to whichever address is ranked first. Always returns the same keys,
    valued None when the entity carries no address, so that flattened rows keep
    a stable column set for CSV or dataframe export.
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

    return {
        **empty,
        "addressEid": address.get("addressEid"),
        "street": address.get("addressLongLabel"),
        "postalCode": address.get("longPostalCode"),
        "city": town.get("villageLabel") or town.get("dispatchLabel"),
        "country": town.get("country"),
        "addressType": entry.get("typeCode"),
    }


def flatten_hcp(result):
    """Flatten one ACTIVITY result into a healthcare-professional row.

    An activity ties an individual to a workplace, so the row carries the
    person, their role there, and that workplace's address. The same individual
    appears once per workplace they practise at, each with its own
    ``activityEid``.
    """
    individual = result.get("individual") or {}
    activity = result.get("activity") or {}
    workplace = result.get("workplace") or {}
    specialty = (specialties(individual) or [{}])[0]

    return {
        "onekeyEid": result.get("onekeyEid"),
        "activityEid": activity.get("activityEid"),
        "individualEid": individual.get("individualEid"),
        "workplaceEid": workplace.get("workplaceEid"),
        "firstName": individual.get("firstName"),
        "lastName": individual.get("lastName"),
        "gender": individual.get("genderLabel"),
        "genderCode": individual.get("genderCode"),
        "title": individual.get("titleCode"),
        "typeCode": individual.get("typeCode"),
        "typeLabel": individual.get("typeCorporateLabel") or individual.get("typeLabel"),
        "specialtyCode": specialty.get("code"),
        "specialtyLabel": specialty.get("corporateLabel") or specialty.get("label"),
        "statusCode": individual.get("statusCode"),
        "isValid": individual.get("statusCode") == "3",
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
