"""Map OneKey entities onto Dynamics 365 / Dataverse tables.

    OneKey workplace  (HCO) -> account
    OneKey individual (HCP) -> contact

Identity
--------
Re-running a sync must update records rather than duplicate them, so each row
carries its OneKey id:

    account.cr4ec_externalid       <- workplaceEid   (max 100 chars; KEY)
    contact.externaluseridentifier <- individualEid  (max 50 chars)

``cr4ec_externalid`` is the org alternate key (``cr4ec_onekeyid``). Account
number is no longer the upsert key. ``externaluseridentifier`` is borrowed on
the contact side as the closest stock external-identity field.

Addresses
---------
Addresses are **not** loaded. Only identity, classification and the
account/contact relationship are written.

This is not just a preference: the target environment cannot accept them. An
alternate key ``cr4ec_externalkey`` is defined on
``customeraddress.addressnumber``, which forces that column to be unique across
the whole org -- but Dynamics numbers every record's addresses 1 and 2, so the
second address row of any kind violates it. Writing any ``address1_*`` field
fails with SQL error 2601.

The mapping is retained behind ``include_address=True`` so it can be switched
on without rework once that key is removed.
"""

# Dataverse string columns are hard-limited; the Web API rejects an oversized
# value outright rather than truncating, so trim on the way in.
MAX_LENGTHS = {
    "account": {
        "name": 160,
        "cr4ec_externalid": 100,
        "accountnumber": 20,
        "address1_line1": 250,
        "address1_city": 80,
        "address1_postalcode": 20,
        "address1_country": 80,
        "telephone1": 50,
        "emailaddress1": 100,
        "websiteurl": 200,
    },
    "contact": {
        "firstname": 50,
        "lastname": 50,
        "externaluseridentifier": 50,
        "jobtitle": 100,
        "address1_line1": 250,
        "address1_city": 80,
        "address1_postalcode": 20,
        "address1_country": 80,
        "telephone1": 50,
        "emailaddress1": 100,
    },
}

# contact.gendercode is a picklist: 1 Male, 2 Female. OneKey uses M/F.
GENDER_CODES = {"M": 1, "F": 2}

# account.address1_addresstypecode: 3 == Primary.
ADDRESS_TYPE_PRIMARY = 3

ACCOUNT_KEY_FIELD = "cr4ec_externalid"
CONTACT_KEY_FIELD = "externaluseridentifier"


def _trim(value, table, field):
    """Return ``value`` as a string clipped to the column's limit, or None."""
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    limit = MAX_LENGTHS.get(table, {}).get(field)
    return text[:limit] if limit else text


def _without_nulls(record):
    """Drop None values so Dataverse keeps existing data instead of clearing it."""
    return {k: v for k, v in record.items() if v is not None}


def hco_to_account(hco, include_address=False):
    """Map a flattened OneKey HCO onto an ``account`` record.

    Args:
        hco: Output of :func:`onekey.parse.flatten_hco`.
        include_address: Off by default -- the target org rejects address
            writes (see the module docstring). Set True once the
            ``customeraddress`` alternate key is removed.

    Returns:
        dict: a Web API payload for ``POST /accounts``.
    """
    name = hco.get("officialName") or hco.get("usualName")
    description_parts = [
        f"Type: {hco['typeLabel']}" if hco.get("typeLabel") else None,
        f"Specialty: {hco['specialtyLabel']}" if hco.get("specialtyLabel") else None,
        f"OneKey ID: {hco.get('workplaceEid')}" if hco.get("workplaceEid") else None,
        f"Source: IQVIA OneKey ({hco.get('codBase')})" if hco.get("codBase") else None,
    ]

    record = {
        "name": _trim(name, "account", "name"),
        ACCOUNT_KEY_FIELD: _trim(hco.get("workplaceEid"), "account", ACCOUNT_KEY_FIELD),
        "description": "\n".join(p for p in description_parts if p) or None,
    }

    if include_address:
        record.update({
            "address1_line1": _trim(hco.get("address_street"), "account", "address1_line1"),
            "address1_city": _trim(hco.get("address_city"), "account", "address1_city"),
            "address1_postalcode": _trim(
                hco.get("address_postalCode"), "account", "address1_postalcode"
            ),
            "address1_country": _trim(
                hco.get("address_country"), "account", "address1_country"
            ),
        })
        if record.get("address1_city") or record.get("address1_line1"):
            record["address1_addresstypecode"] = ADDRESS_TYPE_PRIMARY
    return _without_nulls(record)


def hcp_to_contact(hcp, account_id=None, include_address=False):
    """Map a flattened OneKey HCP onto a ``contact`` record.

    Args:
        hcp: Output of :func:`onekey.parse.flatten_hcp`.
        account_id: GUID of the already-created ``account`` for this person's
            workplace. When given, the contact is linked to it as the parent
            customer, which is what makes the HCP visible under the HCO in the
            CRM UI.
        include_address: Off by default -- the target org rejects address
            writes (see the module docstring). Set True once the
            ``customeraddress`` alternate key is removed.

    Returns:
        dict: a Web API payload for ``POST /contacts``.
    """
    description_parts = [
        f"Specialty: {hcp['specialtyLabel']}" if hcp.get("specialtyLabel") else None,
        f"Type: {hcp['typeLabel']}" if hcp.get("typeLabel") else None,
        f"OneKey individual: {hcp.get('individualEid')}"
        if hcp.get("individualEid") else None,
        f"OneKey activity: {hcp.get('activityEid')}" if hcp.get("activityEid") else None,
        f"Workplace: {hcp['workplaceName']}" if hcp.get("workplaceName") else None,
        f"Source: IQVIA OneKey ({hcp.get('codBase')})" if hcp.get("codBase") else None,
    ]

    record = {
        "firstname": _trim(hcp.get("firstName"), "contact", "firstname"),
        "lastname": _trim(hcp.get("lastName"), "contact", "lastname"),
        CONTACT_KEY_FIELD: _trim(
            hcp.get("individualEid"), "contact", CONTACT_KEY_FIELD
        ),
        "jobtitle": _trim(
            hcp.get("specialtyLabel") or hcp.get("typeLabel"), "contact", "jobtitle"
        ),
        "description": "\n".join(p for p in description_parts if p) or None,
    }

    if include_address:
        record.update({
            "address1_line1": _trim(hcp.get("address_street"), "contact", "address1_line1"),
            "address1_city": _trim(hcp.get("address_city"), "contact", "address1_city"),
            "address1_postalcode": _trim(
                hcp.get("address_postalCode"), "contact", "address1_postalcode"
            ),
            "address1_country": _trim(
                hcp.get("address_country"), "contact", "address1_country"
            ),
        })

    gender = GENDER_CODES.get((hcp.get("genderCode") or "").upper())
    if gender:
        record["gendercode"] = gender

    # Dataverse binds lookups by URL fragment, not by raw GUID.
    if account_id:
        record["parentcustomerid_account@odata.bind"] = f"/accounts({account_id})"

    # lastname is mandatory on contact; fall back so a create cannot fail on it.
    if not record.get("lastname"):
        record["lastname"] = _trim(
            hcp.get("individualEid") or "Unknown", "contact", "lastname"
        )

    return _without_nulls(record)
