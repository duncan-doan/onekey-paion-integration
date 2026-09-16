"""High-level client for retrieving HCP and HCO records from OneKey."""

import logging
import time

from .auth import authenticate

logger = logging.getLogger(__name__)

# Matching strategies accepted by the `method` property of a search field.
EXACT = "EXACT"
CONTAINS = "CONTAINS"
STARTS_WITH = "STARTS_WITH"
PHONETIC = "PHONETIC"
FUZZY = "FUZZY"
SEARCH_METHODS = frozenset({EXACT, CONTAINS, STARTS_WITH, PHONETIC, FUZZY})

# OneKey entity types.
#
# An *activity* is the link between an individual and the workplace they
# practise at (Developer Guide, glossary). Searching ACTIVITY is how you get at
# healthcare professionals, because it returns the individual, the workplace and
# the affiliation together. A practitioner working in three places has three
# activities.
ACTIVITY = "ACTIVITY"
WORKPLACE = "WORKPLACE"
INDIVIDUAL = "INDIVIDUAL"

# Status code 3 == "Valid" in the STA code list; the usual filter for live data.
STATUS_VALID = "3"


def search_field(name, values, method=EXACT, fuzzy_precision=None):
    """Build one criterion for the ``fields`` array of a search request.

    Field names are case sensitive (``codBase`` is not ``CODBASE``). Values
    inside a single criterion are OR'ed together; separate criteria are AND'ed
    unless you splice a literal ``"OR"`` between them.
    """
    if method not in SEARCH_METHODS:
        raise ValueError(
            f"Unknown search method {method!r}; expected one of "
            + ", ".join(sorted(SEARCH_METHODS))
        )
    if isinstance(values, (str, int)):
        values = [values]

    field = {"name": name, "method": method, "values": [str(v) for v in values]}
    if fuzzy_precision is not None:
        field["fuzzyPrecision"] = fuzzy_precision
    return field


class OneKeyClient:
    """Retrieve healthcare professionals and organisations from OneKey.

    Example:
        >>> client = OneKeyClient.connect(
        ...     base_url="https://okws-uat.onekey.iqvia.com",
        ...     username="494_PAION_API_UAT",
        ...     password="...",
        ...     connector_id="494",
        ... )
        >>> hcps = client.search_hcp(cod_base="WDE", last_name="Muller")
    """

    def __init__(self, session):
        self.session = session
        self.config = session.config

    @classmethod
    def connect(cls, base_url, username, password, connector_id, **kwargs):
        """Authenticate and return a client in one step."""
        return cls(
            authenticate(base_url, username, password, connector_id, **kwargs)
        )

    # -- core search ------------------------------------------------------

    def search(
        self,
        entity_type,
        fields,
        cod_base=None,
        iso_cod2=None,
        result_size=10,
        is_in_contract=None,
        extra=None,
    ):
        """Run ``/ok/search`` and return the list of matched entities.

        Args:
            entity_type: :data:`ACTIVITY` (HCPs) or :data:`WORKPLACE` (HCOs).
            fields: Criteria from :func:`search_field`. May contain a literal
                ``"OR"`` string between two criteria to switch the operator.
                OneKey caps a single search at 30 criteria.
            cod_base: OneKey territory code, e.g. ``WDE``. Pass this *or*
                ``iso_cod2``, never both.
            iso_cod2: ISO 3166-1 alpha-2 country, e.g. ``DE``.
            result_size: Maximum records to return.
            is_in_contract: True restricts the search to the connector's own
                subscribed population instead of the whole country database.
            extra: Additional top-level keys merged into the request body.

        Returns:
            list[dict]: the raw ``results`` entries.

        Note:
            ``/ok/search`` has no offset or cursor -- ``resultSize`` is the only
            control, so it cannot page through a large set. Use
            :meth:`count` first to size a query, narrow the criteria, or use the
            ``/pop/refreshMyOnekey`` packet transaction for bulk extraction.
        """
        payload = self._build_payload(
            entity_type, fields, cod_base, iso_cod2, result_size, is_in_contract, extra
        )
        envelope = self.session.post_json("/ok/search", payload)

        results = envelope.get("results") or []
        # Most endpoints return a list; a few key-based ones return a dict map.
        if isinstance(results, dict):
            results = list(results.values())

        total = envelope.get("totalNumberOfResults")
        if total is not None and total > len(results):
            logger.info(
                "OneKey matched %s entities; %d returned (resultSize=%s). "
                "Narrow the criteria to see the rest.",
                total,
                len(results),
                result_size,
            )
        return results

    def count(
        self,
        entity_type,
        fields,
        cod_base=None,
        iso_cod2=None,
        extra=None,
    ):
        """Return how many entities match, without retrieving them.

        Cheaper than a search and does not count against the subscription, so
        it is the right way to size a query before pulling records.

        Note:
            ``/ok/count`` is subscribed per connection point and is not enabled
            on every connector -- it raises
            :class:`~onekey.exceptions.OneKeyForbiddenError` where it is not.
            :meth:`search` reports ``totalNumberOfResults`` as a fallback.
        """
        payload = self._build_payload(
            entity_type, fields, cod_base, iso_cod2, None, None, extra
        )
        envelope = self.session.post_json("/ok/count", payload)
        return envelope.get("resultSize", 0)

    def _build_payload(
        self, entity_type, fields, cod_base, iso_cod2, result_size, is_in_contract, extra
    ):
        if cod_base and iso_cod2:
            raise ValueError(
                "Pass cod_base or iso_cod2, not both -- OneKey rejects requests "
                "carrying both even when they agree."
            )
        if not cod_base and not iso_cod2:
            raise ValueError("One of cod_base or iso_cod2 is required.")

        payload = {"entityType": entity_type, "fields": list(fields)}
        if cod_base:
            payload["codBase"] = (
                [cod_base] if isinstance(cod_base, str) else list(cod_base)
            )
        if iso_cod2:
            payload["isoCod2"] = iso_cod2
        if result_size is not None:
            payload["resultSize"] = int(result_size)
        if is_in_contract is not None:
            payload["isInContract"] = bool(is_in_contract)
        if extra:
            payload.update(extra)
        return payload

    # -- HCP (healthcare professionals) -----------------------------------

    def search_hcp(
        self,
        cod_base=None,
        iso_cod2=None,
        last_name=None,
        first_name=None,
        specialty=None,
        city=None,
        email=None,
        method=STARTS_WITH,
        only_valid=True,
        result_size=10,
        fields=None,
        **kwargs
    ):
        """Search healthcare professionals.

        Convenience wrapper over :meth:`search` with ``entityType=ACTIVITY``,
        which returns the individual together with the workplace they practise
        at and the activity linking them.

        Any criterion left as None is omitted. Pass ``fields`` to supply raw
        criteria instead of (or in addition to) the named arguments.
        """
        criteria = list(fields or [])
        if last_name:
            criteria.append(search_field("individual.lastName", last_name, method))
        if first_name:
            criteria.append(search_field("individual.firstName", first_name, method))
        if specialty:
            criteria.append(
                search_field("individual.qualifications[SP].code", specialty, EXACT)
            )
        if city:
            criteria.append(search_field("address.villageLabel", city, CONTAINS))
        if email:
            criteria.append(
                search_field("activityOrIndividual.emailAddress", email, CONTAINS)
            )
        if only_valid:
            criteria.append(search_field("activity.statusCode", STATUS_VALID, EXACT))

        if not criteria:
            raise ValueError("search_hcp needs at least one criterion.")

        return self.search(
            ACTIVITY,
            criteria,
            cod_base=cod_base,
            iso_cod2=iso_cod2,
            result_size=result_size,
            **kwargs
        )

    # -- HCO (healthcare organisations) -----------------------------------

    def search_hco(
        self,
        cod_base=None,
        iso_cod2=None,
        name=None,
        city=None,
        postal_code=None,
        specialty=None,
        method=CONTAINS,
        only_valid=True,
        result_size=10,
        fields=None,
        **kwargs
    ):
        """Search healthcare organisations (workplaces).

        Convenience wrapper over :meth:`search` with ``entityType=WORKPLACE``:
        hospitals, clinics, pharmacies, practices and the like.
        """
        criteria = list(fields or [])
        if name:
            criteria.append(search_field("workplace.officialName", name, method))
        if city:
            criteria.append(search_field("address.villageLabel", city, CONTAINS))
        if postal_code:
            criteria.append(search_field("address.longPostalCode", postal_code, EXACT))
        if specialty:
            criteria.append(
                search_field("workplace.qualifications.code", specialty, EXACT)
            )
        if only_valid:
            criteria.append(search_field("workplace.statusCode", STATUS_VALID, EXACT))

        if not criteria:
            raise ValueError("search_hco needs at least one criterion.")

        return self.search(
            WORKPLACE,
            criteria,
            cod_base=cod_base,
            iso_cod2=iso_cod2,
            result_size=result_size,
            **kwargs
        )

    # -- retrieval by OneKey id -------------------------------------------

    def get_by_id(self, onekey_eid, entity_type=ACTIVITY, cod_base=None, iso_cod2=None):
        """Fetch one entity by its OneKey id, with full subscribed detail.

        Searching by id returns every attribute contracted for the connector,
        where a search by criteria returns only a thin identifying subset.
        OneKey accounts records fetched this way against the subscription, and
        an id must be the *only* criterion in the request.
        """
        field_name = {
            ACTIVITY: "activity.activityEid",
            WORKPLACE: "workplace.workplaceEid",
            INDIVIDUAL: "individual.individualEid",
        }[entity_type]

        if not cod_base and not iso_cod2:
            # OneKey ids are prefixed with their codBase, e.g. WDER00117070.
            cod_base = onekey_eid[:3].upper()

        results = self.search(
            entity_type,
            [search_field(field_name, onekey_eid, EXACT)],
            cod_base=cod_base,
            iso_cod2=iso_cod2,
            result_size=1,
        )
        return results[0] if results else None

    # -- bulk extraction --------------------------------------------------

    def iter_population(
        self,
        cod_base=None,
        iso_cod2=None,
        date_from=None,
        date_to=None,
        page_size=200,
        max_records=None,
    ):
        """Yield every entity in this connector's population, packet by packet.

        This is the only endpoint that pages, and therefore the only way to pull
        the full HCP/HCO set: ``/ok/search`` is capped by ``resultSize`` with no
        offset. OneKey opens a transaction on the first call and returns a
        ``transactionUniqueIdentifier``; each subsequent call replays that id to
        collect the next packet until ``isTransactionComplete`` is true.

        Omitting the dates returns the whole population. Supplying them returns
        only entities changed in that window, which is what a nightly delta sync
        should do.

        Args:
            cod_base: OneKey territory code, e.g. ``WDE``.
            iso_cod2: ISO alpha-2 country, as an alternative to ``cod_base``.
            date_from: ISO-8601 start, e.g. ``2026-01-01T00:00:00Z``.
            date_to: ISO-8601 end.
            page_size: Records per packet. OneKey accepts 100-2000.
            max_records: Stop after roughly this many records. Useful for
                smoke tests; leave None to walk the whole population.

        Yields:
            dict: raw entity records. Pass each through
            :func:`~onekey.parse.flatten_hcp` or
            :func:`~onekey.parse.flatten_hco` depending on ``entityType``.
        """
        if not cod_base and not iso_cod2:
            raise ValueError("One of cod_base or iso_cod2 is required.")
        if not 100 <= page_size <= 2000:
            raise ValueError("page_size must be between 100 and 2000.")

        payload = {"resultSize": int(page_size)}
        if cod_base:
            payload["codBase"] = cod_base
        if iso_cod2:
            payload["isoCod2"] = iso_cod2
        if date_from:
            payload["isoDateFrom"] = date_from
        if date_to:
            payload["isoDateTo"] = date_to

        yielded = 0
        while True:
            envelope = self.session.post_json("/pop/refreshMyOnekey", payload)
            results = envelope.get("results") or []
            if isinstance(results, dict):
                results = list(results.values())

            for record in results:
                yield record
                yielded += 1
                if max_records is not None and yielded >= max_records:
                    return

            if envelope.get("isTransactionComplete") or not results:
                return

            transaction_id = envelope.get("transactionUniqueIdentifier")
            if not transaction_id:
                logger.warning(
                    "OneKey did not return a transaction id; stopping after %d "
                    "records to avoid replaying the first packet forever.",
                    yielded,
                )
                return

            # Pace packets to stay under OneKey rate limits.
            delay = getattr(self.session.config, "packet_delay", 0) or 0
            if delay > 0:
                time.sleep(delay)

            # Replaying the transaction id is what advances the cursor.
            # The territory must be repeated on every continuation: OneKey
            # rejects a continuation that omits it with a 403, not the 400 the
            # missing parameter would suggest.
            payload = {
                "resultSize": int(page_size),
                "transactionUniqueIdentifier": transaction_id,
            }
            if cod_base:
                payload["codBase"] = cod_base
            if iso_cod2:
                payload["isoCod2"] = iso_cod2

    def close(self):
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()
