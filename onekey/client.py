"""High-level client for retrieving HCP and HCO records from OneKey."""

import logging
import time

from .auth import authenticate
from .exceptions import OneKeyAPIResponseError, OneKeyHTTPError, OneKeyResponseLostError

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

# Each entity type's own OneKey ID field. A search by it returns the entity's
# full contracted detail and must carry no other criterion (Developer Guide
# 6.3.2). A related ID, such as workplace.workplaceEid in an ACTIVITY search,
# is an ordinary filter.
ID_FIELDS = {
    ACTIVITY: "activity.activityEid",
    WORKPLACE: "workplace.workplaceEid",
    INDIVIDUAL: "individual.individualEid",
}

# A refreshMyOnekey packet answered with success=false was not delivered; the
# vendor sample waits and asks for it again (Developer Guide 6.13). Requests
# per packet, and seconds to wait before each repeat (times the attempt).
_PACKET_ATTEMPTS = 3
_PACKET_RETRY_SECONDS = 5

# Packets lost in transit that a walk re-fetches by number before giving up.
_MAX_MISSED_PACKETS = 3
# Times a walk resumes after a continuation's answer was lost (6.13.4).
_RESUME_ATTEMPTS = 3


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
                OneKey caps a single search at 30 criteria (START HERE Guide 6.1.2).
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
                "OneKey search truncated: entity=%s matched=%s returned=%d result_size=%s",
                entity_type,
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

        criteria = list(fields)
        search_fields = [field for field in criteria if isinstance(field, dict)]
        if len(search_fields) > 30:
            raise ValueError("OneKey accepts at most 30 search criteria.")
        own_id = ID_FIELDS.get(entity_type)
        if own_id and len(criteria) != 1 and any(
            field.get("name") == own_id for field in search_fields
        ):
            raise ValueError(f"A search by {own_id} must have no other criterion.")

        payload = {"entityType": entity_type, "fields": criteria}
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

    def get_code_list(self, cod_base, list_external_id, iso_language="en"):
        """Return the codes of one OneKey code list (``/ok/searchCodeList``).

        ``list_external_id`` is the list code (``TET``, ``SP``, ``TIT``, …),
        matched exactly on ``list.externalId``. OneKey returns every code
        without a ``resultSize`` (456 for ``WUK.TIH``).

        The response keys lists by uid (``WUK.TIT``) and, inside each, codes
        by uid (``WUK.TIT.02``). Each code dict carries ``externalId``,
        ``longLabel``, ``shortLabel``, ``isValid`` and ``uid``; the
        integration builds its own external id from ``lookup_table_mapping.csv``
        rather than using ``uid``.

        Returns:
            list[dict]: the codes, invalid ones included, so records that
            still reference an old code keep resolving their lookup.
        """
        payload = {
            "codBase": cod_base,
            "isoLanguage": iso_language or "en",
            "fields": [search_field("list.externalId", [list_external_id], EXACT)],
        }
        envelope = self.session.post_json("/ok/searchCodeList", payload)
        lists = envelope.get("results") or {}
        if isinstance(lists, dict):
            lists = list(lists.values())
        codes = []
        for code_list in lists:
            if not isinstance(code_list, dict):
                continue
            if code_list.get("externalId") not in (None, list_external_id):
                continue
            entries = code_list.get("codes") or {}
            if isinstance(entries, dict):
                entries = list(entries.values())
            codes.extend(code for code in entries if isinstance(code, dict))
        return codes

    # -- retrieval by OneKey id -------------------------------------------

    def get_by_id(self, onekey_eid, entity_type=ACTIVITY, cod_base=None, iso_cod2=None):
        """Fetch one entity by its OneKey id, with full subscribed detail.

        Searching by id returns every attribute contracted for the connector,
        where a search by criteria returns only a thin identifying subset.
        OneKey accounts records fetched this way against the subscription, and
        an id must be the *only* criterion in the request.

        Callers that enforce the contracted population must check the EID
        against ``/pop/refreshMyOnekey`` first and skip IDs not in that set —
        search can still return out-of-contract entities.
        """
        field_name = ID_FIELDS[entity_type]

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
        page_size=None,
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
            page_size: Requested records per packet, 100-2000. OneKey cuts a
                larger request down to the connector's own cap (commonly 250,
                START HERE Guide 6.1.14), so asking for more costs nothing
                while asking for less adds packets. Defaults to the session
                config's ``page_size`` (``ONEKEY_PAGE_SIZE``, 1000), or 200
                without one.
            max_records: Stop after roughly this many records. Useful for
                smoke tests; leave None to walk the whole population.

        Yields:
            dict: raw entity records. Pass each through
            :func:`~onekey.parse.flatten_hcp` or
            :func:`~onekey.parse.flatten_hco` depending on ``entityType``.
        """
        if not cod_base and not iso_cod2:
            raise ValueError("One of cod_base or iso_cod2 is required.")
        if cod_base and iso_cod2:
            raise ValueError("Pass cod_base or iso_cod2, not both.")
        if page_size is None:
            page_size = getattr(getattr(self.session, "config", None), "page_size", None) or 200
        if not 100 <= page_size <= 2000:
            raise ValueError("page_size must be between 100 and 2000.")
        if max_records is not None and max_records <= 0:
            return

        # The territory must be repeated on every continuation: OneKey rejects
        # a continuation that omits it with a 403, not the 400 the missing
        # parameter would suggest.
        request = {"resultSize": int(page_size)}
        if cod_base:
            request["codBase"] = cod_base
        if iso_cod2:
            request["isoCod2"] = iso_cod2
        payload = dict(request)
        if date_from:
            payload["isoDateFrom"] = date_from
        if date_to:
            payload["isoDateTo"] = date_to

        yielded = 0
        packet = 0
        logger.info(
            "OneKey population walk started: codbase=%s page_size=%d "
            "date_from=%s date_to=%s",
            cod_base or iso_cod2,
            page_size,
            date_from,
            date_to,
        )
        while True:
            packet += 1
            if "transactionUniqueIdentifier" in payload:
                results, envelope = self._next_packet(payload, packet)
            else:
                results, envelope = self._population_packet(payload)
            complete = envelope.get("isTransactionComplete")
            transaction_id = envelope.get("transactionUniqueIdentifier")
            if not isinstance(complete, bool):
                raise OneKeyAPIResponseError(
                    "OneKey population omitted a boolean isTransactionComplete"
                )
            packets = []
            received = self._packet_number(envelope)
            if received is not None and received != packet:
                # The transaction id is the same for the whole walk; a final
                # packet may come back without it.
                packets.extend(
                    self._recover_packets(
                        packet,
                        received,
                        transaction_id or payload.get("transactionUniqueIdentifier"),
                        request,
                    )
                )
                packet = received
            if not complete and (not results or not transaction_id):
                # A silent stop would let the caller mark a partial population
                # successful and advance its delta checkpoint.
                raise OneKeyAPIResponseError(
                    "OneKey population is incomplete but returned no records or transaction id"
                )
            packets.append(results)

            for records in packets:
                for record in records:
                    yield record
                    yielded += 1
                    if max_records is not None and yielded >= max_records:
                        logger.info(
                            "OneKey population walk stopped at max_records: records=%d packet=%d",
                            max_records,
                            packet,
                        )
                        return

            logger.info(
                "OneKey packet %d: records=%d total=%d complete=%s | codbase=%s",
                packet,
                len(results),
                yielded,
                complete,
                cod_base or iso_cod2,
            )

            if complete:
                return

            # Pace packets to stay under OneKey rate limits.
            delay = getattr(self.session.config, "packet_delay", 0) or 0
            if delay > 0:
                time.sleep(delay)

            # Replaying the transaction id is what advances the cursor.
            payload = {**request, "transactionUniqueIdentifier": transaction_id}

    def _population_packet(self, payload, resend_lost=True):
        """Request one refreshMyOnekey packet; return (results, envelope).

        A packet answered with ``success: false`` was not delivered, so it is
        requested again, as the vendor sample does. A malformed result list
        fails instead of truncating the walk.
        """
        for attempt in range(1, _PACKET_ATTEMPTS + 1):
            envelope = self.session.post_json(
                "/pop/refreshMyOnekey", payload, resend_lost=resend_lost
            )
            if envelope.get("success") is not False:
                break
            message = str(envelope.get("statusMessage") or "success=false")[:300]
            if attempt == _PACKET_ATTEMPTS:
                raise OneKeyAPIResponseError(
                    f"OneKey population packet was not delivered after {attempt} "
                    f"attempts: {message}",
                    status=str(envelope.get("status") or ""),
                )
            logger.warning(
                "OneKey population packet not delivered, requesting it again: "
                "attempt=%d/%d message=%r",
                attempt,
                _PACKET_ATTEMPTS,
                message,
            )
            time.sleep(_PACKET_RETRY_SECONDS * attempt)

        results = envelope.get("results")
        if results is None:
            results = []
        if isinstance(results, dict):
            results = list(results.values())
        if not isinstance(results, list) or any(not isinstance(r, dict) for r in results):
            raise OneKeyAPIResponseError("OneKey population returned invalid results")
        return results, envelope

    def _next_packet(self, payload, expected):
        """Request the next packet, resuming as Developer Guide 6.13.4 says if its answer is lost.

        A continuation that gets no answer may or may not have been
        delivered, and sending it again blindly can be refused (HTTP 400) or
        skip a packet. So it is not resent: the walk asks for packet
        ``expected`` by number, which OneKey returns if it was delivered,
        and if it was not (HTTP 400, OKWS-3208) asks for the next packet
        again. Any other error stops the walk.
        """
        try:
            return self._population_packet(payload, resend_lost=False)
        except OneKeyResponseLostError as exc:
            lost = exc
        by_number = {**payload, "transactionPacketIdentifier": expected}
        for attempt in range(1, _RESUME_ATTEMPTS + 1):
            delay = 30 * attempt
            logger.warning(
                "OneKey packet %d got no answer, resuming by packet number: "
                "attempt=%d/%d delay_s=%d error=%s",
                expected,
                attempt,
                _RESUME_ATTEMPTS,
                delay,
                lost,
            )
            time.sleep(delay)
            try:
                results, envelope = self._population_packet(by_number)
            except OneKeyHTTPError as exc:
                if exc.status_code != 400:
                    raise
                logger.warning("OneKey packet %d was not delivered: %s", expected, exc)
            else:
                returned = self._packet_number(envelope)
                if returned is not None and returned != expected:
                    raise OneKeyAPIResponseError(
                        f"OneKey returned packet {returned} when packet {expected} was requested"
                    )
                return results, envelope
            try:
                return self._population_packet(payload, resend_lost=False)
            except OneKeyResponseLostError as exc:
                lost = exc
        raise lost

    @staticmethod
    def _packet_number(envelope):
        """The envelope's transactionPacketIdentifier as an int, or None."""
        value = envelope.get("transactionPacketIdentifier")
        if value is None:
            return None
        try:
            return int(value)
        except (TypeError, ValueError):
            raise OneKeyAPIResponseError(
                f"OneKey population returned packet identifier {value!r}"
            ) from None

    def _recover_packets(self, expected, received, transaction_id, request):
        """Fetch the packets OneKey sent but this walk never received.

        A response lost in transit is retried, and OneKey answers the retry
        with the packet after it. Developer Guide 6.13.4 ("Retrieve a specific
        packet") lets a client ask again for a delivered packet by number.
        Returns each recovered packet's results, in order.
        """
        if received < expected or not transaction_id or received - expected > _MAX_MISSED_PACKETS:
            raise OneKeyAPIResponseError(
                f"OneKey population packet sequence broke: expected {expected}, "
                f"received {received}"
            )
        recovered = []
        for number in range(expected, received):
            logger.warning(
                "OneKey packet %d was lost in transit; requesting it again by number",
                number,
            )
            results, envelope = self._population_packet(
                {
                    **request,
                    "transactionUniqueIdentifier": transaction_id,
                    "transactionPacketIdentifier": number,
                }
            )
            returned = self._packet_number(envelope)
            if returned is not None and returned != number:
                raise OneKeyAPIResponseError(
                    f"OneKey returned packet {returned} when packet {number} was requested"
                )
            recovered.append(results)
        return recovered

    def close(self):
        self.session.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()
