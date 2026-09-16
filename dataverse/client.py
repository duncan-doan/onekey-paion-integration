"""Dataverse Web API client: create, retrieve, update and upsert records."""

import logging
import random
import re
import time

import requests

from .auth import TokenProvider
from .config import DataverseConfig
from .exceptions import DataverseHTTPError

logger = logging.getLogger(__name__)

_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})

# Microsoft recommends 100–1000 targets per CreateMultiple/UpdateMultiple for
# standard tables; there is no fixed record maximum (message size / timeout
# bind). Keep lookups smaller to stay under OData URL length limits.
DEFAULT_BULK_BATCH_SIZE = 100
DEFAULT_LOOKUP_CHUNK_SIZE = 50

# entity_set -> (logical_name, primary_id_attribute)
_ENTITY_META = {
    "accounts": ("account", "accountid"),
    "contacts": ("contact", "contactid"),
}


def escape_odata(value):
    """Escape a string for an OData filter literal.

    Single quotes are doubled; everything else is passed through and URL
    encoded by requests. Names like ``O'Brien`` break the filter otherwise.
    """
    return str(value).replace("'", "''")


def entity_meta(entity_set):
    """Return ``(logical_name, primary_id_field)`` for a Web API entity set."""
    if entity_set in _ENTITY_META:
        return _ENTITY_META[entity_set]
    logical = entity_set[:-1] if entity_set.endswith("s") else entity_set
    return logical, f"{logical}id"


def chunked(items, size):
    """Yield successive slices of ``items`` with length ``size``."""
    size = max(1, int(size))
    for start in range(0, len(items), size):
        yield items[start:start + size]


class DataverseClient:
    """Talks to the Dataverse Web API on behalf of an application user.

    Example:
        >>> client = DataverseClient.connect(
        ...     environment_url="https://paion-crm-test.api.crm4.dynamics.com",
        ...     tenant_id="...", client_id="...", client_secret="...",
        ... )
        >>> client.whoami()["UserId"]
    """

    def __init__(self, config, token_provider=None, session=None):
        self.config = config
        self.tokens = token_provider or TokenProvider(
            config.tenant_id,
            config.client_id,
            config.client_secret,
            config.scope,
            timeout=config.timeout,
        )
        self._session = session or requests.Session()

    @classmethod
    def connect(cls, environment_url, tenant_id, client_id, client_secret, **kwargs):
        """Build a client and acquire a token immediately.

        Fetching the token here means a bad secret fails at start-up rather
        than on the first write.
        """
        client = cls(
            DataverseConfig(
                environment_url=environment_url,
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret,
                **kwargs
            )
        )
        client.tokens.token()
        return client

    # -- plumbing ---------------------------------------------------------

    def _headers(self, extra=None):
        headers = {
            "Authorization": f"Bearer {self.tokens.token()}",
            "Accept": "application/json",
            "OData-MaxVersion": "4.0",
            "OData-Version": "4.0",
            "Content-Type": "application/json; charset=utf-8",
        }
        if extra:
            headers.update(extra)
        return headers

    def _raise_for_status(self, response, url):
        if response.status_code < 400:
            return

        code = message = None
        try:
            error = response.json().get("error", {})
            code = error.get("code")
            message = error.get("message")
        except ValueError:
            message = response.text[:500]

        raise DataverseHTTPError(
            f"Dataverse returned HTTP {response.status_code} for {url}: {message}",
            status_code=response.status_code,
            code=code,
            body=response.text[:1000],
            url=url,
        )

    def request(self, method, path, **kwargs):
        """Issue one authenticated Web API request, retrying transient failures."""
        url = path if path.startswith("http") else f"{self.config.api_url}/{path.lstrip('/')}"
        extra_headers = kwargs.pop("headers", None)
        kwargs.setdefault("timeout", self.config.timeout)

        last_error = None
        for attempt in range(1, self.config.max_retries + 1):
            # Rebuild headers each attempt so a renewed token is picked up.
            try:
                response = self._session.request(
                    method, url, headers=self._headers(extra_headers), **kwargs
                )
            except requests.RequestException as exc:
                last_error = DataverseHTTPError(
                    f"Could not reach Dataverse at {url}: {exc}", url=url
                )
            else:
                if response.status_code == 401 and attempt < self.config.max_retries:
                    # Token rejected mid-run; force a refresh and try again.
                    logger.info("Dataverse returned 401; refreshing the token.")
                    self.tokens.token(force_refresh=True)
                    continue
                if response.status_code not in _RETRY_STATUSES:
                    self._raise_for_status(response, url)
                    return response

                try:
                    self._raise_for_status(response, url)
                except DataverseHTTPError as exc:
                    last_error = exc

                # Dataverse sends Retry-After on throttling; respect it.
                retry_after = response.headers.get("Retry-After")
                if retry_after and attempt < self.config.max_retries:
                    try:
                        time.sleep(min(float(retry_after), 120))
                        continue
                    except ValueError:
                        pass

            if attempt < self.config.max_retries:
                delay = (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                logger.warning(
                    "Dataverse %s %s failed (attempt %d/%d); retrying in %.1fs",
                    method, url, attempt, self.config.max_retries, delay,
                )
                time.sleep(delay)

        raise last_error

    # -- reads ------------------------------------------------------------

    def whoami(self):
        """Return the calling user's ids. The cheapest way to prove the token works."""
        return self.request("GET", "WhoAmI").json()

    def retrieve_multiple(self, entity_set, select=None, filter=None, top=None,
                          order_by=None, expand=None, max_records=None):
        """Query an entity set, following ``@odata.nextLink`` across pages.

        Args:
            entity_set: Plural set name, e.g. ``accounts``.
            select: Columns to return.
            filter: OData ``$filter`` expression.
            top: ``$top`` for a single page.
            order_by: ``$orderby`` expression.
            expand: ``$expand`` expression.
            max_records: Stop after this many records overall.

        Returns:
            list[dict]
        """
        params = {}
        if select:
            params["$select"] = ",".join(select) if not isinstance(select, str) else select
        if filter:
            params["$filter"] = filter
        if top:
            params["$top"] = int(top)
        if order_by:
            params["$orderby"] = order_by
        if expand:
            params["$expand"] = expand

        records, url, first = [], entity_set, True
        while url:
            response = self.request("GET", url, params=params if first else None)
            body = response.json()
            records.extend(body.get("value", []))
            if max_records is not None and len(records) >= max_records:
                return records[:max_records]
            url, first = body.get("@odata.nextLink"), False
        return records

    def retrieve(self, entity_set, record_id, select=None):
        """Fetch one record by GUID."""
        params = {"$select": ",".join(select)} if select else None
        return self.request("GET", f"{entity_set}({record_id})", params=params).json()

    def find_one(self, entity_set, filter, select=None):
        """Return the first record matching a filter, or None."""
        rows = self.retrieve_multiple(entity_set, select=select, filter=filter, top=1)
        return rows[0] if rows else None

    # -- writes -----------------------------------------------------------

    def create(self, entity_set, data, return_record=False):
        """Create a record and return its GUID.

        Dataverse returns the new id in the ``OData-EntityId`` header rather
        than a body, unless a representation is requested.
        """
        headers = {}
        if return_record:
            headers["Prefer"] = "return=representation"

        response = self.request("POST", entity_set, json=data, headers=headers or None)
        if return_record and response.content:
            return response.json()

        entity_id = response.headers.get("OData-EntityId", "")
        match = re.search(r"\(([0-9a-fA-F-]{36})\)", entity_id)
        if not match:
            raise DataverseHTTPError(
                f"Created a {entity_set} record but could not parse its id from "
                f"OData-EntityId={entity_id!r}"
            )
        return match.group(1)

    def update(self, entity_set, record_id, data):
        """Patch an existing record by GUID."""
        self.request("PATCH", f"{entity_set}({record_id})", json=data)
        return record_id

    def delete(self, entity_set, record_id):
        """Delete a record by GUID."""
        self.request("DELETE", f"{entity_set}({record_id})")

    def upsert_by_field(self, entity_set, match_field, match_value, data):
        """Create or update, matching on a plain column.

        Dataverse can upsert natively by alternate key, but that needs a key to
        be defined on the table. Matching on an ordinary column keeps this
        working against a stock schema.

        Returns:
            tuple[str, bool]: the record id, and True when it was created.
        """
        existing = self.find_one(
            entity_set,
            filter=f"{match_field} eq '{escape_odata(match_value)}'",
            select=[match_field],
        )
        if existing:
            record_id = existing[next(k for k in existing if k.endswith("id") and
                                      not k.startswith("@"))]
            self.update(entity_set, record_id, data)
            return record_id, False
        return self.create(entity_set, data), True

    # -- bulk writes ------------------------------------------------------

    def find_many_by_field(self, entity_set, match_field, values, select=None,
                           chunk_size=DEFAULT_LOOKUP_CHUNK_SIZE):
        """Return ``{match_value: record}`` for values that already exist.

        Values are queried in chunks so OData filters stay within URL limits.
        """
        unique = []
        seen = set()
        for value in values:
            if value is None or value == "":
                continue
            key = str(value)
            if key not in seen:
                seen.add(key)
                unique.append(key)

        if not unique:
            return {}

        logical, primary_id = entity_meta(entity_set)
        columns = list(select or [])
        for col in (match_field, primary_id):
            if col not in columns:
                columns.append(col)

        found = {}
        for chunk in chunked(unique, chunk_size):
            filter_expr = " or ".join(
                f"{match_field} eq '{escape_odata(v)}'" for v in chunk
            )
            rows = self.retrieve_multiple(
                entity_set, select=columns, filter=filter_expr, top=len(chunk),
            )
            for row in rows:
                match_value = row.get(match_field)
                if match_value is not None:
                    found[str(match_value)] = row
        return found

    def create_multiple(self, entity_set, records, logical_name=None):
        """Create many rows via ``CreateMultiple`` (same table, one request).

        Args:
            entity_set: Plural set name, e.g. ``accounts``.
            records: Payloads without ``@odata.type`` (added automatically).
            logical_name: Entity logical name; inferred when omitted.

        Returns:
            list[str]: Created GUIDs in the same order as ``records``.
        """
        if not records:
            return []

        logical, _ = entity_meta(entity_set)
        if logical_name is not None:
            logical = logical_name
        odata_type = f"Microsoft.Dynamics.CRM.{logical}"
        targets = [{**row, "@odata.type": odata_type} for row in records]
        response = self.request(
            "POST",
            f"{entity_set}/Microsoft.Dynamics.CRM.CreateMultiple",
            json={"Targets": targets},
        )
        body = response.json() if response.content else {}
        ids = body.get("Ids") or []
        if len(ids) != len(records):
            raise DataverseHTTPError(
                f"CreateMultiple on {entity_set} returned {len(ids)} Ids for "
                f"{len(records)} targets",
                status_code=response.status_code,
                body=response.text[:1000],
                url=f"{entity_set}/Microsoft.Dynamics.CRM.CreateMultiple",
            )
        return [str(i) for i in ids]

    def update_multiple(self, entity_set, records, logical_name=None):
        """Update many rows via ``UpdateMultiple`` (same table, one request).

        Each record must include the primary id attribute (e.g. ``accountid``).
        """
        if not records:
            return

        logical, primary_id = entity_meta(entity_set)
        if logical_name is not None:
            logical = logical_name
        odata_type = f"Microsoft.Dynamics.CRM.{logical}"
        targets = []
        for row in records:
            if primary_id not in row:
                raise DataverseHTTPError(
                    f"UpdateMultiple target for {entity_set} is missing {primary_id}"
                )
            targets.append({**row, "@odata.type": odata_type})

        self.request(
            "POST",
            f"{entity_set}/Microsoft.Dynamics.CRM.UpdateMultiple",
            json={"Targets": targets},
        )

    def upsert_multiple(self, entity_set, records, alternate_key_field,
                        logical_name=None):
        """Native ``UpsertMultiple`` using an alternate key on each target.

        Requires the alternate key to exist on the table. Prefer
        ``bulk_upsert_by_field`` when matching on a plain column without a key.

        Each record must include ``alternate_key_field``. Returns ``None``
        (Dataverse responds ``204 NoContent`` without per-row ids).
        """
        if not records:
            return None

        logical, _ = entity_meta(entity_set)
        if logical_name is not None:
            logical = logical_name
        odata_type = f"Microsoft.Dynamics.CRM.{logical}"
        targets = []
        for row in records:
            key_value = row.get(alternate_key_field)
            if key_value is None or key_value == "":
                raise DataverseHTTPError(
                    f"UpsertMultiple target for {entity_set} is missing "
                    f"alternate key {alternate_key_field}"
                )
            target = {
                **row,
                "@odata.type": odata_type,
                "@odata.id": (
                    f"{entity_set}({alternate_key_field}="
                    f"'{escape_odata(key_value)}')"
                ),
            }
            targets.append(target)

        self.request(
            "POST",
            f"{entity_set}/Microsoft.Dynamics.CRM.UpsertMultiple",
            json={"Targets": targets},
        )
        return None

    def bulk_upsert_by_field(
        self,
        entity_set,
        match_field,
        records,
        batch_size=None,
        lookup_chunk_size=DEFAULT_LOOKUP_CHUNK_SIZE,
        fallback_on_error=True,
    ):
        """Bulk create/update by matching on a plain column.

        Looks up existing rows in chunks, then calls ``CreateMultiple`` /
        ``UpdateMultiple`` in batches. Falls back to per-record
        ``upsert_by_field`` when a bulk call fails (optional).

        Args:
            entity_set: Plural set name.
            match_field: Column used to decide create vs update.
            records: Payloads; each must include ``match_field``.
            batch_size: Targets per CreateMultiple/UpdateMultiple request.
                Defaults to ``config.bulk_batch_size`` (recommended 100–1000).
            lookup_chunk_size: Values per OData filter for existence checks.
            fallback_on_error: Retry failed batches record-by-record.

        Returns:
            list[tuple[str, str, bool]]: ``(match_value, record_id, created)``
            in the same order as ``records``.
        """
        if not records:
            return []

        batch_size = batch_size or getattr(
            self.config, "bulk_batch_size", DEFAULT_BULK_BATCH_SIZE
        ) or DEFAULT_BULK_BATCH_SIZE
        _, primary_id = entity_meta(entity_set)

        prepared = []
        for index, data in enumerate(records):
            match_value = data.get(match_field)
            if match_value is None or match_value == "":
                raise DataverseHTTPError(
                    f"bulk_upsert_by_field record[{index}] missing {match_field}"
                )
            prepared.append((str(match_value), dict(data)))

        existing = self.find_many_by_field(
            entity_set,
            match_field,
            [match for match, _ in prepared],
            select=[match_field, primary_id],
            chunk_size=lookup_chunk_size,
        )

        # Preserve input order in the final result list.
        results = [None] * len(prepared)
        creates = []  # (index, match_value, payload)
        updates = []  # (index, match_value, payload_with_id)

        for index, (match_value, payload) in enumerate(prepared):
            row = existing.get(match_value)
            if row and row.get(primary_id):
                update_payload = {**payload, primary_id: row[primary_id]}
                updates.append((index, match_value, update_payload))
            else:
                creates.append((index, match_value, payload))

        def _create_batch(batch):
            payloads = [payload for _, _, payload in batch]
            try:
                ids = self.create_multiple(entity_set, payloads)
                for (index, match_value, _), record_id in zip(batch, ids):
                    results[index] = (match_value, record_id, True)
            except DataverseHTTPError:
                if not fallback_on_error:
                    raise
                logger.warning(
                    "CreateMultiple failed for %d %s; falling back to single creates",
                    len(batch), entity_set,
                )
                for index, match_value, payload in batch:
                    record_id, created = self.upsert_by_field(
                        entity_set, match_field, match_value, payload
                    )
                    results[index] = (match_value, record_id, created)

        def _update_batch(batch):
            payloads = [payload for _, _, payload in batch]
            try:
                self.update_multiple(entity_set, payloads)
                for index, match_value, payload in batch:
                    results[index] = (match_value, payload[primary_id], False)
            except DataverseHTTPError:
                if not fallback_on_error:
                    raise
                logger.warning(
                    "UpdateMultiple failed for %d %s; falling back to single updates",
                    len(batch), entity_set,
                )
                for index, match_value, payload in batch:
                    record_id = payload[primary_id]
                    update_data = {
                        k: v for k, v in payload.items() if k != primary_id
                    }
                    self.update(entity_set, record_id, update_data)
                    results[index] = (match_value, record_id, False)

        for batch in chunked(creates, batch_size):
            _create_batch(batch)
        for batch in chunked(updates, batch_size):
            _update_batch(batch)

        if any(item is None for item in results):
            raise DataverseHTTPError(
                f"bulk_upsert_by_field on {entity_set} produced incomplete results"
            )
        return results

    def close(self):
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()
