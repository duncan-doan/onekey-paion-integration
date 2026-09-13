"""Dataverse Web API client: create, retrieve, update and upsert records."""

import logging
import random
import re
import time
import urllib.parse

import requests

from .auth import TokenProvider
from .config import DataverseConfig
from .exceptions import DataverseHTTPError

logger = logging.getLogger(__name__)

_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})


def escape_odata(value):
    """Escape a string for an OData filter literal.

    Single quotes are doubled; everything else is passed through and URL
    encoded by requests. Names like ``O'Brien`` break the filter otherwise.
    """
    return str(value).replace("'", "''")


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

    def close(self):
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()
