"""Dataverse Web API client: create, retrieve, update and upsert records."""

import json
import logging
import math
import random
import re
import time
import uuid
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from decimal import Decimal
from email.utils import parsedate_to_datetime
from urllib.parse import urlsplit

import requests
from urllib3.exceptions import NewConnectionError

from .auth import TokenProvider
from .config import DataverseConfig
from .exceptions import DataverseAmbiguousWriteError, DataverseHTTPError

logger = logging.getLogger(__name__)

_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})

# The gateway gave up waiting for the backend, which may still have committed.
_GATEWAY_TIMEOUT_STATUSES = frozenset({502, 504})

# 4xx answers that are about the caller, not the record being written.
_SERVICE_4XX = frozenset({401, 403, 429})

# POST actions that are safe to repeat: they set the same values again.
_IDEMPOTENT_ACTIONS = ("UpdateMultiple", "UpsertMultiple")
MAX_BATCH_OPERATIONS = 1000

# Longest Retry-After honoured before the next attempt. Service protection
# asks for up to five minutes. Retry waits do not refresh a job's heartbeat
# (the heartbeat is a Dataverse write and would be throttled too), so two
# capped waits plus timeouts must stay inside the queue worker's 20-minute
# busy check. A longer request is cut to this; the attempt limit still applies.
MAX_RETRY_AFTER_SECONDS = 300


def _retry_after_seconds(value):
    """Parse a numeric or HTTP-date Retry-After into seconds, or None."""
    if value is None:
        return None
    try:
        delay = float(value)
    except (TypeError, ValueError):
        try:
            moment = parsedate_to_datetime(value)
            if moment.tzinfo is None:
                moment = moment.replace(tzinfo=timezone.utc)
            delay = max(0.0, (moment - datetime.now(timezone.utc)).total_seconds())
        except (TypeError, ValueError, OverflowError):
            return None
    return max(0.0, delay) if math.isfinite(delay) and delay >= 0 else None


def _can_fallback_to_single(exc):
    """False when writing record by record cannot help: auth or permissions.

    A bulk 5xx is often the batch itself (a SQL or plug-in timeout), so the
    records are sent singly. The fallback stops at the first single write that
    fails for a service reason, so an outage costs one more request, not one
    per record.
    """
    return exc.status_code not in (401, 403)


def _never_sent(exc: requests.RequestException) -> bool:
    """True when the request failed before reaching Dataverse (safe to repeat)."""
    if isinstance(exc, requests.exceptions.ConnectTimeout):
        return True
    if isinstance(exc, requests.exceptions.ConnectionError):
        reason = exc.args[0] if exc.args else None
        reason = getattr(reason, "reason", reason)  # urllib3 MaxRetryError wraps it
        return isinstance(reason, NewConnectionError)
    return False

# Web API base in absolute URLs (paging links); log lines keep the rest.
_API_BASE = re.compile(r"^https?://[^/]+/api/data/v[\d.]+/")


def _log_path(path: str) -> str:
    """Entity set and key of a request, without host, API version or query."""
    return _API_BASE.sub("", path.split("?", 1)[0])


_BATCH_BOUNDARY = re.compile(r'boundary="?([^";\s]+)"?', re.IGNORECASE)
_BATCH_STATUS = re.compile(r"^HTTP/1\.1 (\d{3})", re.MULTILINE)
_ENTITY_ID = re.compile(r"^OData-EntityId:\s*\S*\(([0-9a-fA-F-]{36})\)", re.MULTILINE | re.IGNORECASE)


def _parse_batch_response(response) -> list[tuple]:
    """Split a ``$batch`` response into ``(status, entity_id, error)`` per request."""
    match = _BATCH_BOUNDARY.search(response.headers.get("Content-Type", ""))
    if not match:
        raise DataverseHTTPError(
            "$batch response has no multipart boundary",
            status_code=response.status_code,
            body=response.text[:1000],
        )
    delimiter = f"--{match.group(1)}"
    results = []
    for part in response.text.split(delimiter)[1:]:
        if part.startswith("--"):
            break  # closing delimiter
        status_match = _BATCH_STATUS.search(part)
        if not status_match:
            continue
        status = int(status_match.group(1))
        id_match = _ENTITY_ID.search(part)
        error = None
        if status >= 400:
            body = re.split(r"\r?\n\r?\n", part[status_match.end():], maxsplit=1)
            text = body[1].strip() if len(body) > 1 else ""
            try:
                error = json.loads(text).get("error", {}).get("message") or text[:500]
            except ValueError:
                error = text[:500] or f"HTTP {status}"
        results.append((status, id_match.group(1) if id_match else None, error))
    return results


# Customer lookups: the Web API navigation name carries the target table
# (parentcustomerid_account), the column read back does not.
_POLYMORPHIC_NAVIGATION = {
    "parentcustomerid_account": "parentcustomerid",
    "parentcustomerid_contact": "parentcustomerid",
    "customerid_account": "customerid",
    "customerid_contact": "customerid",
    "parentid_account": "parentid",
    "parentid_contact": "parentid",
    # ts_affiliation.ts_fromid: an account (HCO-HCO) or a contact (HCP-HCO).
    # Without these, the skip-unchanged read asks for a column that does not
    # exist, Dataverse answers 400, and no affiliation is ever skipped.
    "ts_fromid_account": "ts_fromid",
    "ts_fromid_contact": "ts_fromid",
}
_BIND_BY_ID = re.compile(r"^/?\w+\(([0-9a-fA-F-]{36})\)$")
_ISO_DATETIME = re.compile(r"^\d{4}-\d{2}-\d{2}T")
_NUMBER = re.compile(r"^-?\d+(\.\d+)?$")


def _expected_columns(payload, key_fields):
    """Map a payload to ``{column read back: value it should hold}``.

    A lookup bind compares by the id it points at, read back as
    ``_<column>_value``. Returns None when a field cannot be compared (a bind
    by alternate key, another annotation), so the record is always written.
    """
    expected = {}
    for key, value in payload.items():
        if key in key_fields or key.startswith("@"):
            continue
        if key.endswith("@odata.bind"):
            navigation = key[: -len("@odata.bind")].lower()
            column = _POLYMORPHIC_NAVIGATION.get(navigation, navigation)
            bound = _BIND_BY_ID.match(str(value or ""))
            if not bound:
                return None
            expected[f"_{column}_value"] = bound.group(1).lower()
        elif "@" in key:
            return None
        else:
            expected[key] = value
    return expected


def _instant(text):
    try:
        moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return moment if moment.tzinfo else moment.replace(tzinfo=timezone.utc)


def _same_value(expected, actual) -> bool:
    """True only when the stored value certainly equals the one to write.

    Anything uncertain (other types, unparsed text) counts as different, so
    the record is written: a needless write is harmless, a missed one is not.
    """
    if expected is None or expected == "":
        return actual is None or actual == ""
    if actual is None or actual == "":
        return False
    if isinstance(expected, bool) or isinstance(actual, bool):
        return type(expected) is type(actual) and expected == actual
    if isinstance(expected, (int, float)) and isinstance(actual, (int, float)):
        return expected == actual
    # Choice and number columns accept "126870001" and return 126870001.
    if isinstance(expected, str) and isinstance(actual, (int, float)):
        return bool(_NUMBER.match(expected)) and Decimal(expected) == Decimal(str(actual))
    if isinstance(expected, str) and isinstance(actual, str):
        if expected == actual:
            return True
        # Dataverse returns date-times in UTC with a Z.
        if _ISO_DATETIME.match(expected) and _ISO_DATETIME.match(actual):
            stored, wanted = _instant(actual), _instant(expected)
            return stored is not None and stored == wanted
    return False


def _unchanged(expected, row) -> bool:
    """True when ``row`` already holds every value in ``expected``."""
    for column, value in expected.items():
        if column not in row:
            return False
        actual = row[column]
        if column.startswith("_") and column.endswith("_value"):
            if str(actual or "").lower() != value:
                return False
        elif not _same_value(value, actual):
            return False
    return True


def _call_hook(hook) -> None:
    """Run an after_request hook; a failing hook never fails the request."""
    if hook is None:
        return
    try:
        hook()
    except Exception:  # noqa: BLE001
        logger.debug("after_request hook failed", exc_info=True)


@dataclass
class RequestStats:
    """Running totals of the requests a client has sent.

    The sync engine reads them when a job starts and finishes, and logs the
    difference as one ``http_summary`` line per job.
    """

    calls: int = 0  # request() calls
    attempts: int = 0  # HTTP attempts, retries included
    throttled: int = 0  # 429 responses
    failed: int = 0  # calls that raised after the last attempt
    seconds: float = 0.0  # time inside request(), retry waits included

    def as_dict(self) -> dict:
        return asdict(self)

# Microsoft recommends 100–1000 targets per CreateMultiple/UpdateMultiple for
# standard tables; there is no fixed record maximum (message size / timeout
# bind).
DEFAULT_BULK_BATCH_SIZE = 100
# Values per lookup filter. On UAT a GET for 100 values takes about as long
# as one for 50 (0.28 s against 0.27 s), so one lookup per bulk batch halves
# the round trips of the existence check. _MAX_FILTER_CHARS keeps a filter of
# long keys (composite affiliation keys) well inside URL length limits.
DEFAULT_LOOKUP_CHUNK_SIZE = 100
_MAX_FILTER_CHARS = 6000

# entity_set -> (logical_name, primary_id_attribute)
_ENTITY_META = {
    "accounts": ("account", "accountid"),
    "contacts": ("contact", "contactid"),
    "customeraddresses": ("customeraddress", "customeraddressid"),
}

# Tables that reject bulk multi-ops in this org. bulk_upsert_by_field sends
# each batch of their writes as one $batch of single requests instead.
# customeraddress has neither CreateMultiple nor UpdateMultiple in dev, test
# or prod (2026-10-18); a CreateMultiple would fail and fall back to a lookup
# and a create per record. ts_affiliation creates use CreateMultiple: test
# and prod list it for the table, and it keeps an inactive record's
# statecode, which a single create drops. Adding ts_affiliations here returns
# its creates to the $batch path, where _apply_states sets the state.
_NO_UPDATE_MULTIPLE = frozenset({"customeraddresses", "ts_affiliations"})
_NO_CREATE_MULTIPLE = frozenset({"customeraddresses"})

# Dataverse ignores statecode on a single create (POST) and keeps the new
# record in its default state; CreateMultiple and updates apply it. A record
# created one at a time gets a non-default state with a PATCH right after.
_STATE_FIELDS = ("statecode", "statuscode")


def _split_state(data):
    """Return ``(create body, state)`` for a record created with a POST.

    ``state`` holds statecode (and statuscode, when given) only for a record
    that does not start in the default state 0; it is ``{}`` otherwise and the
    body is ``data`` unchanged.
    """
    statecode = data.get("statecode") if isinstance(data, dict) else None
    if statecode is None or str(statecode) == "0":
        return data, {}
    state = {name: data[name] for name in _STATE_FIELDS if data.get(name) is not None}
    body = {name: value for name, value in data.items() if name not in _STATE_FIELDS}
    return body, state


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


def _filter_chunks(field, values, size):
    """Yield ``field eq 'value' or ...`` filters of at most ``size`` values.

    A filter also stops growing at _MAX_FILTER_CHARS, so long values make
    smaller chunks rather than an over-long URL.
    """
    size = max(1, int(size))
    clauses, length = [], 0
    for value in values:
        clause = f"{field} eq '{escape_odata(value)}'"
        if clauses and (
            len(clauses) >= size or length + 4 + len(clause) > _MAX_FILTER_CHARS
        ):
            yield " or ".join(clauses)
            clauses, length = [], 0
        length += len(clause) + (4 if clauses else 0)
        clauses.append(clause)
    if clauses:
        yield " or ".join(clauses)


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
        self.stats = RequestStats()
        # Called with no arguments after every request, success or not. The
        # sync engine uses it to refresh a running job's heartbeat. Not called
        # during retry waits: under throttling the heartbeat write would be
        # throttled too, and its own waits would stretch the request's.
        self.after_request = None

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
        if 200 <= response.status_code < 300:
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

    def request(self, method, path, idempotent=None, attempts=None, **kwargs):
        """Issue one authenticated Web API request, retrying transient failures.

        Each call is logged at DEBUG and added to ``stats``. A POST is treated as
        a non-idempotent write (``idempotent=False``) unless it is an
        UpdateMultiple/UpsertMultiple action: once it may have reached Dataverse
        it is not repeated, and :class:`DataverseAmbiguousWriteError` is raised.
        ``attempts`` overrides ``config.max_retries`` for this request; 1 sends
        it once, for best-effort writes such as a job heartbeat.
        """
        method = method.upper()
        if idempotent is None:
            idempotent = method != "POST" or path.split("?", 1)[0].endswith(
                _IDEMPOTENT_ACTIONS
            )
        started = time.monotonic()
        attempts_before = self.stats.attempts
        status = None
        self.stats.calls += 1
        try:
            response = self._send(method, path, idempotent, attempts, **kwargs)
            status = response.status_code
            return response
        except Exception as exc:
            self.stats.failed += 1
            status = getattr(exc, "status_code", None) or type(exc).__name__
            raise
        finally:
            elapsed = time.monotonic() - started
            self.stats.seconds += elapsed
            logger.debug(
                "Dataverse %s %s: status=%s attempts=%d ms=%.0f",
                method,
                _log_path(path),
                status,
                self.stats.attempts - attempts_before,
                elapsed * 1000,
            )
            _call_hook(self.after_request)

    def _send(self, method, path, idempotent, attempts=None, **kwargs):
        """The retry loop behind :meth:`request`."""
        url = self._request_url(path)
        extra_headers = kwargs.pop("headers", None)
        kwargs.setdefault("timeout", self.config.timeout)
        # Web API requests should not redirect: in particular, a 307 could
        # forward a write body containing personal data to a different host.
        kwargs["allow_redirects"] = False
        max_attempts = attempts or self.config.max_retries

        last_error = None
        for attempt in range(1, max_attempts + 1):
            self.stats.attempts += 1
            # Rebuild headers each attempt so a renewed token is picked up.
            try:
                response = self._session.request(
                    method, url, headers=self._headers(extra_headers), **kwargs
                )
            except requests.RequestException as exc:
                if not idempotent and not _never_sent(exc):
                    raise DataverseAmbiguousWriteError(
                        f"{method} {_log_path(path)} may or may not have been applied: {exc}",
                        url=url,
                    ) from exc
                last_error = DataverseHTTPError(
                    f"Could not reach Dataverse at {url}: {exc}", url=url
                )
            else:
                if response.status_code == 401 and attempt < max_attempts:
                    # Token rejected mid-run; force a refresh and try again.
                    logger.info("Dataverse returned 401, refreshing token: path=%s", _log_path(path))
                    self.tokens.token(force_refresh=True)
                    continue
                if response.status_code not in _RETRY_STATUSES:
                    self._raise_for_status(response, url)
                    return response

                if response.status_code == 429:
                    self.stats.throttled += 1
                if not idempotent and response.status_code in _GATEWAY_TIMEOUT_STATUSES:
                    raise DataverseAmbiguousWriteError(
                        f"{method} {_log_path(path)} may or may not have been applied: "
                        f"gateway returned {response.status_code}",
                        status_code=response.status_code,
                        url=url,
                    )
                try:
                    self._raise_for_status(response, url)
                except DataverseHTTPError as exc:
                    last_error = exc

                # Dataverse sends Retry-After on throttling; respect it, up to
                # MAX_RETRY_AFTER_SECONDS.
                retry_after = _retry_after_seconds(response.headers.get("Retry-After"))
                if retry_after is not None and attempt < max_attempts:
                    if retry_after > MAX_RETRY_AFTER_SECONDS:
                        logger.warning(
                            "Dataverse %s %s asked to wait %.0fs; waiting %ds",
                            method, _log_path(path), retry_after, MAX_RETRY_AFTER_SECONDS,
                        )
                    time.sleep(min(retry_after, MAX_RETRY_AFTER_SECONDS))
                    continue

            if attempt < max_attempts:
                delay = (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                logger.warning(
                    "Dataverse %s %s retrying: attempt=%d/%d reason=%s delay_s=%.1f",
                    method,
                    _log_path(path),
                    attempt,
                    max_attempts,
                    getattr(last_error, "status_code", None) or "connection_error",
                    delay,
                )
                time.sleep(delay)

        raise last_error

    def _request_url(self, path):
        """Keep credentials on the configured origin, including paging links."""
        base = urlsplit(self.config.api_url)
        try:
            target = urlsplit(path)
            if target.scheme or target.netloc:
                same_origin = (
                    target.scheme == base.scheme and target.hostname == base.hostname
                    and (target.port if target.port is not None else 443)
                    == (base.port if base.port is not None else 443)
                    and target.username is None and target.password is None
                    and not target.fragment
                )
                if not same_origin:
                    raise ValueError("different origin")
                return path
        except ValueError as exc:
            raise DataverseHTTPError("Refusing a Dataverse URL outside the configured origin") from exc
        return f"{self.config.api_url}/{path.lstrip('/')}"

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
        params = {"$select": select if isinstance(select, str) else ",".join(select)} if select else None
        return self.request("GET", f"{entity_set}({record_id})", params=params).json()

    def find_one(self, entity_set, filter, select=None):
        """Return the first record matching a filter, or None."""
        rows = self.retrieve_multiple(entity_set, select=select, filter=filter, top=1)
        return rows[0] if rows else None

    # -- writes -----------------------------------------------------------

    def create(self, entity_set, data, return_record=False):
        """Create a record and return its GUID.

        Dataverse returns the new id in the ``OData-EntityId`` header rather
        than a body, unless a representation is requested. A record that
        starts in a non-default state gets it with a PATCH after the POST,
        which ignores statecode.
        """
        headers = {}
        if return_record:
            headers["Prefer"] = "return=representation"

        body, state = _split_state(data)
        response = self.request("POST", entity_set, json=body, headers=headers or None)
        record = response.json() if return_record and response.content else None
        if record is not None and not state:
            return record

        entity_id = response.headers.get("OData-EntityId", "")
        match = re.search(r"\(([0-9a-fA-F-]{36})\)", entity_id)
        if not match:
            raise DataverseHTTPError(
                f"Created a {entity_set} record but could not parse its id from "
                f"OData-EntityId={entity_id!r}"
            )
        record_id = match.group(1)
        if state:
            self.update(entity_set, record_id, state)
            if record is not None:
                record.update(state)
                return record
        return record_id

    def update(self, entity_set, record_id, data, attempts=None):
        """Patch an existing record by GUID; it is never created.

        ``If-Match: *`` makes a PATCH to a deleted record fail with 404
        instead of recreating it. ``attempts`` is passed to :meth:`request`.
        """
        self.request(
            "PATCH",
            f"{entity_set}({record_id})",
            json=data,
            headers={"If-Match": "*"},
            attempts=attempts,
        )
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
        _, primary_id = entity_meta(entity_set)
        match_filter = f"{match_field} eq '{escape_odata(match_value)}'"
        existing = self.find_one(entity_set, filter=match_filter, select=[primary_id])
        if existing:
            # Read the primary key by name: ts_externalid also ends in "id".
            record_id = existing[primary_id]
            self.update(entity_set, record_id, data)
            return record_id, False
        try:
            return self.create(entity_set, data), True
        except DataverseAmbiguousWriteError:
            # The create may have landed; look again before repeating it.
            landed = self.find_one(entity_set, filter=match_filter, select=[primary_id])
            if landed:
                logger.info(
                    "Create of %s %s had landed despite the error: id=%s",
                    entity_set, match_value, landed[primary_id],
                )
                # The POST that landed did not set the state.
                state = _split_state(data)[1]
                if state:
                    self.update(entity_set, landed[primary_id], state)
                return landed[primary_id], True
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
        columns = select.split(",") if isinstance(select, str) else list(select or [])
        for col in (match_field, primary_id):
            if col not in columns:
                columns.append(col)

        found = {}
        for filter_expr in _filter_chunks(match_field, unique, chunk_size):
            # No $top: a value already stored twice would take a second slot
            # and push another value's row off the page; that value would then
            # look missing and be created again.
            rows = self.retrieve_multiple(
                entity_set, select=columns, filter=filter_expr,
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

    def batch_write(self, operations, idempotent):
        """Send single writes in one ``$batch`` request, outside any change set.

        For tables that reject CreateMultiple/UpdateMultiple: Dataverse still
        runs the writes one by one, but over one HTTP round trip instead of
        one each. ``odata.continue-on-error`` keeps a failed write from
        stopping the rest. Each write still counts toward entitlement limits;
        batching also consumes the combined execution time service limit.

        Args:
            operations: ``(method, path, body)`` tuples, at most 1,000. A
                DELETE takes ``None`` as its body and sends none.
            idempotent: True when every write may safely run twice (PATCH or
                DELETE by id). A batch with creates must pass False, so an
                unknown outcome raises :class:`DataverseAmbiguousWriteError`.

        Returns:
            One ``(status, entity_id, error)`` per operation, in order.
            ``status`` is None when Dataverse returned no response for it.
        """
        if not operations:
            return []
        if len(operations) > MAX_BATCH_OPERATIONS:
            raise DataverseHTTPError(f"$batch supports at most {MAX_BATCH_OPERATIONS} operations")
        boundary = f"batch_{uuid.uuid4().hex}"
        lines = []
        for method, path, body in operations:
            method = method.upper()
            lines.extend(
                [
                    f"--{boundary}",
                    "Content-Type: application/http",
                    "Content-Transfer-Encoding: binary",
                    "",
                    f"{method} {self._request_url(path)} HTTP/1.1",
                ]
            )
            if body is None:
                lines.extend(["", ""])
                continue
            lines.extend(
                [
                    "Content-Type: application/json;type=entry",
                    *(["If-Match: *"] if method == "PATCH" else []),
                    "",
                    json.dumps(body, ensure_ascii=False, default=str),
                ]
            )
        lines.extend([f"--{boundary}--", ""])
        response = self.request(
            "POST",
            "$batch",
            data="\r\n".join(lines).encode("utf-8"),
            headers={
                "Content-Type": f"multipart/mixed;boundary={boundary}",
                "Prefer": "odata.continue-on-error",
            },
            idempotent=idempotent,
        )
        results = _parse_batch_response(response)
        if len(results) != len(operations):
            logger.warning(
                "$batch returned %d responses for %d requests; the rest count as failed",
                len(results),
                len(operations),
            )
        results = results[: len(operations)]
        results.extend(
            (None, None, "no response in $batch")
            for _ in range(len(operations) - len(results))
        )
        return results

    def bulk_upsert_by_field(
        self,
        entity_set,
        match_field,
        records,
        batch_size=None,
        lookup_chunk_size=DEFAULT_LOOKUP_CHUNK_SIZE,
        fallback_on_error=True,
        record_errors=None,
        skip_unchanged=False,
        skipped=None,
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
            record_errors: A dict to collect records Dataverse rejects on
                their own. A record whose single write gets a 4xx about the
                record (not 401/403/429) is stored here as
                ``{match_value: error}`` and reported with record_id None,
                and the other records are still written. When every record
                of a group of several sent one by one is rejected, the fault
                is not in the records, and the error is raised. Without it,
                the first rejected record raises.
            skip_unchanged: Read the payload's columns with the existence
                lookup and do not update a record that already holds every
                value. Lookups compare by the bound id; a bind by alternate
                key, or any value whose equality is uncertain, is written.
            skipped: A set to collect the match values left unchanged.

        Returns:
            list[tuple[str, str, bool]]: ``(match_value, record_id, created)``
            in the same order as ``records``. An unchanged record reports its
            id and ``created=False``, like an update.

        Records sharing a match value are written once, later fields overriding
        earlier ones; the repeats report ``created=False``. A CreateMultiple
        that may or may not have been applied is checked by match value before
        anything is created again.
        """
        if not records:
            return []

        batch_size = batch_size or getattr(
            self.config, "bulk_batch_size", DEFAULT_BULK_BATCH_SIZE
        ) or DEFAULT_BULK_BATCH_SIZE
        _, primary_id = entity_meta(entity_set)

        # One entry per match value: the same value twice would become two
        # creates, i.e. two records with the same key.
        prepared = []  # (match_value, payload)
        positions = {}  # match_value -> index in prepared
        owners = []  # index in prepared for each input record
        for index, data in enumerate(records):
            match_value = data.get(match_field)
            if match_value is None or match_value == "":
                raise DataverseHTTPError(
                    f"bulk_upsert_by_field record[{index}] missing {match_field}"
                )
            key = str(match_value)
            if key in positions:
                position = positions[key]
                prepared[position] = (key, {**prepared[position][1], **data})
            else:
                positions[key] = len(prepared)
                prepared.append((key, dict(data)))
            owners.append(positions[key])
        if len(prepared) < len(records):
            logger.warning(
                "Duplicate %s values merged before upsert: entity=%s duplicates=%d",
                match_field, entity_set, len(records) - len(prepared),
            )

        # With skip_unchanged, the existence lookup also reads the columns each
        # payload writes, and a record that already holds them is not sent.
        compare = {}  # match_value -> {column: expected value}
        columns = [match_field, primary_id]
        if skip_unchanged:
            for match_value, payload in prepared:
                expected = _expected_columns(payload, {match_field, primary_id})
                if expected:
                    compare[match_value] = expected
                    columns.extend(c for c in expected if c not in columns)
        keys = [match for match, _ in prepared]
        try:
            existing = self.find_many_by_field(
                entity_set, match_field, keys, select=columns,
                chunk_size=lookup_chunk_size,
            )
        except DataverseHTTPError as exc:
            # A payload field that is not a readable column (400) only turns
            # the comparison off; the records are still written.
            if not compare or exc.status_code != 400:
                raise
            logger.warning(
                "Cannot read %s columns to skip unchanged records, writing all: %s",
                entity_set, str(exc)[:300],
            )
            compare = {}
            existing = self.find_many_by_field(
                entity_set, match_field, keys, select=[match_field, primary_id],
                chunk_size=lookup_chunk_size,
            )

        # Preserve input order in the final result list.
        results = [None] * len(prepared)
        creates = []  # (index, match_value, payload)
        updates = []  # (index, match_value, payload_with_id)
        unchanged = 0

        for index, (match_value, payload) in enumerate(prepared):
            row = existing.get(match_value)
            if row and row.get(primary_id):
                expected = compare.get(match_value)
                if expected is not None and _unchanged(expected, row):
                    results[index] = (match_value, row[primary_id], False)
                    unchanged += 1
                    if skipped is not None:
                        skipped.add(match_value)
                    continue
                update_payload = {**payload, primary_id: row[primary_id]}
                updates.append((index, match_value, update_payload))
            else:
                creates.append((index, match_value, payload))
        if skip_unchanged:
            logger.info(
                "Unchanged %s skipped: unchanged=%d updates=%d creates=%d",
                entity_set, unchanged, len(updates), len(creates),
            )

        def _create_one(index, match_value, payload):
            record_id, created = self.upsert_by_field(
                entity_set, match_field, match_value, payload
            )
            results[index] = (match_value, record_id, created)

        def _rejects_record(exc):
            """True when record_errors is on and the error is about the record."""
            status = getattr(exc, "status_code", None)
            return (
                record_errors is not None
                and not isinstance(exc, DataverseAmbiguousWriteError)
                and status is not None
                and 400 <= status < 500
                and status not in _SERVICE_4XX
            )

        def _one_by_one(writes, write_one, batch_size):
            """Send ``writes`` singly, keeping rejected records in record_errors.

            ``batch_size`` is the batch they came from: when none of a batch of
            several could be written, the cause is a mapping or schema fault
            shared by every record, so the error is raised.
            """
            rejected = []
            for index, match_value, payload in writes:
                try:
                    write_one(index, match_value, payload)
                except DataverseHTTPError as exc:
                    if not _rejects_record(exc):
                        raise
                    rejected.append((index, match_value, exc))
            if batch_size > 1 and len(rejected) == batch_size:
                raise rejected[0][2]
            for index, match_value, exc in rejected:
                logger.warning(
                    "Dataverse rejected %s %s=%s: status=%s error=%r",
                    entity_set, match_field, match_value, exc.status_code, str(exc)[:300],
                )
                results[index] = (match_value, None, False)
                record_errors[match_value] = exc

        def _set_state_one(index, match_value, state):
            self.update(entity_set, results[index][1], state)

        def _apply_states(created):
            """PATCH the state of records just created by a $batch POST.

            ``created`` are ``(index, match_value, payload)`` whose result
            holds the new id. Only a record that starts in a non-default state
            is sent. A PATCH the $batch rejects is retried alone; one that
            still fails is the record's error, so a later run sets it.
            """
            if entity_set not in _NO_CREATE_MULTIPLE:
                return
            writes = [
                (index, match_value, _split_state(payload)[1])
                for index, match_value, payload in created
            ]
            writes = [write for write in writes if write[2]]
            if not writes:
                return
            outcomes = self.batch_write(
                [
                    ("PATCH", f"{entity_set}({results[index][1]})", state)
                    for index, _, state in writes
                ],
                idempotent=True,
            )
            retry = [
                write
                for write, (status, _, _) in zip(writes, outcomes)
                if not status or status >= 400
            ]
            if retry:
                logger.warning(
                    "$batch rejected %d of %d state updates for %s, retrying them one by one",
                    len(retry), len(writes), entity_set,
                )
            _one_by_one(retry, _set_state_one, len(writes))

        def _create_batch(batch):
            payloads = [payload for _, _, payload in batch]
            try:
                if entity_set in _NO_CREATE_MULTIPLE:
                    outcomes = self.batch_write(
                        [("POST", entity_set, _split_state(payload)[0]) for payload in payloads],
                        idempotent=False,
                    )
                    ids = [
                        record_id if status and status < 400 and record_id else None
                        for status, record_id, _ in outcomes
                    ]
                else:
                    ids = self.create_multiple(entity_set, payloads)
            except DataverseAmbiguousWriteError as exc:
                # The batch may have been created: keep what landed and create
                # only the rest.
                landed = self.find_many_by_field(
                    entity_set,
                    match_field,
                    [match_value for _, match_value, _ in batch],
                    select=[match_field, primary_id],
                    chunk_size=lookup_chunk_size,
                )
                missing = []
                arrived = []
                for index, match_value, payload in batch:
                    row = landed.get(match_value)
                    if row and row.get(primary_id):
                        results[index] = (match_value, row[primary_id], True)
                        arrived.append((index, match_value, payload))
                    else:
                        missing.append((index, match_value, payload))
                logger.warning(
                    "CreateMultiple outcome unknown for %s, checked by %s: "
                    "records=%d landed=%d missing=%d error=%r",
                    entity_set, match_field, len(batch), len(batch) - len(missing),
                    len(missing), str(exc)[:300],
                )
                _apply_states(arrived)
                if missing and not fallback_on_error:
                    raise
                _one_by_one(missing, _create_one, len(batch))
                return
            except DataverseHTTPError as exc:
                if not fallback_on_error or not _can_fallback_to_single(exc):
                    raise
                logger.warning(
                    "CreateMultiple failed for %s, creating one by one: "
                    "records=%d status=%s error=%r",
                    entity_set, len(batch), exc.status_code, str(exc)[:300],
                )
                _one_by_one(batch, _create_one, len(batch))
                return

            # A create that failed inside a $batch (rejected, throttled or a
            # server error) is retried alone through the normal retry loop. The
            # creates that succeeded keep their ids. A service error that
            # persists is raised by the first single write that hits it.
            rejected = []
            created = []
            for (index, match_value, payload), record_id in zip(batch, ids):
                if record_id:
                    results[index] = (match_value, record_id, True)
                    created.append((index, match_value, payload))
                else:
                    rejected.append((index, match_value, payload))
            _apply_states(created)
            if rejected:
                logger.warning(
                    "$batch rejected %d of %d creates for %s, retrying them one by one",
                    len(rejected), len(batch), entity_set,
                )
            # A create retried alone goes through create(), which sets its state.
            _one_by_one(rejected, _create_one, len(batch))

        def _update_one(index, match_value, payload):
            record_id = payload[primary_id]
            update_data = {
                k: v for k, v in payload.items() if k != primary_id
            }
            self.update(entity_set, record_id, update_data)
            results[index] = (match_value, record_id, False)

        def _update_batch(batch):
            payloads = [payload for _, _, payload in batch]
            rejected_at: set[int] = set()
            try:
                if entity_set in _NO_UPDATE_MULTIPLE:
                    # PATCH by id sets the same values again, so the $batch
                    # is safe to resend after an unknown outcome.
                    outcomes = self.batch_write(
                        [
                            (
                                "PATCH",
                                f"{entity_set}({payload[primary_id]})",
                                {k: v for k, v in payload.items() if k != primary_id},
                            )
                            for payload in payloads
                        ],
                        idempotent=True,
                    )
                    rejected_at = {
                        position
                        for position, (status, _, _) in enumerate(outcomes)
                        if not status or status >= 400
                    }
                else:
                    self.update_multiple(entity_set, payloads)
            except DataverseHTTPError as exc:
                if not fallback_on_error or not _can_fallback_to_single(exc):
                    raise
                logger.warning(
                    "UpdateMultiple failed for %s, updating one by one: "
                    "records=%d status=%s error=%r",
                    entity_set, len(batch), exc.status_code, str(exc)[:300],
                )
                _one_by_one(batch, _update_one, len(batch))
                return

            for position, (index, match_value, payload) in enumerate(batch):
                if position not in rejected_at:
                    results[index] = (match_value, payload[primary_id], False)
            if rejected_at:
                logger.warning(
                    "$batch rejected %d of %d updates for %s, retrying them one by one",
                    len(rejected_at), len(batch), entity_set,
                )
            # Retried alone, a rejected update either succeeds or gives the
            # real error (raised, or kept in record_errors).
            _one_by_one(
                [batch[position] for position in sorted(rejected_at)],
                _update_one,
                len(batch),
            )

        create_size = min(batch_size, MAX_BATCH_OPERATIONS) if entity_set in _NO_CREATE_MULTIPLE else batch_size
        update_size = min(batch_size, MAX_BATCH_OPERATIONS) if entity_set in _NO_UPDATE_MULTIPLE else batch_size
        for batch in chunked(creates, create_size):
            _create_batch(batch)
        for batch in chunked(updates, update_size):
            _update_batch(batch)

        if any(item is None for item in results):
            raise DataverseHTTPError(
                f"bulk_upsert_by_field on {entity_set} produced incomplete results"
            )
        # One result per input record; a repeated match value reports an update.
        expanded = []
        reported = set()
        for position in owners:
            match_value, record_id, created = results[position]
            expanded.append((match_value, record_id, created and position not in reported))
            reported.add(position)
        return expanded

    def close(self):
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()
