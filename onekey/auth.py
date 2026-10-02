"""Authentication for OneKey web services.

A note on tokens
----------------
OneKey does **not** issue an access token. There is no ``/oauth/token``
endpoint, no ``grant_type`` exchange and no bearer credential to cache or
refresh. Every OneKey endpoint is protected by HTTP Basic authentication over
TLS, and the ``Authorization: Basic <base64(user:password)>`` header is sent on
each individual request (Developer Guide 5.2.2 "Authentication details"). The
guide's only mention of OAuth2 is a forward-looking note that it may be
supported "in next release"; it has not shipped.

Authentication is therefore stateless: there is nothing to retrieve up front
and nothing that expires mid-run. What :func:`authenticate` gives you instead
is the closest useful equivalent -- it packages the credentials into a
connection-pooled :class:`OneKeySession` and verifies them once, up front,
against ``/system/connection/test`` so that bad credentials surface at start-up
rather than on the first data call.
"""

import base64
import logging
import random
import time
from dataclasses import asdict, dataclass

import requests

from .config import OneKeyConfig
from .exceptions import (
    OneKeyAPIResponseError,
    OneKeyAuthError,
    OneKeyForbiddenError,
    OneKeyHTTPError,
    OneKeyNotFoundError,
    OneKeyRateLimitError,
)

logger = logging.getLogger(__name__)

_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})


@dataclass
class RequestStats:
    """Running totals of the requests a session has sent.

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


def basic_auth_header(username, password):
    """Return the ``Authorization`` header value for HTTP Basic auth."""
    raw = f"{username}:{password}".encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


class OneKeySession:
    """An authenticated, connection-pooled handle on a OneKey connector.

    Holds the credentials and a pooled :class:`requests.Session`, applies the
    Basic auth header to every call, maps OneKey's documented status codes onto
    typed exceptions, and retries the transient ones with backoff.

    Prefer :func:`authenticate` over constructing this directly -- it validates
    the credentials before handing the session back.
    """

    def __init__(self, config, session=None):
        self.config = config
        self._session = session or requests.Session()
        self.stats = RequestStats()
        self._apply_default_headers()

    def _apply_default_headers(self) -> None:
        self._session.headers.update(
            {
                "Authorization": basic_auth_header(
                    self.config.username, self.config.password
                ),
                "Accept": "application/json",
                "User-Agent": "onekey-paion-integration/0.1",
            }
        )

    def _reset_connection_pool(self) -> None:
        """Drop pooled sockets after ConnectionReset / aborted TLS sessions."""
        try:
            self._session.close()
        except Exception:  # noqa: BLE001
            pass
        self._session = requests.Session()
        self._apply_default_headers()

    # -- plumbing ---------------------------------------------------------

    def _raise_for_status(self, response, url):
        """Map a non-2xx response onto the documented exception type."""
        status = response.status_code
        if status < 400:
            return

        body = response.text[:1000]
        cls = {
            401: OneKeyAuthError,
            403: OneKeyForbiddenError,
            404: OneKeyNotFoundError,
            429: OneKeyRateLimitError,
        }.get(status, OneKeyHTTPError)

        messages = {
            401: "OneKey rejected the credentials (401). Check the username "
            "and password for this environment.",
            403: "OneKey refused access (403). The user is authenticated but "
            "not permitted here: either this endpoint is not enabled for the "
            "connector, or the caller's IP is outside the OneKey allow-list.",
            404: f"OneKey has no resource at {url} (404). Check the endpoint "
            "path and the connector id.",
            429: "OneKey rate limit hit (429). Slow down and retry later.",
        }
        raise cls(
            messages.get(status, f"OneKey returned HTTP {status} for {url}"),
            status_code=status,
            body=body,
            url=url,
        )

    def request(self, method, path, with_connector=True, **kwargs):
        """Issue one authenticated request, with retries on transient failures.

        Each call is logged at DEBUG as ``onekey_call`` and added to ``stats``.
        """
        url = self.config.url(path, with_connector=with_connector)
        kwargs.setdefault("timeout", self.config.timeout)
        kwargs.setdefault("verify", self.config.verify_ssl)

        started = time.monotonic()
        attempts_before = self.stats.attempts
        status = None
        self.stats.calls += 1
        try:
            response = self._send(method, url, path, **kwargs)
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
                "OneKey %s %s: status=%s attempts=%d ms=%.0f",
                method,
                path,
                status,
                self.stats.attempts - attempts_before,
                elapsed * 1000,
            )

    def _send(self, method, url, path, **kwargs):
        """The retry loop behind :meth:`request`."""
        last_error = None
        for attempt in range(1, self.config.max_retries + 1):
            self.stats.attempts += 1
            try:
                response = self._session.request(method, url, **kwargs)
            except requests.RequestException as exc:
                last_error = OneKeyHTTPError(
                    f"Could not reach OneKey at {url}: {exc}", url=url
                )
                # Stale keep-alive sockets often surface as ConnectionResetError;
                # rebuild the pool before the next attempt.
                self._reset_connection_pool()
                if attempt < self.config.max_retries:
                    delay = min(30 * attempt, 180) + random.uniform(0, 2)
                    logger.warning(
                        "OneKey %s %s retrying after connection error: "
                        "attempt=%d/%d error=%s delay_s=%.1f",
                        method,
                        path,
                        attempt,
                        self.config.max_retries,
                        type(exc).__name__,
                        delay,
                    )
                    time.sleep(delay)
                continue
            else:
                if response.status_code not in _RETRY_STATUSES:
                    self._raise_for_status(response, url)
                    return response
                last_error = None
                try:
                    self._raise_for_status(response, url)
                except OneKeyHTTPError as exc:
                    last_error = exc

                # Honour Retry-After on 429; use a long floor so we do not
                # re-hit the limit immediately after a short exponential wait.
                if response.status_code == 429:
                    self.stats.throttled += 1
                if response.status_code == 429 and attempt < self.config.max_retries:
                    headers = getattr(response, "headers", None) or {}
                    retry_after = headers.get("Retry-After")
                    delay = None
                    if retry_after:
                        try:
                            delay = float(retry_after)
                        except ValueError:
                            delay = None
                    if delay is None:
                        delay = min(60 * attempt, 300) + random.uniform(0, 5)
                    else:
                        delay = min(max(delay, 30), 300)
                    logger.warning(
                        "OneKey %s %s throttled (429): attempt=%d/%d delay_s=%.0f",
                        method, path, attempt, self.config.max_retries, delay,
                    )
                    time.sleep(delay)
                    continue

            if attempt < self.config.max_retries:
                # Exponential backoff, jittered so parallel workers desynchronise.
                delay = (2 ** (attempt - 1)) + random.uniform(0, 0.5)
                logger.warning(
                    "OneKey %s %s retrying: attempt=%d/%d reason=%s delay_s=%.1f",
                    method,
                    path,
                    attempt,
                    self.config.max_retries,
                    getattr(last_error, "status_code", None) or "error",
                    delay,
                )
                time.sleep(delay)

        raise last_error

    def post_json(self, path, payload, with_connector=True):
        """POST a JSON body and return the decoded ``response`` envelope.

        OneKey wraps successful payloads in a top-level ``response`` object and
        reports a ``status`` of ``SUCCESS``, ``WARNING`` or ``ERROR`` inside it.
        """
        # The guide is explicit that the charset must be declared for UTF-8.
        headers = {"Content-Type": "application/json; charset=utf-8"}
        raw = self.request(
            "POST", path, with_connector=with_connector, json=payload, headers=headers
        )

        try:
            body = raw.json()
        except ValueError as exc:
            raise OneKeyAPIResponseError(
                f"OneKey returned a non-JSON body: {raw.text[:300]}"
            ) from exc

        envelope = body.get("response", body)
        status = str(envelope.get("status", "")).upper()
        if status == "ERROR":
            raise OneKeyAPIResponseError(
                "OneKey reported an ERROR status: "
                + str(envelope.get("statusMessage") or envelope),
                status=status,
                payload=envelope,
            )
        if status == "WARNING":
            # The envelope also carries the returned records; log only the status.
            results = envelope.get("results")
            logger.warning(
                "OneKey returned WARNING for %s: message=%r results=%s",
                path,
                str(envelope.get("statusMessage") or "")[:300],
                len(results) if isinstance(results, (list, dict)) else 0,
            )
        return envelope

    def test_connection(self):
        """Verify the credentials against ``/system/connection/test``.

        Returns True on success and raises :class:`OneKeyAuthError` (or another
        :class:`OneKeyHTTPError` subclass) otherwise. This endpoint is a GET and
        takes no connector id; it returns HTTP 200 with an empty body.
        """
        self.request("GET", "/system/connection/test", with_connector=False)
        return True

    def close(self):
        self._session.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def __repr__(self):
        return (
            f"OneKeySession(base_url={self.config.base_url!r}, "
            f"username={self.config.username!r}, "
            f"connector_id={self.config.connector_id!r})"
        )


def authenticate(
    base_url,
    username,
    password,
    connector_id,
    timeout=30.0,
    verify_ssl=True,
    validate=True,
):
    """Authenticate against OneKey and return a ready-to-use session.

    This is the entry point for "get me a connection to OneKey". Because OneKey
    uses stateless HTTP Basic auth rather than issuing a token (see the module
    docstring), the returned :class:`OneKeySession` *is* the credential: it
    carries the ``Authorization`` header and applies it to every later call.

    Args:
        base_url: Environment root, e.g. ``https://okws-uat.onekey.iqvia.com``.
        username: OneKey web-services user.
        password: Password for that user.
        connector_id: Connection point id (``cid``), e.g. ``494``.
        timeout: Per-request timeout in seconds.
        verify_ssl: TLS verification. Leave enabled outside of debugging.
        validate: Round-trip ``/system/connection/test`` before returning, so
            bad credentials fail here instead of on the first data call. Set
            False to build the session without any network I/O.

    Returns:
        OneKeySession: an authenticated session.

    Raises:
        OneKeyAuthError: the credentials were rejected (HTTP 401).
        OneKeyForbiddenError: authenticated but not permitted (HTTP 403).
        OneKeyConfigError: a required argument was blank.
    """
    config = OneKeyConfig(
        base_url=base_url,
        username=username,
        password=password,
        connector_id=connector_id,
        timeout=timeout,
        verify_ssl=verify_ssl,
    )
    session = OneKeySession(config)

    if validate:
        try:
            session.test_connection()
        except Exception:
            session.close()
            raise
        logger.info(
            "Authenticated with OneKey: base_url=%s user=%s connector=%s",
            config.base_url,
            config.username,
            config.connector_id,
        )
    return session
