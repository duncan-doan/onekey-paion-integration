"""OAuth2 client-credentials authentication for Dataverse.

Unlike OneKey (see :mod:`onekey.auth`), Dataverse really does issue a token.
This is the standard Microsoft Entra ID client-credentials flow: POST the
application's id and secret to the tenant token endpoint, receive a bearer
token, send it as ``Authorization: Bearer <token>`` on every Web API call.

Tokens last an hour. :class:`TokenProvider` caches one and renews it a little
before expiry, so a long-running sync does not fail halfway through.
"""

import logging
import random
import socket
import threading
import time

import requests
import urllib3.util.connection as urllib3_connection

from .exceptions import DataverseAuthError, DataverseConfigError

logger = logging.getLogger(__name__)

# Entra and Dataverse often resolve to IPv6 on this network, and those
# handshakes never finish, so the request timeout never fires. IPv4 does.
urllib3_connection.allowed_gai_family = lambda: socket.AF_INET

AUTHORITY = "https://login.microsoftonline.com"

# Renew this many seconds before the token actually expires, so a request that
# is in flight when the clock runs out does not get a 401.
_RENEW_MARGIN_SECONDS = 300

# A token request that gets no answer, throttling or a server error is tried
# again: renewing the token mid-sync must not end the run on one blip.
# Refused credentials (4xx other than 429) fail at once.
_TOKEN_ATTEMPTS = 4
_TOKEN_RETRY_STATUSES = frozenset({429, 500, 502, 503, 504})


def get_access_token(
    tenant_id,
    client_id,
    client_secret,
    scope,
    authority=AUTHORITY,
    timeout=30.0,
):
    """Fetch an access token using the client-credentials grant.

    Args:
        tenant_id: Entra ID tenant (directory) GUID.
        client_id: Application (client) GUID.
        client_secret: Client secret value.
        scope: Resource scope, e.g.
            ``https://paion-crm-test.api.crm4.dynamics.com/.default``. The
            ``/.default`` suffix is required for client credentials -- it asks
            for the application permissions already consented to the app
            registration, and Entra rejects the request without it.
        authority: Login host. Only change for sovereign clouds.
        timeout: Request timeout in seconds.

    Returns:
        dict: ``{"access_token": ..., "expires_in": ..., "token_type": ...}``.

    Raises:
        DataverseAuthError: the token request was refused.
        DataverseConfigError: a required argument was blank.
    """
    for name, value in (
        ("tenant_id", tenant_id),
        ("client_id", client_id),
        ("client_secret", client_secret),
        ("scope", scope),
    ):
        if not str(value or "").strip():
            raise DataverseConfigError(f"{name} is required to request a token")

    # A stray slash here yields a confusing "tenant not found"; normalise it.
    url = f"{authority.rstrip('/')}/{str(tenant_id).strip('/')}/oauth2/v2.0/token"

    for attempt in range(1, _TOKEN_ATTEMPTS + 1):
        try:
            response = requests.post(
                url,
                data={
                    "grant_type": "client_credentials",
                    "client_id": client_id,
                    "client_secret": client_secret,
                    "scope": scope,
                },
                headers={"Content-Type": "application/x-www-form-urlencoded"},
                timeout=timeout,
            )
        except requests.RequestException as exc:
            if attempt == _TOKEN_ATTEMPTS:
                raise DataverseAuthError(f"Could not reach the token endpoint {url}: {exc}") from exc
            reason = type(exc).__name__
        else:
            if response.status_code not in _TOKEN_RETRY_STATUSES or attempt == _TOKEN_ATTEMPTS:
                break
            reason = response.status_code
        retry_after = _retry_after_seconds(response) if reason == 429 or reason == 503 else None
        delay = retry_after if retry_after is not None else min(2 ** attempt, 30) + random.uniform(0, 1)
        logger.warning(
            "Token request retrying: attempt=%d/%d reason=%s delay_s=%.1f",
            attempt,
            _TOKEN_ATTEMPTS,
            reason,
            delay,
        )
        time.sleep(delay)

    try:
        payload = response.json()
    except ValueError:
        raise DataverseAuthError(
            f"Token endpoint returned a non-JSON body (HTTP {response.status_code}): "
            f"{response.text[:300]}"
        )

    if response.status_code != 200 or "access_token" not in payload:
        raise DataverseAuthError(
            payload.get("error_description")
            or f"Token request failed with HTTP {response.status_code}",
            error=payload.get("error"),
            error_codes=payload.get("error_codes"),
            correlation_id=payload.get("correlation_id"),
        )
    return payload


def _retry_after_seconds(response) -> float | None:
    """A numeric Retry-After header, capped at a minute, or None."""
    try:
        return min(max(float(response.headers.get("Retry-After")), 0.0), 60.0)
    except (TypeError, ValueError):
        return None


class TokenProvider:
    """Caches an access token and renews it before it expires.

    Thread-safe: a sync that fans out across workers shares one token rather
    than each thread asking Entra for its own.
    """

    def __init__(
        self,
        tenant_id,
        client_id,
        client_secret,
        scope,
        authority=AUTHORITY,
        timeout=30.0,
    ):
        self.tenant_id = tenant_id
        self.client_id = client_id
        self.client_secret = client_secret
        self.scope = scope
        self.authority = authority
        self.timeout = timeout

        self._token = None
        self._expires_at = 0.0
        self._lock = threading.Lock()

    def token(self, force_refresh=False):
        """Return a valid access token, fetching or renewing if needed."""
        with self._lock:
            if (
                force_refresh
                or self._token is None
                or time.time() >= self._expires_at
            ):
                payload = get_access_token(
                    self.tenant_id,
                    self.client_id,
                    self.client_secret,
                    self.scope,
                    authority=self.authority,
                    timeout=self.timeout,
                )
                self._token = payload["access_token"]
                expires_in = int(payload.get("expires_in", 3600))
                # A short-lived token must never be cached past its real
                # expiry; keep half its lifetime when the usual margin is
                # larger than the token's lifetime.
                margin = min(_RENEW_MARGIN_SECONDS, expires_in / 2)
                self._expires_at = time.time() + max(0, expires_in - margin)
                logger.info(
                    "Acquired Dataverse token: scope=%s valid_s=%d",
                    self.scope,
                    expires_in,
                )
            return self._token

    def __repr__(self):
        return (
            f"TokenProvider(tenant_id={self.tenant_id!r}, "
            f"client_id={self.client_id!r}, scope={self.scope!r})"
        )
