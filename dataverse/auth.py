"""OAuth2 client-credentials authentication for Dataverse.

Unlike OneKey (see :mod:`onekey.auth`), Dataverse really does issue a token.
This is the standard Microsoft Entra ID client-credentials flow: POST the
application's id and secret to the tenant token endpoint, receive a bearer
token, send it as ``Authorization: Bearer <token>`` on every Web API call.

Tokens last an hour. :class:`TokenProvider` caches one and renews it a little
before expiry, so a long-running sync does not fail halfway through.
"""

import logging
import threading
import time

import requests

from .exceptions import DataverseAuthError, DataverseConfigError

logger = logging.getLogger(__name__)

AUTHORITY = "https://login.microsoftonline.com"

# Renew this many seconds before the token actually expires, so a request that
# is in flight when the clock runs out does not get a 401.
_RENEW_MARGIN_SECONDS = 300


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
        raise DataverseAuthError(f"Could not reach the token endpoint {url}: {exc}")

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
                self._expires_at = time.time() + max(
                    60, expires_in - _RENEW_MARGIN_SECONDS
                )
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
