"""Connection settings for a OneKey connection point."""

import os
from dataclasses import dataclass

from .exceptions import OneKeyConfigError

# The three syndicated OneKey environments, per the "START HERE" guide 6.1.1.
ENVIRONMENTS = {
    "prd": "https://okws.ok.imshealth.com",
    "uat": "https://okws-uat.onekey.iqvia.com",
    "qa": "https://okws-qa.ok.imshealth.com",
}


@dataclass(frozen=True)
class OneKeyConfig:
    """Everything needed to reach one OneKey connection point.

    Attributes:
        base_url: Environment root, e.g. ``https://okws-uat.onekey.iqvia.com``.
        username: OneKey web-services user.
        password: Password for that user.
        connector_id: Connection point id (``cid``). OneKey appends it to the
            endpoint path so it can resolve which subscription applies; a
            single user may have access to several connectors.
        timeout: Per-request timeout in seconds.
        max_retries: Attempts for transport errors and 429/5xx responses.
        verify_ssl: TLS verification. Leave enabled outside of debugging.
    """

    base_url: str
    username: str
    password: str
    connector_id: str
    timeout: float = 30.0
    max_retries: int = 8
    verify_ssl: bool = True
    # Seconds to wait between successful population packets (rate-limit hygiene).
    packet_delay: float = 1.0

    def __post_init__(self):
        for field in ("base_url", "username", "password", "connector_id"):
            if not str(getattr(self, field) or "").strip():
                raise OneKeyConfigError(f"{field} is required")
        object.__setattr__(self, "base_url", self.base_url.rstrip("/"))
        object.__setattr__(self, "connector_id", str(self.connector_id).strip())

    def url(self, path, with_connector=True):
        """Build an absolute endpoint URL.

        OneKey takes the connector id as the last path segment rather than as a
        query parameter, e.g. ``/ok/search/494`` (Developer Guide 5.2.3).
        """
        path = "/" + path.strip("/")
        if with_connector:
            path = f"{path}/{self.connector_id}"
        return self.base_url + path

    @classmethod
    def from_env(cls, prefix="ONEKEY_"):
        """Build a config from environment variables.

        Reads ``<prefix>BASE_URL``, ``<prefix>USERNAME``, ``<prefix>PASSWORD``
        and ``<prefix>CONNECTOR_ID``. ``<prefix>ENV`` (``prd``/``uat``/``qa``)
        is accepted instead of an explicit base URL.
        """
        base_url = os.environ.get(f"{prefix}BASE_URL")
        if not base_url:
            env = (os.environ.get(f"{prefix}ENV") or "").strip().lower()
            if env not in ENVIRONMENTS:
                raise OneKeyConfigError(
                    f"Set {prefix}BASE_URL, or {prefix}ENV to one of: "
                    + ", ".join(sorted(ENVIRONMENTS))
                )
            base_url = ENVIRONMENTS[env]

        return cls(
            base_url=base_url,
            username=os.environ.get(f"{prefix}USERNAME", ""),
            password=os.environ.get(f"{prefix}PASSWORD", ""),
            connector_id=os.environ.get(f"{prefix}CONNECTOR_ID", ""),
            timeout=float(os.environ.get(f"{prefix}TIMEOUT", "30")),
            max_retries=int(os.environ.get(f"{prefix}MAX_RETRIES", "8")),
            verify_ssl=os.environ.get(f"{prefix}VERIFY_SSL", "true").lower()
            not in ("0", "false", "no"),
            packet_delay=float(os.environ.get(f"{prefix}PACKET_DELAY", "1.0")),
        )

    def __repr__(self):
        # Keep the password out of logs and tracebacks.
        return (
            f"OneKeyConfig(base_url={self.base_url!r}, "
            f"username={self.username!r}, password='***', "
            f"connector_id={self.connector_id!r})"
        )
