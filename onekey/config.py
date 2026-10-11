"""Connection settings for a OneKey connection point."""

import math
import os
from dataclasses import dataclass
from urllib.parse import urlsplit

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
    # Seconds to wait between successful population packets. OneKey allows
    # about 50 calls a minute per endpoint, shared by every worker, and a
    # packet takes ~1.6 s: with 2.5 s three parallel walks stay near 48 a
    # minute (1.0 s gave ~58 and throttle waits of 30-300 s each).
    packet_delay: float = 2.5
    # Requested /pop/refreshMyOnekey packet size (protocol range 100-2000).
    # Connector-specific backend caps can lower the actual packet size.
    page_size: int = 1000

    def __post_init__(self):
        for field in ("base_url", "username", "password", "connector_id"):
            if not str(getattr(self, field) or "").strip():
                raise OneKeyConfigError(f"{field} is required")
        base_url = str(self.base_url).strip().rstrip("/")
        try:
            parts = urlsplit(base_url)
            valid_url = (
                parts.scheme == "https"
                and bool(parts.hostname)
                and parts.username is None
                and parts.password is None
                and not parts.query
                and not parts.fragment
            )
        except ValueError:
            valid_url = False
        if not valid_url:
            raise OneKeyConfigError(
                "base_url must be an HTTPS URL without credentials, query, or fragment"
            )
        object.__setattr__(self, "base_url", base_url)
        object.__setattr__(self, "connector_id", str(self.connector_id).strip())
        if (
            isinstance(self.max_retries, bool)
            or not isinstance(self.max_retries, int)
            or self.max_retries < 1
        ):
            raise OneKeyConfigError("max_retries must be a positive integer (total attempts)")
        for field, allow_zero in (("timeout", False), ("packet_delay", True)):
            try:
                value = float(getattr(self, field))
            except (TypeError, ValueError):
                raise OneKeyConfigError(f"{field} must be a finite number") from None
            if not math.isfinite(value) or value < 0 or (value == 0 and not allow_zero):
                constraint = "nonnegative" if allow_zero else "positive"
                raise OneKeyConfigError(f"{field} must be finite and {constraint}")
            object.__setattr__(self, field, value)
        if not 100 <= int(self.page_size) <= 2000:
            raise OneKeyConfigError("page_size must be between 100 and 2000")

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
            packet_delay=float(os.environ.get(f"{prefix}PACKET_DELAY", "2.5")),
            page_size=int(os.environ.get(f"{prefix}PAGE_SIZE", "1000")),
        )

    def __repr__(self):
        # Keep the password out of logs and tracebacks.
        return (
            f"OneKeyConfig(base_url={self.base_url!r}, "
            f"username={self.username!r}, password='***', "
            f"connector_id={self.connector_id!r})"
        )
