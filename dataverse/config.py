"""Connection settings for a Dataverse environment."""

import math
import os
import re
from dataclasses import dataclass
from urllib.parse import urlsplit

from .exceptions import DataverseConfigError


@dataclass(frozen=True)
class DataverseConfig:
    """Everything needed to reach one Dataverse environment.

    Attributes:
        environment_url: Org root, e.g.
            ``https://paion-crm-test.api.crm4.dynamics.com``.
        tenant_id: Entra ID tenant (directory) GUID.
        client_id: Application (client) GUID.
        client_secret: Client secret value.
        api_version: Web API version segment.
        timeout: Per-request timeout in seconds.
        max_retries: Attempts for transport errors and 429/5xx responses.
    """

    environment_url: str
    tenant_id: str
    client_id: str
    client_secret: str
    api_version: str = "v9.2"
    timeout: float = 60.0
    max_retries: int = 3
    # CreateMultiple / UpdateMultiple have no hard record cap; Microsoft
    # recommends starting at 100–1000 for standard tables (often ~100 for
    # account/contact with plugins). See bulk-operations docs.
    bulk_batch_size: int = 100

    def __post_init__(self):
        for field in ("environment_url", "tenant_id", "client_id", "client_secret"):
            if not str(getattr(self, field) or "").strip():
                raise DataverseConfigError(f"{field} is required")

        url = self.environment_url.strip().rstrip("/")
        if "://" not in url:
            url = "https://" + url
        # Accept a pasted Web API URL and reduce it to the org root.
        url = re.sub(r"/api/data/v[\d.]+/?$", "", url)
        try:
            parsed = urlsplit(url)
            valid_url = (
                parsed.scheme == "https" and bool(parsed.hostname)
                and parsed.username is None and parsed.password is None
                and not parsed.path and not parsed.query and not parsed.fragment
                and not any(char.isspace() for char in url)
            )
            parsed.port  # Validate a pasted port before issuing a request.
        except ValueError:
            valid_url = False
        if not valid_url:
            raise DataverseConfigError("environment_url must be an HTTPS organization root")
        if not isinstance(self.api_version, str) or not re.fullmatch(r"v\d+\.\d+", self.api_version):
            raise DataverseConfigError("api_version must be a version such as v9.2")
        if (
            isinstance(self.timeout, bool) or not isinstance(self.timeout, (int, float))
            or not math.isfinite(self.timeout) or self.timeout <= 0
        ):
            raise DataverseConfigError("timeout must be a positive finite number")
        for field in ("max_retries", "bulk_batch_size"):
            value = getattr(self, field)
            if isinstance(value, bool) or not isinstance(value, int) or value < 1:
                raise DataverseConfigError(f"{field} must be a positive integer")
        object.__setattr__(self, "environment_url", url)

    @property
    def scope(self):
        """The ``/.default`` scope for this environment."""
        return f"{self.environment_url}/.default"

    @property
    def api_url(self):
        """Base URL of the Web API."""
        return f"{self.environment_url}/api/data/{self.api_version}"

    @classmethod
    def from_env(cls, prefix="DATAVERSE_"):
        """Build a config from ``<prefix>URL``/``TENANT_ID``/``CLIENT_ID``/``CLIENT_SECRET``."""
        missing = [
            f"{prefix}{name}"
            for name in ("URL", "TENANT_ID", "CLIENT_ID", "CLIENT_SECRET")
            if not os.environ.get(f"{prefix}{name}")
        ]
        if missing:
            raise DataverseConfigError("Missing environment variables: " + ", ".join(missing))

        try:
            timeout = float(os.environ.get(f"{prefix}TIMEOUT", "60"))
            max_retries = int(os.environ.get(f"{prefix}MAX_RETRIES", "3"))
            bulk_batch_size = int(os.environ.get(f"{prefix}BULK_BATCH_SIZE", "100"))
        except ValueError as exc:
            raise DataverseConfigError("Dataverse numeric environment settings are invalid") from exc

        return cls(
            environment_url=os.environ[f"{prefix}URL"],
            tenant_id=os.environ[f"{prefix}TENANT_ID"],
            client_id=os.environ[f"{prefix}CLIENT_ID"],
            client_secret=os.environ[f"{prefix}CLIENT_SECRET"],
            api_version=os.environ.get(f"{prefix}API_VERSION", "v9.2"),
            timeout=timeout,
            max_retries=max_retries,
            bulk_batch_size=bulk_batch_size,
        )

    def __repr__(self):
        return (
            f"DataverseConfig(environment_url={self.environment_url!r}, "
            f"tenant_id={self.tenant_id!r}, client_id={self.client_id!r}, "
            f"client_secret='***')"
        )
