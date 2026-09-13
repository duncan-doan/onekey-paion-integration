"""Client for Microsoft Dataverse / Dynamics 365 Web API."""

from .auth import AUTHORITY, TokenProvider, get_access_token
from .client import DataverseClient, escape_odata
from .config import DataverseConfig
from .exceptions import (
    DataverseAuthError,
    DataverseConfigError,
    DataverseError,
    DataverseHTTPError,
)

__all__ = [
    "AUTHORITY",
    "DataverseAuthError",
    "DataverseClient",
    "DataverseConfig",
    "DataverseConfigError",
    "DataverseError",
    "DataverseHTTPError",
    "TokenProvider",
    "escape_odata",
    "get_access_token",
]
