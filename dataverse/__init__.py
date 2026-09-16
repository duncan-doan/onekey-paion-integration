"""Client for Microsoft Dataverse / Dynamics 365 Web API."""

from .auth import AUTHORITY, TokenProvider, get_access_token
from .client import DataverseClient, escape_odata, entity_meta, chunked
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
    "chunked",
    "entity_meta",
    "escape_odata",
    "get_access_token",
]
