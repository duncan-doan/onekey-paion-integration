"""Client for IQVIA OneKey web services (HCP and HCO reference data).

Quick start:
    >>> from onekey import OneKeyClient
    >>> client = OneKeyClient.connect(
    ...     base_url="https://okws-uat.onekey.iqvia.com",
    ...     username="494_PAION_API_UAT",
    ...     password="...",
    ...     connector_id="494",
    ... )
    >>> for hcp in client.search_hcp(cod_base="WDE", last_name="Muller"):
    ...     print(flatten_hcp(hcp))

Authentication is HTTP Basic, not a token exchange -- see :mod:`onekey.auth`.
"""

from .auth import OneKeySession, authenticate, basic_auth_header
from .client import (
    ACTIVITY,
    CONTAINS,
    EXACT,
    FUZZY,
    INDIVIDUAL,
    PHONETIC,
    STARTS_WITH,
    WORKPLACE,
    OneKeyClient,
    search_field,
)
from .config import ENVIRONMENTS, OneKeyConfig
from .exceptions import (
    OneKeyAPIResponseError,
    OneKeyAuthError,
    OneKeyConfigError,
    OneKeyError,
    OneKeyForbiddenError,
    OneKeyHTTPError,
    OneKeyNotFoundError,
    OneKeyRateLimitError,
)
from .parse import flatten_hco, flatten_hcp, primary_address, specialties

__version__ = "0.1.0"

__all__ = [
    "ACTIVITY",
    "CONTAINS",
    "ENVIRONMENTS",
    "EXACT",
    "FUZZY",
    "INDIVIDUAL",
    "OneKeyAPIResponseError",
    "OneKeyAuthError",
    "OneKeyClient",
    "OneKeyConfig",
    "OneKeyConfigError",
    "OneKeyError",
    "OneKeyForbiddenError",
    "OneKeyHTTPError",
    "OneKeyNotFoundError",
    "OneKeyRateLimitError",
    "OneKeySession",
    "PHONETIC",
    "STARTS_WITH",
    "WORKPLACE",
    "authenticate",
    "basic_auth_header",
    "flatten_hco",
    "flatten_hcp",
    "primary_address",
    "search_field",
    "specialties",
]
