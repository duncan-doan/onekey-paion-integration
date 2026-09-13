"""Exception hierarchy for the OneKey web services client."""


class OneKeyError(Exception):
    """Base class for every error raised by this package."""


class OneKeyConfigError(OneKeyError):
    """Raised when the client is configured with missing or invalid values."""


class OneKeyHTTPError(OneKeyError):
    """Raised when OneKey answers with a non-2xx HTTP status.

    The OneKey status-code table is documented in the Developer Guide,
    section 5.2.8 "Response HTTP status codes".
    """

    def __init__(self, message, status_code=None, body=None, url=None):
        super().__init__(message)
        self.status_code = status_code
        self.body = body
        self.url = url


class OneKeyAuthError(OneKeyHTTPError):
    """HTTP 401 - the credentials were rejected by OneKey."""


class OneKeyForbiddenError(OneKeyHTTPError):
    """HTTP 403 - authenticated, but not allowed to reach this resource.

    Two causes are common. Either the endpoint is not enabled for this
    connection point -- endpoints are subscribed per connector, so a valid user
    can reach /ok/search and still be refused /ok/count -- or the caller's IP is
    outside the OneKey allow-list configured against the WS user
    (Developer Guide 6.2).
    """


class OneKeyNotFoundError(OneKeyHTTPError):
    """HTTP 404 - unknown endpoint or connector."""


class OneKeyRateLimitError(OneKeyHTTPError):
    """HTTP 429 - too many requests in too short a time."""


class OneKeyAPIResponseError(OneKeyError):
    """Raised when OneKey returns HTTP 200 but a payload-level failure.

    Every OneKey response carries a ``status`` property that is one of
    ``SUCCESS``, ``WARNING`` or ``ERROR`` regardless of the endpoint
    (Developer Guide 6.1).
    """

    def __init__(self, message, status=None, payload=None):
        super().__init__(message)
        self.status = status
        self.payload = payload
