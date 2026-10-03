"""Exception hierarchy for the Dataverse client."""


class DataverseError(Exception):
    """Base class for every error raised by this package."""


class DataverseConfigError(DataverseError):
    """Raised when the client is configured with missing or invalid values."""


class DataverseAuthError(DataverseError):
    """Raised when the OAuth2 token request fails.

    Carries the Azure AD error code, which is what actually tells you what is
    wrong: ``AADSTS7000215`` is a bad client secret, ``AADSTS700016`` an
    unknown application, ``AADSTS900023`` a bad tenant id.
    """

    def __init__(self, message, error=None, error_codes=None, correlation_id=None):
        super().__init__(message)
        self.error = error
        self.error_codes = error_codes or []
        self.correlation_id = correlation_id


class DataverseHTTPError(DataverseError):
    """Raised when the Web API answers with a non-2xx status."""

    def __init__(self, message, status_code=None, code=None, body=None, url=None):
        super().__init__(message)
        self.status_code = status_code
        self.code = code
        self.body = body
        self.url = url


class DataverseAmbiguousWriteError(DataverseHTTPError):
    """A non-idempotent write may or may not have been applied.

    Raised instead of retrying a create when the request may have reached
    Dataverse but no answer came back (read timeout, dropped connection, gateway
    502/504). Repeating it could create the record twice: look it up first.
    """
