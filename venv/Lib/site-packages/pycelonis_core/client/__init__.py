"""This module defines blocking and asynchronous clients."""

import enum

from httpx import Request, RequestNotRead


class KeyType(enum.Enum):
    """Key type used for authorization token."""

    APP_KEY = "APP_KEY"

    USER_KEY = "USER_KEY"
    """
    Deprecated in favor of 'BEARER'.
    """

    BEARER = "BEARER"

    def get_authorization_prefix(self) -> str:
        """Returns prefix for authorization header based on key type.

        Returns:
            Authorization prefix.
        """
        if self == KeyType.APP_KEY:
            return "AppKey"
        return "Bearer"


def decode_request_content(request: Request) -> str:
    """Returns decoded request content.

    Args:
        request: Request where content is read.

    Returns:
        Decoded request content
    """
    try:
        return request.content.decode("utf-8")
    except (RequestNotRead, UnicodeDecodeError):
        return "BINARY_CONTENT"
