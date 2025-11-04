import enum
from datetime import datetime, timedelta


class AuthMethod(enum.Enum):
    """OAuth method to request client credentials, client secret basic or post."""

    BASIC = "BASIC"

    POST = "POST"


class AuthToken:
    """Class for handling OAuth 2 Token logic."""

    BUFFER_IN_SECONDS = 60

    def __init__(self) -> None:
        self.access_token = None
        self.expires_at = datetime.now()

    def update_token(self, access_token: str, expires_in: float) -> None:
        """Replaces the access token with the new one and sets the new expiration time."""
        self.access_token = access_token  # type: ignore
        self.expires_at = timedelta(expires_in) + datetime.now()

    def is_expired(self) -> bool:
        """Returns whether the token is expired."""
        return not self.access_token or self.expires_at + timedelta(seconds=self.BUFFER_IN_SECONDS) <= datetime.now()

    def __str__(self) -> str:
        return self.access_token  # type: ignore
