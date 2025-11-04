"""Response processor module containing class to process response of requests."""

import io
import typing

from httpx import HTTPStatusError, Response
from pycelonis_core.client import decode_request_content
from pycelonis_core.utils.errors import (
    PyCelonisHTTPStatusError,
    PyCelonisNotFoundError,
    PyCelonisPermissionError,
    PyCelonisValueError,
)

try:
    from pydantic.v1 import parse_obj_as  # type: ignore
except ImportError:
    from pydantic import parse_obj_as  # type: ignore

T = typing.TypeVar("T")


class ResponseProcessor:
    """Class to process httpx response."""

    def process(
        self,
        response: Response,
        type_: typing.Optional[typing.Type[T]] = None,
        parse_json: bool = True,
    ) -> typing.Any:
        """Verifies whether response is valid and parses response body if parse_json=True."""
        self._verify_response(response)

        if parse_json:
            return self._parse_response(response, type_)
        return response

    def _verify_response(self, response: Response) -> None:
        """Verifies response status code and raises exception in case of failure."""
        if response.status_code in [401, 403]:
            message = f"You don't have permission to perform '{response.request.method}' -> '{response.request.url}'."
            raise PyCelonisPermissionError(message)

        if response.status_code == 404:
            message = f"Object at '{response.request.url}' not found."
            raise PyCelonisNotFoundError(message)

        try:
            response.raise_for_status()
        except HTTPStatusError as error:
            self._process_error(error)

    def _parse_response(self, response: Response, type_: typing.Optional[typing.Type[T]] = None) -> typing.Any:
        """Parses response according to response type."""
        if response.content == b"":
            return None  # Response is empty

        if type_ == io.BytesIO:
            return io.BytesIO(response.content)

        if response.headers.get("content-type", "").strip().startswith("application/json"):
            if type_ is None:
                return response.json()
            return parse_obj_as(type_, response.json())

        raise PyCelonisValueError(f"Can't parse response content: {response.content!r}")

    def _process_error(self, error: HTTPStatusError) -> None:
        message = f"""
            Request : {error.request.method} -> {error.request.url}
            Headers : {error.request.headers}
            Data    : {decode_request_content(error.request)}
            Response: {str(error.args[0])}
            Data    : {error.response.text}
        """
        raise PyCelonisHTTPStatusError(message)
