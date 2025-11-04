"""Request body extractor module containing class to extract request body."""

import io
import typing
from typing import Iterable

from httpx._types import RequestContent, RequestFiles
from pycelonis_core.base.base_model import PyCelonisBaseModel
from pycelonis_core.utils.errors import PyCelonisTypeError


class RequestBodyExtractor:
    """Class to extract request body and process it."""

    def extract(
        self, request_body: typing.Any
    ) -> typing.Tuple[typing.Optional[RequestContent], typing.Optional[RequestFiles], typing.Optional[typing.Any]]:
        """Extracts request body and assigns it to either content, files, or json based on data type."""
        request_body = self._process_request_body(request_body)
        return self._assign_request_body(request_body)

    def _process_request_body(self, request_body: typing.Any) -> typing.Any:
        """Processes request body and returns json representation."""
        if isinstance(request_body, PyCelonisBaseModel):
            return request_body.json_dict(by_alias=True)
        if isinstance(request_body, list):
            return [self._process_request_body(item) for item in request_body]
        if isinstance(request_body, dict):
            return {key: self._process_request_body(value) for key, value in request_body.items()}
        if isinstance(request_body, tuple):
            return tuple(self._process_request_body(element) for element in request_body)
        if isinstance(request_body, (str, bool, int, float, io.BytesIO, io.BufferedReader)):
            return request_body
        if request_body is None:
            return request_body

        raise PyCelonisTypeError(f"Can't parse request body {request_body}. Try setting `parse_json` to False.")

    def _assign_request_body(
        self, request_body: typing.Any
    ) -> typing.Tuple[typing.Optional[RequestContent], typing.Optional[RequestFiles], typing.Optional[typing.Any]]:
        """Assigns request body to proper parameter."""
        content = files = json = None

        if self._is_multipart_request(request_body):
            files = request_body
        elif isinstance(request_body, str):
            content = request_body
        else:
            json = request_body
        return content, files, json

    def _is_multipart_request(self, request_body: typing.Any) -> bool:
        """Returns whether request body is multipart request."""
        if not request_body or not isinstance(request_body, dict):
            return False

        for value in request_body.values():
            if isinstance(value, (io.BytesIO, io.BufferedReader)):
                return True

            if isinstance(value, Iterable):
                for nested_value in value:
                    if isinstance(nested_value, io.BytesIO):
                        return True
        return False
