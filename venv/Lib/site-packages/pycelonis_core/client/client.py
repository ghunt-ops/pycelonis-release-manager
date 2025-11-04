"""Module to define base class for client.

This module defines base class for a client that can be used for any type of requests.
"""

import logging
import ssl
import typing

import httpx
from httpx import Cookies
from httpx._types import (
    CookieTypes,
    ProxyTypes,
    QueryParamTypes,
    RequestContent,
    RequestData,
    RequestFiles,
    TimeoutTypes,
)
from pycelonis_core.client import KeyType
from pycelonis_core.client.request_body_extractor import RequestBodyExtractor
from pycelonis_core.client.response_processor import ResponseProcessor
from pycelonis_core.utils.errors import PyCelonisValueError
from pycelonis_core.utils.httpx import RetryTransport

logger = logging.getLogger(__name__)

APITokenType = typing.Union[str, typing.Callable[[], str]]


class Client:
    """Blocking/synchronous client class."""

    def __init__(
        self,
        base_url: str,
        api_token: APITokenType,
        key_type: KeyType,
        user_agent: str,
        timeout: typing.Optional[TimeoutTypes] = None,
        retries: int = 0,
        delay: int = 1,
        request_body_extractor: typing.Optional[RequestBodyExtractor] = None,
        response_processor: typing.Optional[ResponseProcessor] = None,
        verify_ssl: typing.Union[str, bool, ssl.SSLContext] = False,
        proxy: typing.Optional[ProxyTypes] = None,
        mounts: typing.Optional[typing.Dict[str, httpx.BaseTransport]] = None,
    ) -> None:
        self._base_url = base_url

        if isinstance(api_token, str):
            self._api_token_callable: typing.Callable[[], str] = lambda base_url: api_token  # type: ignore
        elif callable(api_token):
            self._api_token_callable = api_token  # type: ignore
        else:
            raise PyCelonisValueError("Invalid 'api_token' type, should be 'str' or 'Callable[[], str]'")

        self._key_type = key_type
        self._user_agent = user_agent
        self._request_body_extractor = request_body_extractor or RequestBodyExtractor()
        self._response_processor = response_processor or ResponseProcessor()

        self._client = httpx.Client(
            base_url=self.base_url,
            headers=self.headers,
            transport=RetryTransport(
                verify=verify_ssl,
                limits=httpx.Limits(max_keepalive_connections=5, max_connections=10),
                retries=retries,
                delay=delay,
                status_forcelist=[429, 502, 503, 504],
                allowed_methods=["HEAD", "GET", "PUT", "POST", "DELETE", "OPTIONS"],
            ),
            timeout=timeout,
            proxy=proxy,
            mounts=mounts,
            verify=verify_ssl,
            trust_env=False,
        )

    @property
    def client(self) -> httpx.Client:
        """Returns httpx client.

        Returns:
            Httpx client.
        """
        return self._client

    @property
    def cookies(self) -> Cookies:
        """Returns cookies of httpx client.

        Returns:
            Cookies of httpx client
        """
        return self.client.cookies

    @property
    def base_url(self) -> str:
        """Returns base url of httpx client.

        Returns:
            Base url of httpx client
        """
        return self._base_url

    @property
    def headers(self) -> typing.Dict:
        """Returns headers of httpx client.

        Returns:
            Headers of httpx client
        """
        return {"user-agent": self._user_agent}

    def request(
        self,
        method: str,
        url: str,
        content: typing.Optional[RequestContent] = None,
        data: typing.Optional[RequestData] = None,
        files: typing.Optional[RequestFiles] = None,
        json: typing.Optional[typing.Any] = None,
        params: typing.Optional[QueryParamTypes] = None,
        headers: typing.Optional[typing.Dict[str, str]] = None,
        cookies: typing.Optional[CookieTypes] = None,
        timeout: typing.Optional[TimeoutTypes] = None,
        follow_redirects: bool = True,
        type_: typing.Optional[typing.Type] = None,
        parse_json: bool = False,
        request_body: typing.Any = None,
    ) -> typing.Any:
        """Sends request with given parameters."""
        if request_body is not None:
            if content is not None or files is not None or json is not None:
                raise PyCelonisValueError("Can't set request body and either one of [content, files, json]!")

            content, files, json = self._request_body_extractor.extract(request_body)

        request_headers = self._get_request_headers(headers)
        request = self.client.build_request(
            method=method,
            url=url,
            content=content,
            data=data,
            files=files,
            json=json,
            params=params,
            headers=request_headers,
            cookies=cookies,
            timeout=timeout,
        )

        response = self.client.send(request, follow_redirects=follow_redirects)
        return self._response_processor.process(response=response, type_=type_, parse_json=parse_json)

    def _get_request_headers(self, headers: typing.Optional[typing.Dict[str, str]] = None) -> typing.Dict[str, str]:
        """Returns request specific headers."""
        request_headers = self.headers

        token: typing.Optional[str] = None
        if self._api_token_callable is not None:
            token = self._api_token_callable(base_url=self._base_url)  # type: ignore
        if token is not None:
            authorization_prefix = self._key_type.get_authorization_prefix()
            request_headers["authorization"] = f"{authorization_prefix} {token}"

        if "XSRF-TOKEN" in self.client.cookies:
            request_headers["x-csrf-token"] = self.client.cookies["XSRF-TOKEN"]

        if headers is not None:
            request_headers.update(headers)

        return request_headers
