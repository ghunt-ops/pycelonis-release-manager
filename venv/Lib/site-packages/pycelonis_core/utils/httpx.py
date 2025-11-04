"""httpx module containing classes related to httpx."""

import logging
import time
import typing

import httpx
from pycelonis_core.utils.errors import PyCelonisValueError

logger = logging.getLogger(__name__)


class RetryTransport(httpx.HTTPTransport):
    """httpx transport that enables retries in case of failed http requests."""

    def __init__(
        self,
        retries: int = 0,
        delay: int = 1,
        status_forcelist: typing.Optional[typing.List[int]] = None,
        allowed_methods: typing.Optional[typing.List[str]] = None,
        **kwargs: typing.Any,
    ) -> None:
        """Initializes RetryTransport.

        Args:
            retries: Number of total retries if request is failing.
            delay: Delay between retries in seconds.
            status_forcelist: Response status that should trigger a retry.
            allowed_methods: HTTP methods for which retries are enabled.
        """
        if retries > 10:
            raise PyCelonisValueError("Invalid 'total_retries' value [max 10].")
        if delay < 1:
            raise PyCelonisValueError("Invalid 'delay' value [min 1].")

        self.retries = retries
        self.delay = delay
        self.status_forcelist = status_forcelist or []
        self.allowed_methods = allowed_methods or []

        super().__init__(**kwargs)

    def handle_request(
        self,
        request: httpx.Request,
    ) -> httpx.Response:
        """Executes request and performs retries if necessary and enabled."""
        if self._is_retryable(request.method):
            return self._handle_request_with_retry(request)
        return super().handle_request(request)

    def _handle_request_with_retry(
        self,
        request: httpx.Request,
    ) -> httpx.Response:
        response = super().handle_request(request)

        for _ in range(self.retries):
            if not self._need_retry(response.status_code):
                break

            logger.warning("Request for %s failed with status code %s. Retrying...", request.url, response.status_code)
            if not response.is_closed:
                response.close()

            time.sleep(self.delay)
            response = super().handle_request(request)
        return response

    def _is_retryable(self, method: str) -> bool:
        """Returns whether for given method retries should be applied."""
        return method in self.allowed_methods and self.retries > 0

    def _need_retry(self, status_code: int) -> bool:
        """Returns whether retry is necessary for given response status code."""
        return status_code in self.status_forcelist
