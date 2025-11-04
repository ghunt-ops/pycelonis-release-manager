"""Module for PyCelonis specific error classes.

This module contains all PyCelonis specific error classes used throughout the package.

Typical usage example:

    ```python
    raise PyCelonisError(
        "This is an error in PyCelonis"
    )
    ```
"""

import logging
import typing

logger = logging.getLogger(__name__)


class PyCelonisError(Exception):
    """Base Error class."""

    def __init__(self, message: typing.Optional[str] = None) -> None:
        """Instantiate PyCelonisError with given message.

        Args:
            message: Error message to display.
        """
        self.message = message
        super().__init__(message)

        logger.debug("%s: %s", self.__class__.__name__, message, stack_info=True)


class PyCelonisHTTPStatusError(PyCelonisError):
    """Raises for HTTPStatusError."""


class PyCelonisPermissionError(PyCelonisError):
    """Raises for PermissionError."""


class PyCelonisValueError(PyCelonisError):
    """Raised for ValueErrors."""


class PyCelonisAttributeError(PyCelonisError):
    """Raised for AttributeErrors."""


class PyCelonisTypeError(PyCelonisError):
    """Raised for TypeErrors."""


class PyCelonisNotFoundError(PyCelonisError):
    """Raised when object with given id was not found."""
