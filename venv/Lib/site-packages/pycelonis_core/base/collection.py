"""Module to define the CelonisCollection.

This module defines the CelonisCollection used in PyCelonis to facilitate searching objects by their attributes.

Typical usage example:

    ```python
    data_models = CelonisCollection(
        [
            data_model1,
            data_model2,
        ]
    )
    found_data_models = data_models.find(
        "TEST_DM",
        search_attribute="name",
    )
    data_model = data_models.find_single(
        "TEST_DM",
        search_attribute="name",
    )
    ```
"""

import textwrap
import typing

from pycelonis_core.utils.errors import PyCelonisNotFoundError, PyCelonisValueError

T = typing.TypeVar("T")


class CelonisCollection(typing.List[T]):
    """Class to make lists searchable."""

    @classmethod
    def from_list(cls, list_: typing.List) -> "CelonisCollection":
        """Convert list to Celonis Collection.

        Args:
            list_: List to convert.

        Returns:
            CelonisCollection from given list.
        """
        return CelonisCollection(list_)

    def find_all(self, search_term: typing.Any, search_attribute: str = "name") -> "CelonisCollection[T]":
        """Return all objects with matching `search_attribute` for given `search_term`.

        Args:
            search_term: Term to search for within objects of collection.
            search_attribute: Attribute of objects to search, default 'name'.

        Returns:
            CelonisCollection with matching objects.
        """
        return CelonisCollection(
            filter(
                lambda x: hasattr(x, search_attribute) and getattr(x, search_attribute) == search_term,
                self,
            )
        )

    def find(self, search_term: typing.Any, search_attribute: str = "name", default: typing.Optional[T] = None) -> T:
        """Return single object with matching `search_attribute` for given `search_term`.

        Args:
            search_term: Term to search for within objects of collection.
            search_attribute: Attribute of objects to search, default 'name'.
            default: Default to return if no matching object found. If no default PyCelonisNotFoundError is raised
                if no object found.

        Returns:
            Object matching `search_term` for `search_attribute`.

        Raises:
            PyCelonisNotFoundError: Raised if no object in collection matches `search_term` for `search_attribute` and
                no default given.
            PyCelonisValueError: Raised if multiple objects in collection match `search_term` for `search_attribute`.
        """
        found_objects = self.find_all(search_term, search_attribute)

        if len(found_objects) == 0:
            if default:
                return default
            raise PyCelonisNotFoundError(f"No object found for '{search_attribute}'='{search_term}'.")

        if len(found_objects) == 1:
            return found_objects[0]

        raise PyCelonisValueError(f"More than one object match for '{search_attribute}'='{search_term}'.")

    def find_by_id(self, search_id: str) -> T:
        """Return single object with matching ID for given `search_id`.

        Args:
            search_id: ID to search for within objects of collection.

        Returns:
            Object matching `search_id`.
        """
        return self.find(search_id, search_attribute="id")

    def __repr__(self) -> str:
        if len(self) == 0:
            return "[]"

        elements = ",\n".join(textwrap.indent(repr(element), "\t") for element in self)
        return f"[\n{elements}\n]"
