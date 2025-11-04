"""Module for knowledge model components."""

import typing

from pycelonis.service.semantic_layer.service import (
    AttributeMetadata,
    BusinessRecordMetadata,
    FilterMetadata,
    PqlBaseMetadata,
)
from pycelonis_core.base.collection import CelonisCollection
from saolapy.pql.base import PQLColumn, PQLFilter

try:
    from pydantic.v1 import validator  # type: ignore
except ImportError:
    from pydantic import validator  # type: ignore


class FinalAttribute(AttributeMetadata):
    """Class for knowledge model read only record attributes."""

    def get_column(self) -> PQLColumn:
        """Returns query of attribute.

        Returns:
            PQLColumn with attribute query.

        Examples:
            Extract data based on PQLs from knowledge model:
            ```python
            from pycelonis.pql import (
                PQL,
                PQLColumn,
            )

            record = knowledge_model.get_content().records.find_by_id(
                "ACTIVITIES"
            )
            attribute = record.attributes.find_by_id(
                "ACTIVITY_EN"
            )

            query = (
                PQL()
                + attribute.get_column()
            )

            (
                data_query,
                query_environment,
            ) = knowledge_model.resolve_query(
                query
            )
            df = data_model.export_data_frame(
                data_query,
                query_environment,
            )
            ```
        """
        return PQLColumn(name=self.id, query=self.pql)


class FinalIdentifier(PqlBaseMetadata):
    """Class for knowledge model read only record identifiers."""

    def get_column(self) -> PQLColumn:
        """Returns query of identifier.

        Returns:
            PQLColumn with identifier query.

        Examples:
            Extract data based on PQLs from knowledge model:
            ```python
            from pycelonis.pql import (
                PQL,
                PQLColumn,
            )

            record = knowledge_model.get_content().records.find_by_id(
                "ACTIVITIES"
            )
            identifier = record.identifier

            query = (
                PQL()
                + identifier.get_column()
            )

            (
                data_query,
                query_environment,
            ) = knowledge_model.resolve_query(
                query
            )
            df = data_model.export_data_frame(
                data_query,
                query_environment,
            )
            ```
        """
        return PQLColumn(name=self.id, query=self.pql)


class FinalRecord(BusinessRecordMetadata):
    """Class for knowledge model read only records."""

    attributes: typing.Optional[CelonisCollection[FinalAttribute]]  # type: ignore
    identifier: typing.Optional[FinalIdentifier]

    # validators
    _record_validators = validator(
        "attributes",
        allow_reuse=True,
    )(CelonisCollection.from_list)


class FinalFilter(FilterMetadata):
    """Class for knowledge model read only filters."""

    def get_filter(self) -> PQLFilter:
        """Returns query of filter.

        Returns:
            PQLColumn with filter query.

        Examples:
            Extract data based on PQLs from knowledge model:
            ```python
            from pycelonis.pql import (
                PQL,
                PQLColumn,
            )

            km_filter = knowledge_model.get_content().filters.find_by_id(
                "FILTER"
            )

            query = (
                PQL()
                + PQLColumn(
                    name="<name>",
                    query="<query>",
                )
                + km_filter.get_filter()
            )

            (
                data_query,
                query_environment,
            ) = knowledge_model.resolve_query(
                query
            )
            df = data_model.export_data_frame(
                data_query,
                query_environment,
            )
            ```
        """
        return PQLFilter(query=self.pql)
