"""Module for analysis components."""

import typing
from abc import ABC, abstractmethod

from pycelonis.pql import PQL, OrderByColumn, PQLColumn, PQLFilter
from pycelonis_core.base.base_model import PyCelonisBaseModel
from pycelonis_core.base.collection import CelonisCollection

try:
    from pydantic.v1 import Extra  # type: ignore
except ImportError:
    from pydantic import Extra  # type: ignore


class AnalysisComponent(PyCelonisBaseModel, extra=Extra.allow):  # type: ignore
    """Class for analysis components."""

    id: typing.Optional[str]
    """Id of component."""
    type: typing.Optional[str]
    """Type of component (e.g. pql-table)."""
    title: typing.Optional[str]
    """Title of component shown in UI."""
    componentFilter: typing.Optional[str]
    """Raw component filter."""

    def get_filter(self) -> typing.Optional[PQLFilter]:
        """Returns filter of component.

        Returns:
            Component filter.

        Examples:
            Adding component filter to custom query and extract data:
            ```python
            sheet = analysis.get_content().draft.document.sheets[
                0
            ]
            olap_table = sheet.components.find(
                "#{OLAP Table}",
                search_attribute="title",
            )

            query = (
                PQL()
                + PQLColumn(
                    name="TEST",
                    query="<query",
                )
            )
            query += olap_table.get_filter()

            (
                data_query,
                query_environment,
            ) = analysis.resolve_query(
                query
            )
            df = data_model.export_data_frame(
                data_query,
                query_environment,
            )
            ```
        """
        return PQLFilter(query=self.componentFilter) if self.componentFilter else None

    def __main_attributes__(self) -> typing.List[str]:
        return ["id", "title", "type"]


class DataComponent(ABC, AnalysisComponent):
    """Abstract component class for components displaying data."""

    def get_query(self, use_translated_names: bool = True, exclude_hidden_columns: bool = False) -> PQL:
        """Returns full query with column and filter for component.

        Args:
            use_translated_names: If true, the translated names will be used as column names.
                If false, default names will be used.
            exclude_hidden_columns: If true, the columns that are marked hidden from UI will not be returned.

        Returns:
            A PQL query.

        Examples:
            Adding component query (columns + filters) to custom query and extract data:
            ```python
            sheet = analysis.get_content().draft.document.sheets[
                0
            ]
            olap_table = sheet.components.find(
                "#{OLAP Table}",
                search_attribute="title",
            )

            query = (
                PQL()
                + PQLColumn(
                    name="TEST",
                    query="<query",
                )
            )
            query += olap_table.get_query()

            (
                data_query,
                query_environment,
            ) = analysis.resolve_query(
                query
            )
            df = data_model.export_data_frame(
                data_query,
                query_environment,
            )
            ```
        """
        query = PQL()
        query += self.get_columns(use_translated_names, exclude_hidden_columns)

        if component_filter := self.get_filter():
            query += component_filter
        return query

    def get_columns(
        self, use_translated_names: bool = True, exclude_hidden_columns: bool = False
    ) -> typing.List[PQLColumn]:
        """Returns all columns of component including dimensions and kpis.

        Args:
            use_translated_names: If true, the translated names will be used as column names.
                If false, default names will be used.
            exclude_hidden_columns: If true, the columns that are marked hidden from UI will not be returned.

        Returns:
            List of PQLColumns.

        Examples:
            Adding component columns to custom query and extract data:
            ```python
            sheet = analysis.get_content().draft.document.sheets[
                0
            ]
            olap_table = sheet.components.find(
                "#{OLAP Table}",
                search_attribute="title",
            )

            query = (
                PQL()
                + PQLColumn(
                    name="TEST",
                    query="<query",
                )
            )
            query += olap_table.get_columns()

            (
                data_query,
                query_environment,
            ) = analysis.resolve_query(
                query
            )
            df = data_model.export_data_frame(
                data_query,
                query_environment,
            )
            ```
        """
        raw_columns = self._exclude_hidden_columns(self._raw_columns) if exclude_hidden_columns else self._raw_columns
        return [self._build_query_column(raw_column, use_translated_names) for raw_column in raw_columns]

    def _exclude_hidden_columns(self, columns: typing.List[typing.Dict]) -> typing.List[typing.Dict]:
        """Excludes hidden columns from raw columns."""
        return [col for col in columns if not col.get("notIncluded", False)]

    def _build_query_column(self, raw_column: typing.Dict, use_translated_names: bool) -> PQLColumn:
        """Returns PQLColumn using translated name if specified and exists."""
        if use_translated_names and "translatedName" in raw_column.keys() and raw_column["translatedName"]:
            return PQLColumn(name=raw_column["translatedName"], query=raw_column["text"])

        return PQLColumn(name=raw_column["name"], query=raw_column["text"])

    @property
    @abstractmethod
    def _raw_columns(self) -> typing.List[typing.Dict[str, typing.Any]]:
        """Returns list of dictionaries of columns containing `name` and `text`."""


class SingleKPI(DataComponent):
    """Class for gauges, numbers, fills, etc."""

    TYPE: typing.ClassVar[str] = "single-kpi"

    formula: typing.Dict

    @property
    def _raw_columns(self) -> typing.List[typing.Dict[str, typing.Any]]:
        """Returns list of dictionaries of columns containing `name` and `text`."""
        return [self.formula]


class PQLTable(DataComponent):
    """Class for OLAP tables, column charts, line charts, etc."""

    TYPE: typing.ClassVar[str] = "pql-table"

    distinct: typing.Optional[bool]
    axis0: typing.List[typing.Dict]
    axis1: typing.List[typing.Dict]
    axis2: typing.List[typing.Dict]

    def get_query(self, use_translated_names: bool = True, exclude_hidden_columns: bool = False) -> PQL:
        """Returns full query with all columns and filter for component.

        Args:
            use_translated_names: If true, the translated names will be used as column names.
                If false, default names will be used.
            exclude_hidden_columns: If true, the columns that are marked hidden from UI will not be returned.

        Returns:
            A PQL query.

        Examples:
            Adding table query to custom query and extract data:
            ```python
            sheet = analysis.get_content().draft.document.sheets[
                0
            ]
            olap_table = sheet.components.find(
                "#{OLAP Table}",
                search_attribute="title",
            )

            query = (
                PQL()
                + PQLColumn(
                    name="TEST",
                    query="<query",
                )
            )
            query += olap_table.get_query()

            (
                data_query,
                query_environment,
            ) = analysis.resolve_query(
                query
            )
            df = data_model.export_data_frame(
                data_query,
                query_environment,
            )
            ```
        """
        query = super().get_query(use_translated_names, exclude_hidden_columns)
        query.distinct = self.distinct or False
        query += self.get_order_by_columns()
        return query

    def get_order_by_columns(self) -> typing.List[OrderByColumn]:
        """Returns order by columns of component.

        Returns:
            Order by columns of component
        """
        order_by_columns = {}

        for raw_column in self._raw_columns:
            if "sortingIndex" in raw_column and raw_column["sortingIndex"] is not None:
                order_by_columns[raw_column["sortingIndex"]] = OrderByColumn(
                    query=raw_column["text"], ascending=raw_column.get("sorting", "ASC") == "ASC"
                )

        return [column for _, column in sorted(order_by_columns.items())]

    @property
    def _raw_columns(self) -> typing.List[typing.Dict[str, typing.Any]]:
        """Returns list of dictionaries of columns containing `name` and `text`."""
        return self.axis0 + self.axis1 + self.axis2


class PivotTable(DataComponent):
    """Class for Pivot Tables."""

    TYPE: typing.ClassVar[str] = "pivot"

    axis0: typing.List[typing.Dict]
    axis1: typing.List[typing.Dict]
    axis2: typing.List[typing.Dict]

    @property
    def _raw_columns(self) -> typing.List[typing.Dict[str, typing.Any]]:
        """Returns list of dictionaries of columns containing `name` and `text`."""
        return self.axis0 + self.axis1 + self.axis2


class Boxplot(DataComponent):
    """Class for Box Plots."""

    TYPE: typing.ClassVar[str] = "boxplot"

    dimension: typing.Dict
    distribution: typing.Dict

    @property
    def _raw_columns(self) -> typing.List[typing.Dict[str, typing.Any]]:
        """Returns list of dictionaries of columns containing `name` and `text`."""
        return [self.dimension, self.distribution]


class WorldMap(DataComponent):
    """Class for World Map Plots."""

    TYPE: typing.ClassVar[str] = "world-map"

    formula: typing.Dict
    kpiFormula: typing.Dict
    tooltipFormula: typing.Dict

    @property
    def _raw_columns(self) -> typing.List[typing.Dict[str, typing.Any]]:
        """Returns list of dictionaries of columns containing `name` and `text`."""
        return [self.formula, self.kpiFormula, {"name": "TooltipFormula", "text": self.tooltipFormula["text"]}]


class ComponentFactory:
    """Factory class to create AnalysisComponent with given type."""

    @staticmethod
    def get_component(component: AnalysisComponent) -> "AnalysisComponent":
        """Returns instance of AnalysisComponent depending on type.

        Args:
            component: AnalysisComponent containing properties of component.

        Returns:
            A AnalysisComponent object with proper class based on type.
        """
        if component.type == PQLTable.TYPE:
            return PQLTable(**component.dict())
        if component.type == SingleKPI.TYPE:
            return SingleKPI(**component.dict())
        if component.type == PivotTable.TYPE:
            return PivotTable(**component.dict())
        if component.type == Boxplot.TYPE:
            return Boxplot(**component.dict())
        if component.type == WorldMap.TYPE:
            return WorldMap(**component.dict())
        return AnalysisComponent(**component.dict())

    @staticmethod
    def get_components(components: typing.List[AnalysisComponent]) -> CelonisCollection[AnalysisComponent]:
        """Returns instances of AnalysisComponent depending on type.

        Args:
            components: List of AnalysisComponent containing properties of components.

        Returns:
            A CelonisCollection of AnalysisComponent objects with proper class based on type.
        """
        return CelonisCollection(ComponentFactory.get_component(component) for component in components)
