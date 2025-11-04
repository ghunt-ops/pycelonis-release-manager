"""Module for analysis content."""

import typing

from pycelonis.ems.apps.content_node.analysis.component import AnalysisComponent, ComponentFactory
from pycelonis.pql import PQLFilter
from pycelonis.service.process_analytics.service import AnalysisTransport, DraftTransport, KpiTransport
from pycelonis_core.base.base_model import PyCelonisBaseModel
from pycelonis_core.base.collection import CelonisCollection

try:
    from pydantic.v1 import Extra, validator  # type: ignore
except ImportError:
    from pydantic import Extra, validator  # type: ignore


class AnalysisSheet(PyCelonisBaseModel, extra=Extra.allow):  # type: ignore
    """Class for analysis sheet containing components such as tables or kpis."""

    id: typing.Optional[str]
    """Analysis sheet id."""
    name: typing.Optional[str]
    """Name of analysis sheet."""
    sheet_filter: typing.Dict
    """Sheet filter, filtering data in sheet."""
    components: typing.Optional[CelonisCollection[AnalysisComponent]]
    """Components (tables, kpis, etc.) of sheet."""

    # validators
    _components_validator = validator("components", allow_reuse=True)(ComponentFactory.get_components)

    def get_filter(self) -> typing.Optional[PQLFilter]:
        """Returns sheet filter.

        Returns:
            Sheet filter.

        Examples:
            Adding sheet filter to custom query and extract data:
            ```python
            sheet = analysis.get_content().draft.document.sheets[
                0
            ]

            query = (
                PQL()
                + PQLColumn(
                    name="TEST",
                    query="<query",
                )
            )
            query += sheet.get_filter()

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
        if self.sheet_filter and self.sheet_filter.get("text", None):
            return PQLFilter(query=self.sheet_filter["text"])
        return None

    def __main_attributes__(self) -> typing.List[str]:
        return ["id", "name"]


class AnalysisDocument(PyCelonisBaseModel, extra=Extra.allow):  # type: ignore
    """Class for analysis document containing sheets."""

    id: typing.Optional[str]
    """Id of analysis."""
    name: typing.Optional[str]
    """Name of analysis."""
    components: typing.Optional[CelonisCollection[AnalysisSheet]]
    """Sheets of analysis."""
    stateless_load_script: typing.Optional[str]
    """Load script of analysis."""

    variables: typing.Optional[typing.List]
    """Variables of analysis."""

    # validators
    _components_validator = validator("components", allow_reuse=True)(CelonisCollection.from_list)

    @property
    def sheets(self) -> typing.Optional[CelonisCollection[AnalysisSheet]]:
        """Returns sheets of analysis document.

        Returns:
            Analysis sheet.
        """
        return self.components

    def get_filter(self) -> typing.Optional[PQLFilter]:
        """Returns load script filter.

        Returns:
            Load script PQL filter.

        Examples:
            Adding load script filter to custom query and extract data:
            ```python
            document = analysis.get_content().draft.document

            query = (
                PQL()
                + PQLColumn(
                    name="TEST",
                    query="<query",
                )
            )
            query += document.get_filter()

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
        return PQLFilter(query=self.stateless_load_script) if self.stateless_load_script else None

    def __main_attributes__(self) -> typing.List[str]:
        return ["id", "name"]


class AnalysisDraft(DraftTransport):
    """Class for analysis draft containing document."""

    title: typing.Optional[str]
    """Title of analysis draft."""
    document: typing.Optional[AnalysisDocument]  # Overwrite typing.Any to use AnalysisDocument class instead of dict
    """Contains sheets and components of draft."""

    def __main_attributes__(self) -> typing.List[str]:
        return ["id", "title"]


class AnalysisInfo(AnalysisTransport):
    """Class for basic analysis metadata."""

    def __main_attributes__(self) -> typing.List[str]:
        return ["id", "name"]


class AnalysisContent(PyCelonisBaseModel, extra=Extra.allow):  # type: ignore
    """Class for analysis content to read properties from analysis."""

    analysis: typing.Optional[AnalysisInfo]
    """Metadata of analysis."""
    kpis: typing.Optional[CelonisCollection[KpiTransport]]
    """KPIs defined in analysis."""
    draft: typing.Optional[AnalysisDraft]
    """Draft of analysis containing sheets and components."""
    data_model_id: typing.Optional[str]
    """Id of data model used for analysis."""

    # validators
    _kpis_validator = validator("kpis", allow_reuse=True)(CelonisCollection.from_list)

    def __main_attributes__(self) -> typing.List[str]:
        return ["data_model_id", "analysis", "draft"]
