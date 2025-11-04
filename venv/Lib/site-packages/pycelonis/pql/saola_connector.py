import typing
from typing import TYPE_CHECKING, List

import pandas as pd
from pycelonis.pql.pql_debugger import PQLDebugger
from pycelonis.pql.pql_parser import PQLParser
from pycelonis.service.integration.service import ProxyQueryEnvironmentV2, ProxySavedFormulaV2, QueryEnvironment
from pycelonis.service.pql_language.service import PqlQueryType
from pycelonis_core.utils.errors import PyCelonisHTTPStatusError, PyCelonisValueError
from saolapy.pql.base import PQL, PQLColumn, PQLFilter
from saolapy.saola_connector import SaolaConnector
from pycelonis.utils.deprecation import deprecation_class_warning

if TYPE_CHECKING:
    from pycelonis.ems import DataModel
    from pycelonis.ems.apps.content_node.analysis import PublishedAnalysis
    from pycelonis.ems.studio.content_node.knowledge_model import KnowledgeModel


def verify_columns(data_model: "DataModel", columns: List[PQLColumn]) -> None:
    """Verifies given columns."""
    for col in columns:
        error_messages = PQLDebugger.debug(data_model.client, data_model.id, col.query, PqlQueryType.DIMENSION)
        if error_messages:
            raise ValueError(f"Errors in column '{col.name}':\n\n" + "\n\n\n".join(error_messages))


def verify_filters(data_model: "DataModel", filters: List[PQLFilter]) -> None:
    """Verifies given filters."""
    for filter_ in filters:
        error_messages = PQLDebugger.debug(data_model.client, data_model.id, filter_.query, PqlQueryType.FILTER)
        if error_messages:
            raise ValueError("Errors in filter:\n\n" + "\n\n\n".join(error_messages))


class DataModelSaolaConnector(SaolaConnector):
    """Data model saola connector."""

    def __init__(self, data_model: "DataModel", use_api_v2: bool = False):
        self.data_model = data_model
        self._use_api_v2 = use_api_v2

    def _export_data(self, query: PQL) -> pd.DataFrame:
        """Exports given PQL as data frame."""
        return self.data_model._export_data_frame(query, use_api_v2=self._use_api_v2)

    def verify_query(self, query: PQL) -> None:
        """Verifies given query."""
        verify_columns(self.data_model, query.columns)
        verify_filters(self.data_model, query.filters)

    def convert_filter_to_expressions(self, filter_: PQLFilter) -> List[str]:
        """Converts given pql filter to conditional expressions.

        Args:
            filter_: PQL filter to convert.

        Returns:
            Conditional expressions resulting from pql filter.
        """
        return PQLParser.convert_filter_to_expressions(self.data_model.client, self.data_model.id, filter_.query)


class KnowledgeModelSaolaConnector(SaolaConnector):
    """Knowledge model saola connector."""

    def __init__(self, data_model: "DataModel", knowledge_model: "KnowledgeModel", draft: bool = True):
        """Initialize the knowledge model connector using the given data model and knowledge models.

        Args:
            data_model: Data model to use within the connector
            knowledge_model: Knowledge model to use within the connector
            draft: If true, uses draft of knowledge model for data exports, if false uses published version.
        """
        self.data_model = data_model
        self.knowledge_model = knowledge_model
        self.draft = draft

    def _export_data(self, query: PQL, **kwargs: typing.Any) -> pd.DataFrame:
        """Exports given PQL as data frame."""
        return self.knowledge_model._export_data_frame(query, self.draft)

    def verify_query(self, query: PQL) -> None:
        """Verifies given query."""
        # Currently not supported by PQL service due to KPIs and variables

    def convert_filter_to_expressions(self, filter_: PQLFilter) -> List[str]:
        """Converts given pql filter to conditional expressions.

        Args:
            filter_: PQL filter to convert.

        Returns:
            Conditional expressions resulting from pql filter.
        """
        filter_list = []
        try:
            filter_list += PQLParser.convert_filter_to_expressions(
                self.data_model.client, self.data_model.id, filter_.query
            )
        except PyCelonisHTTPStatusError as e:
            if e.message is not None and e.message.find("400 Bad Request"):
                raise PyCelonisValueError(
                    "Complex filters (containing e.g. KPIs) are not supported with the KnowledgeModelSaolaConnector."
                    "Please, remove the filter or use the DataModelSaolaConnector instead."
                ) from e
            raise e
        return filter_list


class AnalysisSaolaConnector(SaolaConnector):
    """Analysis saola connector.

    !!! warning
        The class `AnalysisSaolaConnector` has been deprecated and will be removed in future versions.
        Please use `KnowledgeModelConnector` from now on to export data
        You can refer from tutorial `Pulling Data from an Analysis`.
    """

    def __init__(self, data_model: "DataModel", analysis: "PublishedAnalysis", use_api_v2: bool = False):
        self.data_model = data_model
        self.analysis = analysis
        self._use_api_v2 = use_api_v2
        deprecation_class_warning(
            "`AnalysisSaolaConnector`", "Please refer to `KnowledgeModelConnector` for data exports."
        )

    def _export_data(self, query: PQL) -> pd.DataFrame:
        """Exports given PQL as data frame."""
        data_query, query_environment = self.analysis._resolve_query(query)
        if self._use_api_v2:
            if query_environment:
                query_environment = _query_environment_to_v2(query_environment)  # type: ignore

        return self.data_model._export_data_frame(data_query, query_environment, use_api_v2=self._use_api_v2)

    def verify_query(self, query: PQL) -> None:
        """Verifies given query."""
        # Currently not supported by PQL service due to KPIs and variables

    def convert_filter_to_expressions(self, filter_: PQLFilter) -> List[str]:
        """Converts given pql filter to conditional expressions.

        Args:
            filter_: PQL filter to convert.

        Returns:
            Conditional expressions resulting from pql filter.
        """
        filter_list = []
        try:
            filter_list += PQLParser.convert_filter_to_expressions(
                self.data_model.client, self.data_model.id, filter_.query
            )
        except PyCelonisHTTPStatusError as e:
            if e.message is not None and e.message.find("400 Bad Request"):
                raise PyCelonisValueError(
                    "Complex filters (containing e.g. variables and KPIs) are not supported with the "
                    "AnalysisSaolaConnector. Please, remove the filter or use the DataModelSaolaConnector instead."
                ) from e
            raise e
        return filter_list


def _query_environment_to_v2(query_environment: QueryEnvironment) -> ProxyQueryEnvironmentV2:
    saved_formulas = []
    if query_environment.kpi_infos.kpis:  # type: ignore
        for kpi in query_environment.kpi_infos.kpis.values():  # type: ignore
            saved_formulas.append(
                ProxySavedFormulaV2(
                    error=kpi.error,  # type: ignore
                    formula=kpi.formula,  # type: ignore
                    name=kpi.name,  # type: ignore
                    parameter_count=kpi.parameter_count,  # type: ignore
                )
            )

    return ProxyQueryEnvironmentV2(
        saved_formulas=saved_formulas,
        load_script=query_environment.load_script,
        user_name=query_environment.user_name,
    )
