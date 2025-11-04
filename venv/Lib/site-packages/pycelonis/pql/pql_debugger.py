from functools import lru_cache
from textwrap import indent
from typing import List

from pycelonis.pql.pql_parser import PQLParser
from pycelonis.service.pql_language.service import (
    Diagnostic,
    PqlBasicBatchParams,
    PqlBasicParams,
    PqlLanguageService,
    PqlQueryType,
)
from pycelonis_core.client.client import Client


class PQLDebugger:
    """Class for debugging PQL queries."""

    @staticmethod
    @lru_cache(maxsize=1024)
    def debug(client: Client, data_model_id: str, query: str, query_type: PqlQueryType) -> List[str]:
        """Debugs given query and returns formatted error messages.

        Args:
            client: Client to call pql language API.
            data_model_id: Data model to run query on.
            query: Query to verify.
            query_type: Type of query.

        Returns:
            List of error messages.
        """
        pql_basic_batch_params = PqlBasicBatchParams(
            batch=[PqlBasicParams(query=query, query_type=query_type, data_model_id=data_model_id)]
        )

        batch_response = PqlLanguageService.post_api_lsp_publish_diagnostics_batch(client, pql_basic_batch_params)

        return [
            PQLDebugger._format_error_message(query, diagnostic)
            for result in batch_response.results or []
            if result is not None
            for diagnostic in result.diagnostics or []
            if diagnostic is not None
        ]

    @staticmethod
    def _format_error_message(query: str, diagnostic: Diagnostic) -> str:
        error_cause = PQLParser.extract_sub_query(query, diagnostic.range.start, diagnostic.range.end)  # type: ignore
        return (
            f"Error in line {diagnostic.range.start.line} column {diagnostic.range.start.character}:\n"  # type: ignore
            f"{indent(query, '  ')}\n\n"
            f"{diagnostic.message}: {error_cause}"
        )
