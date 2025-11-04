import io
import logging
import typing
import uuid

from pycelonis.config import Config
from pycelonis.errors import PyCelonisDataExportFailedError, PyCelonisDataExportRunningError
from pycelonis.service.semantic_layer.service import (
    ProxyDataExportStatusResponseV2 as SemanticProxyDataExportStatusResponseV2,
    SemanticLayerService,
    ExportStatusKnowledgeModelByRootWithKeyRequest,
    QueryContext,
    ProxyExportStatusV2,
)
from pycelonis.utils.polling import poll
from pycelonis_core.client.client import Client

try:
    from pydantic.v1 import Field  # type: ignore
except ImportError:
    from pydantic import Field  # type: ignore


logger = logging.getLogger(__name__)


class KnowledgeModelDataExport(SemanticProxyDataExportStatusResponseV2):
    """Data export object to interact with version knowledge model data exports using semantic layer endpoints."""

    client: Client = Field(..., exclude=True)
    id: uuid.UUID
    """Id of data export."""
    root_with_key: str
    """Knowledge model root concatenated with key."""
    query_context: str
    """Operational query context."""

    @classmethod
    def from_transport(
        cls,
        client: Client,
        root_with_key: str,
        query_context: QueryContext,
        data_export_transport: SemanticProxyDataExportStatusResponseV2,
    ) -> "KnowledgeModelDataExport":
        """Creates high-level data export object from given SemanticProxyDataExportStatusResponseV2.

        Args:
            client: Client to use to make API calls for given data pool.
            root_with_key: Knowledge model root concatenated by key.
            data_export_transport: SemanticProxyDataExportStatusResponseV2 object containing properties of data export.
            query_context: Operational level (published or unpublished) for the data export context.

        Returns:
            A SemanticProxyDataExportStatusResponseV2 object with properties from transport and given client.
        """
        return cls(
            client=client, root_with_key=root_with_key, query_context=query_context, **data_export_transport.dict()
        )

    ############################################################
    # Chunk
    ############################################################
    def get_chunks(self) -> typing.Generator[io.BytesIO, None, None]:
        """Yields exported chunks.

        Yields:
            Byte chunks of exported data.

        """
        logger.info("Export result chunks for data export with id '%s'", str(self.id))

        knowledge_model_data_export_status = self._get_status()

        if knowledge_model_data_export_status.export_status == ProxyExportStatusV2.RUNNING:
            raise PyCelonisDataExportRunningError()

        if knowledge_model_data_export_status.export_status != ProxyExportStatusV2.DONE:
            raise PyCelonisDataExportFailedError(
                knowledge_model_data_export_status.export_status, knowledge_model_data_export_status.messages
            )

        for chunk in knowledge_model_data_export_status.exported_chunks:  # type: ignore
            yield self._get_chunk_result(chunk.id)  # type: ignore

    def wait_for_execution(self) -> None:
        """Waits until data export execution is done."""

        def is_done(data_export_status: SemanticProxyDataExportStatusResponseV2) -> bool:
            """Returns whether data export is still running."""
            return data_export_status.export_status != ProxyExportStatusV2.RUNNING

        def format_status(data_export_status: SemanticProxyDataExportStatusResponseV2) -> str:
            """Returns formatted data export status containing status and message."""
            formatted_export_status = "Status:"

            if data_export_status.export_status:
                formatted_export_status += f" {data_export_status.export_status}"
            if data_export_status.messages:
                for message in data_export_status.messages:
                    formatted_export_status += f" {message}"

            return formatted_export_status

        logger.info("Wait for execution of data export with id '%s'", str(self.id))
        poll(
            target=self._get_status,
            wait_for=is_done,
            message=format_status,
            sleep=Config.POLLING_WAIT_SECONDS,
        )

    def _get_chunk_result(self, chunk_id: int) -> io.BytesIO:
        """Gets export result from chunked endpoint.."""
        return SemanticLayerService.post_api_knowledge_models_by_knowledge_model_root_with_key_root_with_key_exports_export_id_chunks_chunk_id(
            self.client,
            self.root_with_key,
            str(self.id),
            str(chunk_id),
            ExportStatusKnowledgeModelByRootWithKeyRequest(query_context=self.query_context),
        )

    def _get_status(self) -> SemanticProxyDataExportStatusResponseV2:
        export_query_response = SemanticLayerService.post_api_knowledge_models_by_knowledge_model_root_with_key_root_with_key_exports_export_id(
            self.client,
            self.root_with_key,
            str(self.id),
            ExportStatusKnowledgeModelByRootWithKeyRequest(query_context=self.query_context),
        )
        return export_query_response.query_response  # type:ignore

    def __main_attributes__(self) -> typing.Optional[typing.List[str]]:
        return ["id", "export_status", "messages"]
