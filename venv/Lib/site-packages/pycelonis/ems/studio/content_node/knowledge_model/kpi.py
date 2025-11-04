import logging
import typing
from typing import Optional

from pycelonis.service.semantic_layer.service import KpiMetadata, SemanticLayerService
from pycelonis_core.client.client import Client

try:
    from pydantic.v1 import Field  # type: ignore
except ImportError:
    from pydantic import Field  # type: ignore


logger = logging.getLogger(__name__)


class Kpi(KpiMetadata):
    """Kpi object to interact with kpi specific endpoints."""

    client: Client = Field(..., exclude=True)
    id: str
    """Id of Kpi."""
    display_name: Optional[str]
    """Display name of Kpi."""
    pql: Optional[str]
    """PQL query of Kpi."""
    knowledge_model_id: str
    """Id of knowledge model where Kpi is located."""

    @classmethod
    def from_transport(cls, client: Client, knowledge_model_id: str, kpi_transport: KpiMetadata) -> "Kpi":
        """Creates high-level Kpi object from given KpiMetadata.

        Args:
            client: Client to use to make API calls for given kpi.
            knowledge_model_id: Id of knowledge model where kpi is located.
            kpi_transport: KpiMetadata object containing properties of kpi.

        Returns:
            A Kpi object with properties from transport and given client.
        """
        return cls(client=client, knowledge_model_id=knowledge_model_id, **kpi_transport.dict())

    def update(self) -> None:
        """Pushes local changes of kpi to EMS and updates properties with response from EMS."""
        updated_kpi = SemanticLayerService.put_api_knowledge_model_layer_asset_id_kpis_kpi_id(
            self.client, self.knowledge_model_id, self.id, self
        )
        logger.info("Successfully updated kpi with id '%s'", self.id)
        self._update(updated_kpi)

    def sync(self) -> None:
        """Syncs kpi properties with EMS."""
        synced_kpi = SemanticLayerService.get_api_knowledge_model_layer_asset_id_kpis_kpi_id(
            self.client, self.knowledge_model_id, self.id
        )
        self._update(synced_kpi)

    def delete(self) -> None:
        """Deletes kpi."""
        SemanticLayerService.delete_api_knowledge_model_layer_asset_id_kpis_kpi_id(
            self.client, self.knowledge_model_id, self.id
        )
        logger.info("Successfully deleted kpi with id '%s'", self.id)

    def __main_attributes__(self) -> typing.Optional[typing.List[str]]:
        return ["id", "display_name", "pql", "knowledge_model_id"]
