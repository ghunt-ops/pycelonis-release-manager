import logging
import typing
from typing import Optional

from pycelonis.service.semantic_layer.service import FilterMetadata, SemanticLayerService
from pycelonis_core.client.client import Client

try:
    from pydantic.v1 import Field  # type: ignore
except ImportError:
    from pydantic import Field  # type: ignore


logger = logging.getLogger(__name__)


class Filter(FilterMetadata):
    """Filter object to interact with filter specific endpoints."""

    client: Client = Field(..., exclude=True)
    id: str
    """Id of Filter."""
    display_name: Optional[str]
    """Display name of Filter."""
    pql: Optional[str]
    """PQL query of Filter."""
    knowledge_model_id: str
    """Id of knowledge model where Filter is located."""

    @classmethod
    def from_transport(cls, client: Client, knowledge_model_id: str, filter_transport: FilterMetadata) -> "Filter":
        """Creates high-level Filter object from given FilterMetadata.

        Args:
            client: Client to use to make API calls for given filter.
            knowledge_model_id: Id of knowledge model where filter is located.
            filter_transport: FilterMetadata object containing properties of filter.

        Returns:
            A Filter object with properties from transport and given client.
        """
        return cls(client=client, knowledge_model_id=knowledge_model_id, **filter_transport.dict())

    def update(self) -> None:
        """Pushes local changes of filter to EMS and updates properties with response from EMS."""
        updated_filter = SemanticLayerService.put_api_knowledge_model_layer_asset_id_filters_filter_id(
            self.client, self.knowledge_model_id, self.id, self
        )
        logger.info("Successfully updated filter with id '%s'", self.id)
        self._update(updated_filter)

    def sync(self) -> None:
        """Syncs filter properties with EMS."""
        synced_filter = SemanticLayerService.get_api_knowledge_model_layer_asset_id_filters_filter_id(
            self.client, self.knowledge_model_id, self.id
        )
        self._update(synced_filter)

    def delete(self) -> None:
        """Deletes filter."""
        SemanticLayerService.delete_api_knowledge_model_layer_asset_id_filters_filter_id(
            self.client, self.knowledge_model_id, self.id
        )
        logger.info("Successfully deleted filter with id '%s'", self.id)

    def __main_attributes__(self) -> typing.Optional[typing.List[str]]:
        return ["id", "display_name", "pql", "knowledge_model_id"]
