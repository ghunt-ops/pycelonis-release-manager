import logging
import typing
from typing import Optional

from pycelonis.service.semantic_layer.service import SemanticLayerService, VariableMetadata
from pycelonis_core.client.client import Client

try:
    from pydantic.v1 import Field  # type: ignore
except ImportError:
    from pydantic import Field  # type: ignore

logger = logging.getLogger(__name__)


class Variable(VariableMetadata):
    """Variable object to interact with variable specific endpoints."""

    client: Client = Field(..., exclude=True)
    id: str
    """Id of variable."""
    display_name: Optional[str]
    """Display name of variable."""
    value: Optional[str]
    """Value of variable."""
    knowledge_model_id: str
    """Id of knowledge model where variable is located."""

    @classmethod
    def from_transport(
        cls, client: Client, knowledge_model_id: str, variable_transport: VariableMetadata
    ) -> "Variable":
        """Creates high-level Variable object from given VariableMetadata.

        Args:
            client: Client to use to make API calls for given variable.
            knowledge_model_id: Id of knowledge model where variable is located.
            variable_transport: VariableMetadata object containing properties of variable.

        Returns:
            A Variable object with properties from transport and given client.
        """
        return cls(client=client, knowledge_model_id=knowledge_model_id, **variable_transport.dict())

    def update(self) -> None:
        """Pushes local changes of variable to EMS and updates properties with response from EMS."""
        updated_variable = SemanticLayerService.put_api_knowledge_model_layer_asset_id_variables_variable_id(
            self.client, self.knowledge_model_id, self.id, self
        )
        logger.info("Successfully updated variable with id '%s'", self.id)
        self._update(updated_variable)

    def sync(self) -> None:
        """Syncs variable properties with EMS."""
        synced_variable = SemanticLayerService.get_api_knowledge_model_layer_asset_id_variables_variable_id(
            self.client, self.knowledge_model_id, self.id
        )
        self._update(synced_variable)

    def delete(self) -> None:
        """Deletes variable."""
        SemanticLayerService.delete_api_knowledge_model_layer_asset_id_variables_variable_id(
            self.client, self.knowledge_model_id, self.id
        )
        logger.info("Successfully deleted variable with id '%s'", self.id)

    def __main_attributes__(self) -> typing.Optional[typing.List[str]]:
        return ["id", "display_name", "value", "knowledge_model_id"]
