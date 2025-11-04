import logging
import typing

from pycelonis.service.integration.service import IntegrationService, PoolVariableTransport
from pycelonis_core.client.client import Client

try:
    from pydantic.v1 import Field  # type: ignore
except ImportError:
    from pydantic import Field  # type: ignore

logger = logging.getLogger(__name__)


class PoolVariable(PoolVariableTransport):
    """PoolVariable object to interact with data pool variable specific data integration endpoints."""

    client: Client = Field(..., exclude=True)
    id: str
    """Id of pool variable."""
    pool_id: str
    """Id of data pool to which pool variable corresponds."""
    name: typing.Optional[str]
    """Name of pool variable."""

    @classmethod
    def from_transport(cls, client: Client, pool_variable_transport: PoolVariableTransport) -> "PoolVariable":
        """Creates high-level data pool variable object from the given PoolVariableTransport.

        Args:
            client: Client to use to make API calls for given job.
            pool_variable_transport: PoolVariableTransport object containing properties of pool variable.

        Returns:
            A PoolVariable object with properties from transport and given client.
        """
        return cls(client=client, **pool_variable_transport.dict())

    def update(self) -> None:
        """Pushes local changes of data pool variable to EMS and updates properties with response from EMS."""
        updated_pool_variable = IntegrationService.put_api_pools_pool_id_variables_id(
            self.client, self.pool_id, self.id, self
        )
        logger.info("Successfully updated pool variable with id '%s'", self.id)
        self._update(updated_pool_variable)

    def delete(self) -> None:
        """Deletes pool variable."""
        IntegrationService.delete_api_pools_pool_id_variables_id(self.client, self.pool_id, self.id)
        logger.info("Successfully deleted pool variable with id '%s'", self.id)
