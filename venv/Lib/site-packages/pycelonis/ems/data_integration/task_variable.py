import logging
from typing import Optional

from pycelonis.service.integration.service import IntegrationService, TaskVariableTransport
from pycelonis_core.client.client import Client
from pycelonis_core.utils.errors import PyCelonisValueError

try:
    from pydantic.v1 import Field  # type: ignore
except ImportError:
    from pydantic import Field  # type: ignore


logger = logging.getLogger(__name__)


class TaskVariable(TaskVariableTransport):
    """TaskVariable object to interact with task variables specific data integration endpoints."""

    client: Client = Field(..., exclude=True)
    id: Optional[str]
    """Id of task variable."""
    pool_id: str
    """Id of data pool where task is located."""

    @property
    def task_instance_id(self) -> str:
        """Returns task instance id for given task variable.

        Returns:
            Task instance id of task variable.
        """
        if (
            self.values is None
            or len(self.values) == 0
            or self.values[0] is None
            or self.values[0].task_instance_id is None
        ):
            raise PyCelonisValueError("Incorrect TaskVariable instance. Either values field is empty or length is 0.")
        return self.values[0].task_instance_id

    @classmethod
    def from_transport(cls, client: Client, task_variable_transport: TaskVariableTransport) -> "TaskVariable":
        """Creates high-level task variable object from the given TaskVariableTransport.

        Args:
            client: Client to use to make API calls for given job.
            task_variable_transport: TaskVariableTransport object containing properties of task.

        Returns:
            A TaskVariable object with properties from transport and given client.
        """
        return cls(client=client, **task_variable_transport.dict())

    def update(self) -> None:
        """Pushes local changes of task variable to EMS and updates properties with response from EMS."""
        self._verify_id_set()

        updated_task_variable = IntegrationService.put_api_pools_pool_id_tasks_task_instance_id_variables_id(
            self.client,
            self.pool_id,
            self.task_instance_id,
            self.id,  # type: ignore
            self,
        )
        logger.info("Successfully updated task variable with id '%s'", self.id)
        self._update(updated_task_variable)

    def delete(self) -> None:
        """Deletes task variable."""
        self._verify_id_set()

        IntegrationService.delete_api_pools_pool_id_tasks_task_instance_id_variables_id(
            self.client,
            self.pool_id,
            self.task_instance_id,
            self.id,  # type: ignore
        )
        logger.info("Successfully deleted task variable with id '%s'", self.id)

    def _verify_id_set(self) -> None:
        if self.id is None:
            raise ValueError("TaskVariable id must be set to operate.")
