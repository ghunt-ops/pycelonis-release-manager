import typing

from pycelonis.service.integration.service import PoolColumn, PoolColumnType
from pycelonis_core.client.client import Client

try:
    from pydantic.v1 import Field  # type: ignore
except ImportError:
    from pydantic import Field  # type: ignore


class DataModelTableColumn(PoolColumn):
    """Data model table column object to interact with data model table column specific data integration endpoints."""

    client: Client = Field(..., exclude=True)
    name: typing.Optional[str]
    """Name of data model table column."""
    length: typing.Optional[int]
    """Length of data model table column."""
    type_: typing.Optional[PoolColumnType]
    """Data type of data model table column."""
    data_pool_id: str
    """Id of data pool where data model table column is located."""
    data_model_id: str
    """Id of data model where data model table column is located."""
    table_name: str
    """Name of table where data model table column is located."""
    table_alias: typing.Optional[str]
    """Alias of table where data model table column is located."""

    @classmethod
    def from_transport(
        cls,
        client: Client,
        data_pool_id: str,
        data_model_id: str,
        table_name: str,
        table_alias: typing.Optional[str],
        pool_column_transport: PoolColumn,
    ) -> "DataModelTableColumn":
        """Creates high-level data model table column object from given PoolColumn.

        Args:
            client: Client to use to make API calls for given data model table.
            data_pool_id: Id of data pool where table is located.
            data_model_id: Id of data model where table is located
            table_name: Name of data model table where column is located.
            table_alias: Alias of data model table where column is located.
            pool_column_transport: PoolColumn object containing properties of data model table column.

        Returns:
            A DataModelTableColumn object with properties from transport and given client.
        """
        return cls(
            client=client,
            data_pool_id=data_pool_id,
            data_model_id=data_model_id,
            table_name=table_name,
            table_alias=table_alias,
            **pool_column_transport.dict(),
        )
