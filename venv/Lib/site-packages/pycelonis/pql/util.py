import typing

from pycelonis.config import Config
from pycelonis.ems.data_integration.data_model_table_column import DataModelTableColumn
from pycelonis.pql.saola_connector import DataModelSaolaConnector
from saolapy.saola_connector import SaolaConnector

if typing.TYPE_CHECKING:
    from pycelonis.ems import DataModel


def convert_column_to_pql_string(column: DataModelTableColumn) -> str:
    """Converts DataModelTableColumn to PQL string.

    Args:
        column: DataModelTableColumn to convert.

    Returns:
        PQL string.
    """
    return f'"{column.table_alias or column.table_name}"."{column.name}"'


def extract_saola_connector(
    saola_connector: typing.Optional["SaolaConnector"] = None, data_model: typing.Optional["DataModel"] = None
) -> typing.Optional["SaolaConnector"]:
    """Returns saola connector. Uses data model if given. If not it uses the given saola connector or none."""
    if saola_connector is not None and data_model is not None:
        raise ValueError("Can't set `saola_connector` and `data_model` simultaneously.")

    if data_model is not None:
        return DataModelSaolaConnector(data_model=data_model)
    if saola_connector is not None:
        return saola_connector
    if Config.DEFAULT_DATA_MODEL is not None:
        return DataModelSaolaConnector(data_model=Config.DEFAULT_DATA_MODEL)
    return None
