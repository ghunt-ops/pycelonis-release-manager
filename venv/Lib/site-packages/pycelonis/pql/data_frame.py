import logging
from typing import TYPE_CHECKING, Any, List, MutableMapping, Optional, Type, Union

import pandas as pd
from pycelonis.ems.data_integration.data_model_table_column import DataModelTableColumn
from pycelonis.pql.series import Series
from pycelonis.pql.util import convert_column_to_pql_string, extract_saola_connector
from pycelonis_core.utils.ml_workbench import INTERNAL_TRACKING_LOGGER
from saolapy.pandas.data_frame import DataFrame as BaseDataFrame
from saolapy.types import SeriesLike

if TYPE_CHECKING:
    from pycelonis.ems import DataModel

internal_tracking_logger = logging.getLogger(INTERNAL_TRACKING_LOGGER)


class DataFrame(BaseDataFrame):
    """PyCelonis DataFrame class."""

    _series_class: Type[Series] = Series

    def __init__(
        self,
        data: MutableMapping[str, Union[SeriesLike, "DataModelTableColumn"]],
        *args: Any,
        data_model: Optional["DataModel"] = None,
        **kwargs: Any,
    ) -> None:
        internal_tracking_logger.debug("Initialize DataFrame.", extra={"tracking_type": "SAOLAPY"})

        kwargs["saola_connector"] = extract_saola_connector(
            saola_connector=kwargs.get("saola_connector", None), data_model=data_model
        )

        for key in data.keys():
            if isinstance(data[key], DataModelTableColumn):
                data[key] = convert_column_to_pql_string(data[key])  # type: ignore

        super().__init__(data, *args, **kwargs)  # type: ignore

    def to_pandas(self, *args: Any, **kwargs: Any) -> pd.DataFrame:
        """Exports data from data model."""
        df = super().to_pandas(*args, **kwargs)
        internal_tracking_logger.debug(
            "Exported DataFrame.",
            extra={"nrows": df.shape[0], "ncolumns": df.shape[1], "tracking_type": "SAOLAPY_EXPORT"},
        )
        return df

    def __setitem__(  # type: ignore
        self,
        key: Union[str, List[str]],
        value: Union[
            "DataFrame",
            SeriesLike,
            "DataModelTableColumn",
            List[Union[SeriesLike, "DataModelTableColumn"]],
        ],
    ) -> None:
        if isinstance(value, DataModelTableColumn):
            value = convert_column_to_pql_string(value)

        if isinstance(value, list) and not isinstance(value, str):
            value = [
                column
                if not isinstance(column, DataModelTableColumn)  # type: ignore
                else convert_column_to_pql_string(column)
                for column in value  # type: ignore
            ]

        super().__setitem__(key, value)  # type: ignore

    def _repr_pretty_(self, p: Any, cycle: bool) -> None:
        return super()._repr_pretty_(p, cycle)
