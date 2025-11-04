import logging
from typing import TYPE_CHECKING, Any, Optional, Union

import pandas as pd
from pycelonis.ems.data_integration.data_model_table_column import DataModelTableColumn
from pycelonis.pql.util import convert_column_to_pql_string, extract_saola_connector
from pycelonis_core.utils.ml_workbench import INTERNAL_TRACKING_LOGGER
from saolapy.pandas.series import Series as BaseSeries
from saolapy.types import ColumnLike

internal_tracking_logger = logging.getLogger(INTERNAL_TRACKING_LOGGER)


if TYPE_CHECKING:
    from pycelonis.ems import DataModel


class Series(BaseSeries):
    """PyCelonis series class."""

    def __init__(
        self,
        data: Union[ColumnLike, DataModelTableColumn],
        *args: Any,
        data_model: Optional["DataModel"] = None,
        **kwargs: Any,
    ) -> None:
        internal_tracking_logger.debug("Initialize Series.", extra={"tracking_type": "SAOLAPY"})

        kwargs["saola_connector"] = extract_saola_connector(
            saola_connector=kwargs.get("saola_connector", None), data_model=data_model
        )

        if isinstance(data, DataModelTableColumn):
            if "name" not in kwargs:
                kwargs["name"] = data.name
            data = convert_column_to_pql_string(data)

        super().__init__(data, *args, **kwargs)

    def to_pandas(self, *args: Any, **kwargs: Any) -> pd.Series:
        """Exports data from data model."""
        series = super().to_pandas(*args, **kwargs)
        internal_tracking_logger.debug(
            "Exported Series.", extra={"nrows": series.shape[0], "tracking_type": "SAOLAPY_EXPORT"}
        )
        return series

    def _repr_pretty_(self, p: Any, cycle: bool) -> None:
        return super()._repr_pretty_(p, cycle)
