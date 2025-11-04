"""Module to define the PyDantic base model used in PyCelonis.

This module defines the PyDantic base model used in PyCelonis as well as all required encoding and conversion functions.

Typical usage example:

    ```python
    class PyCelonisModel(
        PyCelonisBaseModel
    ):
        pass
    ```
"""

import enum
import json
import warnings
from datetime import datetime
from typing import Any, Callable, Dict, Generator, List, Optional, Union

try:
    from pydantic.v1 import BaseConfig, BaseModel  # type: ignore
    from pydantic.v1.fields import ModelField  # type: ignore
    from pydantic.v1.validators import enum_member_validator  # type: ignore
except ImportError:
    from pydantic import BaseConfig, BaseModel  # type: ignore
    from pydantic.fields import ModelField  # type: ignore
    from pydantic.validators import enum_member_validator  # type: ignore


def to_camel(string: str) -> str:
    """Converts snake case string to camel case.

    Args:
        string: Snake case string to convert.

    Returns:
        A string in camel case.
    """
    temp = string.split("_")
    return temp[0] + "".join(e.title() for e in temp[1:])


def to_unix_timestamp_ms(timestamp: datetime) -> int:
    """Returns datetime in milliseconds since epoch.

    Args:
        timestamp: timestamp to convert to millisecond since epoch.

    Returns:
        An int representing the milliseconds since epoch.
    """
    return int(timestamp.timestamp() * 1e3)


class PyCelonisBaseModel(BaseModel):
    """Base Model for any PyCelonis object."""

    class Config:
        """Basic configuration used by PyCelonis PyDantic models."""

        json_encoders = {datetime: to_unix_timestamp_ms}
        alias_generator = to_camel
        allow_population_by_field_name = True
        arbitrary_types_allowed = True
        use_enum_values = True

    def json_dict(self, *args: Any, **kwargs: Any) -> Dict:
        """Convert model to dict with encoded properties.

        Args:
            args: Args that are passed to `self.json`.
            kwargs: Kwargs that are passed to `self.json`.

        Returns:
            A dictionary with encoded properties of model.
        """
        return json.loads(
            self.json(*args, **kwargs)
        )  # Workaround due to https://github.com/samuelcolvin/pydantic/issues/1409

    def _update(self, transport: "PyCelonisBaseModel") -> None:
        """Updates object properties with properties of transport."""
        for key in transport.dict().keys():
            if key in self.__getter_attributes__():
                continue
            setattr(self, key, getattr(transport, key))

    def __main_attributes__(self) -> Optional[List[str]]:
        """Main attributes of object that will be shown as representation. If None, all attributes are shown."""
        return None

    def __getter_attributes__(self) -> List[str]:
        """Getter attributes are not accessible as property anymore. If empty, all attributes are accessible."""
        return []

    def __repr_str__(self, join_str: str, main_attributes: Optional[List] = None) -> str:
        if main_attributes is None:
            return super().__repr_str__(join_str)

        return join_str.join(
            repr(v) if a is None else f"{a}={v!r}" for a, v in self.__repr_args__() if a in main_attributes
        )

    def __repr__(self) -> str:
        return f"{self.__repr_name__()}({self.__repr_str__(', ', main_attributes=self.__main_attributes__())})"

    def __getattribute__(self, item: str) -> Any:
        if item != "__getter_attributes__" and item in self.__getter_attributes__():
            warnings.warn(
                f"Accessing {item} as attribute only returns partial information. Use the get_{item} function instead."
            )
        return super().__getattribute__(item)


class PyCelonisBaseEnum(str, enum.Enum):
    """Base class for any PyCelonis enum."""

    def __str__(self) -> str:
        """Return value, so it can be used as input to httpx requests."""
        return self.value

    @classmethod
    def __get_validators__(cls) -> Generator[Callable, None, None]:
        yield cls._validate

    @classmethod
    def _validate(cls, value: Any, field: "ModelField", config: "BaseConfig") -> Union[str, enum.Enum]:
        """Validate enum reference without checking if it's a member to improve backwards compatibility."""
        if not config.use_enum_values:
            return enum_member_validator(value, field=field, config=config)

        if not isinstance(value, (str, cls)):
            raise ValueError(f"value `{value}` is not a valid `{cls.__name__}`")

        return value.value if isinstance(value, cls) else value
