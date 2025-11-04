"""This module contains the global configuration for PyCelonis."""

import typing

from pycelonis_core.utils.errors import PyCelonisAttributeError

if typing.TYPE_CHECKING:
    from pycelonis.ems import DataModel


class _ConfigMeta(type):
    """Metaclass for PyCelonis configuration."""

    def __setattr__(cls, name: str, value: typing.Any) -> None:
        if name == "_config":
            super(_ConfigMeta, cls).__setattr__(name, value)
        elif name in cls._config:
            if name == "POLLING_WAIT_SECONDS" and value < 1:
                raise ValueError("POLLING_WAIT_SECONDS needs to be greater than 1.")
            cls._config[name] = value
        else:
            raise PyCelonisAttributeError(f"Config has no attribute {name}.")

    def __getattr__(cls, name: str) -> typing.Any:
        if name == "_config" or name not in cls._config:
            return super(_ConfigMeta, cls).__getattribute__(name)
        return cls._config[name]


class Config(metaclass=_ConfigMeta):
    """Global configuration for PyCelonis."""

    POLLING_WAIT_SECONDS: int
    """Time to wait between API calls during polling."""

    DISABLE_TQDM: bool
    """If true, tqdm is disabled."""

    DEFAULT_DATA_MODEL: typing.Optional["DataModel"]
    """Default data model used by PyCelonis."""

    _DEFAULT_CONFIG = {
        "POLLING_WAIT_SECONDS": 2,
        "DISABLE_TQDM": False,
        "DEFAULT_DATA_MODEL": None,
    }

    _config = dict(_DEFAULT_CONFIG)

    @classmethod
    def load(cls, config: typing.Dict) -> None:
        """Loads values from dictionary into configuration.

        Args:
            config: Dictionary containing new configuration values.
        """
        for key, value in config.items():
            setattr(cls, key, value)

    @classmethod
    def export(cls) -> typing.Dict:
        """Returns configuration as dictionary.

        Returns:
            Configuration dictionary.
        """
        return cls._config

    @classmethod
    def reset(cls) -> None:
        """Resets configuration to default values."""
        cls._config = dict(Config._DEFAULT_CONFIG)
