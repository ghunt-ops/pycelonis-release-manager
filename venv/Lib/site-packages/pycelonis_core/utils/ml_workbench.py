"""Module contains all ML Workbench specific functionality."""

import logging
import os
import sys
import typing
import uuid

ML_APP_ID_ENV = "CELONIS_ML_APP_ID"
CELONIS_URL_ENV = "CELONIS_URL"
CELONIS_API_TOKEN_ENV = "CELONIS_API_TOKEN"
CELONIS_KEY_TYPE_ENV = "CELONIS_KEY_TYPE"
OAUTH_CLIENT_ID_ENV = "OAUTH_CLIENT_ID"
OAUTH_CLIENT_SECRET_ENV = "OAUTH_CLIENT_SECRET"
OAUTH_SCOPES_ENV = "OAUTH_SCOPES"
ACCESS_TOKEN_PATH_ENV = "OAUTH_ACCESS_TOKEN_PATH"
OAUTH_METHOD = "OAUTH_METHOD"

# Logger for detailed usage statistics only used if tracking is activated for team in workbench
TRACKING_LOGGER = "pycelonis_tracking"

# Logger for product usage overview which is always activated
INTERNAL_TRACKING_LOGGER = "pycelonis_internal_tracking"


SUPPORT_ID = str(uuid.uuid4())
ML_APP_ID = os.environ.get(ML_APP_ID_ENV, "")
URL = os.environ.get(CELONIS_URL_ENV, "")
_URL = URL.split(".")
TEAM = _URL[0] if len(_URL) >= 1 else ""
ENV = _URL[1] if len(_URL) >= 2 else ""


def is_running_in_ml_workbench() -> bool:
    """Returns true if application is running within ML Workbench.

    Returns:
        Boolean specifying if application is running in ML Workbench.
    """
    return ML_APP_ID_ENV in os.environ


def setup_ml_workbench_logging() -> None:
    """Sets up ML Workbench specific logging configuration."""

    def _set_log_levels() -> None:
        logging.getLogger("urllib3").setLevel(logging.ERROR)
        logging.getLogger("ddtrace").setLevel(logging.CRITICAL)
        logging.getLogger("pycelonis").setLevel(logging.INFO)

    def _get_formatter() -> logging.Formatter:
        return logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")

    def _get_stderr_handler(formatter: logging.Formatter) -> logging.Handler:
        handler = logging.StreamHandler(sys.stderr)
        handler.setLevel(logging.WARNING)
        handler.setFormatter(formatter)
        return handler

    def _get_stdout_handler(formatter: logging.Formatter) -> logging.Handler:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(logging.DEBUG)
        handler.addFilter(lambda log: 1 if log.levelno < logging.WARNING else 0)  # type: ignore
        handler.setFormatter(formatter)
        return handler

    _set_log_levels()

    _log_formatter = _get_formatter()
    _stdout_handler = _get_stdout_handler(_log_formatter)
    _stderr_handler = _get_stderr_handler(_log_formatter)

    logger = logging.getLogger("pycelonis")
    logger.addHandler(_stderr_handler)
    logger.addHandler(_stdout_handler)


def setup_ml_workbench_tracking() -> None:
    """Sets up ML Workbench specific tracking in order to have logs available for support ticket process."""

    def _get_process_id() -> typing.Optional[int]:
        import psutil

        # Catches ZombieProcess, NoSuchProcess and replaces attrs
        for process in psutil.process_iter(attrs=["pid", "name"], ad_value="NotAvailable"):
            if process.name() == "jupyter-noteboo" and process.pid != "NotAvailable":
                return process.pid

        return None

    def _get_tracking_handler(output_file: str) -> logging.Handler:
        from pythonjsonlogger import jsonlogger

        handler = logging.FileHandler(output_file)
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(jsonlogger.JsonFormatter())
        return handler

    def _get_default_extra_filter() -> logging.Filter:
        from pycelonis.__version__ import __version__

        class _DefaultExtraFilter(logging.Filter):
            def filter(self, record: logging.LogRecord) -> bool:
                record.pycelonis = {  # type: ignore
                    "support_id": SUPPORT_ID,
                    "ml_app_id": ML_APP_ID,
                    "version": __version__,
                    "team": TEAM,
                    "env": ENV,
                    "url": URL,
                }
                return True

        return _DefaultExtraFilter()

    def _setup_dd() -> None:
        """Setup datadog for tracing."""
        try:
            from ddtrace import config, patch  # type: ignore
            from pycelonis.__version__ import __version__

            # dirty hack to avoid ddtrace error message in tutorials
            logging.getLogger("ddtrace._monkey").disabled = True
            logging.getLogger("ddtrace.internal.writer").disabled = True

            config.env = ENV
            config.service = "pycelonis"
            config.version = __version__
            patch(logging=True, requests=True)
        except ImportError:
            pass

    def _setup_tracking_logger(logger_name: str) -> None:
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.DEBUG)
        logger.addHandler(_tracking_handler)
        logger.addFilter(_extra_filter)

    _process_id = _get_process_id()
    if _process_id:
        _tracking_handler = _get_tracking_handler(f"/proc/{_process_id}/fd/1")

        _extra_filter = _get_default_extra_filter()

        _setup_tracking_logger(TRACKING_LOGGER)
        _setup_tracking_logger(INTERNAL_TRACKING_LOGGER)
        _setup_dd()
