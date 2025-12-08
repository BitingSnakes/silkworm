from __future__ import annotations

import os
from typing import Any

from logly import logger as _logger  # type: ignore[import]

_configured = False


def _configure_if_needed() -> Any:
    """
    Configure the shared Logly logger once using env overrides and
    return it so callers can bind additional context.
    """
    global _configured
    if _configured:
        return _logger

    level = os.getenv("SILKWORM_LOG_LEVEL", "INFO").upper()
    _logger.configure(
        level=level,
        show_time=True,
        show_module=True,
        show_function=False,
        show_filename=False,
        show_lineno=False,
    )
    _configured = True
    return _logger


def get_logger(**context: Any) -> Any:
    """
    Grab the shared Logly logger with optional bound context fields.
    """
    base = _configure_if_needed()
    return base.bind(**context) if context else base


def complete_logs() -> None:
    """
    Flush buffered log messages if the logger has been configured.
    """
    if not _configured:
        return
    _logger.complete()
