from __future__ import annotations

import os
from typing import Protocol, cast, runtime_checkable

from logly import logger as _logger  # type: ignore[import]

_typed_logger = cast("_Logger", _logger)


@runtime_checkable
class _Logger(Protocol):
    def configure(
        self,
        *,
        level: str,
        show_time: bool,
        show_module: bool,
        show_function: bool,
        show_filename: bool,
        show_lineno: bool,
        console_levels: dict[str, bool] | None = None,
    ) -> None: ...

    def bind(self, **context: object) -> "_Logger": ...

    def info(self, message: str, **context: object) -> None: ...
    def debug(self, message: str, **context: object) -> None: ...
    def warning(self, message: str, **context: object) -> None: ...
    def error(self, message: str, **context: object) -> None: ...
    def complete(self) -> None: ...


_LEVELS: tuple[str, ...] = (
    "TRACE",
    "DEBUG",
    "INFO",
    "SUCCESS",
    "WARNING",
    "ERROR",
    "CRITICAL",
    "FAIL",
)
_ALIASES = {
    "WARN": "WARNING",
    "ERR": "ERROR",
    "FATAL": "FAIL",
}

_configured = False


def _normalized_level(raw_level: str) -> str:
    """
    Normalize user-provided log levels to values accepted by logly.
    Unknown levels fall back to INFO.
    """
    level = raw_level.upper()
    level = _ALIASES.get(level, level)
    return level if level in _LEVELS else "INFO"


def _console_levels(min_level: str) -> dict[str, bool]:
    """
    Build a console_levels map for logly so it actually filters logs.
    logly's global level currently doesn't gate console output, so we
    disable lower levels explicitly.
    """
    min_level = _normalized_level(min_level)
    min_index = _LEVELS.index(min_level)
    return {level: idx >= min_index for idx, level in enumerate(_LEVELS)}


def _configure_if_needed() -> _Logger:
    """
    Configure the shared Logly logger once using env overrides and
    return it so callers can bind additional context.
    """
    global _configured
    if _configured:
        return _typed_logger

    level = _normalized_level(os.getenv("SILKWORM_LOG_LEVEL", "INFO"))
    _typed_logger.configure(
        level=level,
        console_levels=_console_levels(level),
        show_time=True,
        show_module=True,
        show_function=False,
        show_filename=False,
        show_lineno=False,
    )
    _configured = True
    return _typed_logger


def get_logger(**context: object) -> _Logger:
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
    _typed_logger.complete()
