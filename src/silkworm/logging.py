from __future__ import annotations

import os
from typing import Literal, Protocol, cast, runtime_checkable

from logly import logger as _logger  # type: ignore[import]

_typed_logger = cast("_Logger", _logger)

type LogLevel = (
    Literal[
        "TRACE",
        "DEBUG",
        "INFO",
        "SUCCESS",
        "WARNING",
        "ERROR",
        "CRITICAL",
        "FAIL",
        "WARN",
        "ERR",
        "FATAL",
    ]
    | None
)


@runtime_checkable
class _Logger(Protocol):
    def configure(
        self,
        *,
        handlers: list[dict[str, object]] | None = None,
    ) -> None: ...

    def bind(self, **context: object) -> _Logger: ...

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
        handlers=[
            {
                "sink": "stderr",
                "level": level,
            }
        ],
    )
    _configured = True
    return _typed_logger


def get_logger(**context: object) -> _Logger:
    """
    Grab the shared Logly logger with optional bound context fields.
    """
    base = _configure_if_needed()
    return base.bind(**context) if context else base


def log_at_level(
    logger: _Logger,
    level: LogLevel,
    message: str,
    **context: object,
) -> None:
    """
    Emit a log message at a caller-selected level.

    Passing ``None`` intentionally suppresses the message. That escape hatch is
    useful for very noisy per-item paths, but should be used sparingly.
    """
    if level is None:
        return

    normalized = _normalized_level(level)
    match normalized:
        case "TRACE" | "DEBUG":
            logger.debug(message, **context)
        case "INFO" | "SUCCESS":
            logger.info(message, **context)
        case "WARNING":
            logger.warning(message, **context)
        case "ERROR" | "CRITICAL" | "FAIL":
            logger.error(message, **context)
        case _:
            logger.info(message, **context)


def complete_logs() -> None:
    """
    Flush buffered log messages if the logger has been configured.
    """
    if not _configured:
        return
    _typed_logger.complete()
