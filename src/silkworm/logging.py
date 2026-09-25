"""Structured logging interfaces and configuration helpers for Silkworm.

Log records accept arbitrary keyword context. The shared logger defaults to
stderr and reads ``SILKWORM_LOG_LEVEL`` the first time it is requested.
"""

from __future__ import annotations

import json
import logging as stdlib_logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from types import TracebackType
from typing import Literal, Protocol, TextIO, assert_never, cast, runtime_checkable

from ._resources import raise_cleanup_errors

type _NormalizedLogLevel = Literal[
    "TRACE",
    "DEBUG",
    "INFO",
    "SUCCESS",
    "WARNING",
    "ERROR",
    "CRITICAL",
    "FAIL",
]
type LogLevel = _NormalizedLogLevel | Literal["WARN", "ERR", "FATAL"] | None
type _ExcInfo = (
    bool
    | tuple[type[BaseException], BaseException, TracebackType | None]
    | tuple[None, None, None]
    | BaseException
    | None
)


@runtime_checkable
class Logger(Protocol):
    """Structured logger returned by :func:`get_logger` and ``Spider.log``.

    Keyword arguments become structured context fields on each record.
    """

    def configure(
        self,
        *,
        handlers: list[dict[str, object]] | None = None,
    ) -> None:
        """Replace the logger's handlers with the supplied configurations.

        Each handler mapping accepts ``sink``, ``level``, ``serialize``, and
        ``colorize``. A sink may be ``"stderr"``, ``"stdout"``, a filesystem
        path, or a writable text stream.
        """
        ...

    def bind(self, **context: object) -> Logger:
        """Return a logger carrying ``context`` on every subsequent record."""
        ...

    def info(self, message: str, **context: object) -> None:
        """Emit an informational record with structured context."""
        ...

    def debug(self, message: str, **context: object) -> None:
        """Emit a diagnostic record with structured context."""
        ...

    def warning(self, message: str, **context: object) -> None:
        """Emit a warning record with structured context."""
        ...

    def error(self, message: str, **context: object) -> None:
        """Emit an error record with structured context."""
        ...

    def exception(self, message: str, **context: object) -> None:
        """Emit an error record including the active exception traceback."""
        ...

    def complete(self) -> None:
        """Flush all configured handlers."""
        ...


_LEVELS: tuple[_NormalizedLogLevel, ...] = (
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
_STDLIB_LEVELS = {
    "TRACE": stdlib_logging.DEBUG,
    "DEBUG": stdlib_logging.DEBUG,
    "INFO": stdlib_logging.INFO,
    "SUCCESS": stdlib_logging.INFO,
    "WARNING": stdlib_logging.WARNING,
    "ERROR": stdlib_logging.ERROR,
    "CRITICAL": stdlib_logging.CRITICAL,
    "FAIL": stdlib_logging.CRITICAL,
}


def _normalized_level(raw_level: str) -> _NormalizedLogLevel:
    """
    Normalize user-provided log levels to supported values.
    Unknown levels fall back to INFO.
    """
    level = raw_level.upper()
    level = _ALIASES.get(level, level)
    return level if level in _LEVELS else "INFO"


class _TextFormatter(stdlib_logging.Formatter):
    def __init__(self, *, colorize: bool = False) -> None:
        super().__init__()
        self._colorize = colorize

    def format(self, record: stdlib_logging.LogRecord) -> str:
        level = record.levelname
        if self._colorize:
            color = {
                "DEBUG": "\033[36m",
                "INFO": "\033[32m",
                "WARNING": "\033[33m",
                "ERROR": "\033[31m",
                "CRITICAL": "\033[35m",
            }.get(level, "")
            if color:
                level = f"{color}{level}\033[0m"

        timestamp = self.formatTime(record, "%Y-%m-%d %H:%M:%S")
        message = f"{timestamp} | {level} | {record.getMessage()}"
        context = cast("dict[str, object]", getattr(record, "silkworm_context", {}))
        if context:
            fields = " ".join(f"{key}={value!r}" for key, value in context.items())
            message = f"{message} | {fields}"
        if record.exc_info:
            message = f"{message}\n{self.formatException(record.exc_info)}"
        return message


class _JsonFormatter(stdlib_logging.Formatter):
    def format(self, record: stdlib_logging.LogRecord) -> str:
        context = cast("dict[str, object]", getattr(record, "silkworm_context", {}))
        payload = {
            **context,
            "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str, ensure_ascii=False)


@dataclass(slots=True, frozen=True)
class _LoggerAdapter:
    _logger: stdlib_logging.Logger
    _context: dict[str, object] = field(default_factory=dict[str, object])

    def configure(
        self,
        *,
        handlers: list[dict[str, object]] | None = None,
    ) -> None:
        configurations: list[dict[str, object]] = (
            handlers if handlers is not None else [{"sink": "stderr", "level": "INFO"}]
        )
        configured_handlers: list[stdlib_logging.Handler] = []
        try:
            for configuration in configurations:
                configured_handlers.append(self._create_handler(configuration))
        except BaseException as exc:
            cleanup_errors: list[BaseException] = []
            for handler in configured_handlers:
                try:
                    handler.close()
                except BaseException as cleanup_exc:  # noqa: BLE001
                    cleanup_errors.append(cleanup_exc)
            for cleanup_error in cleanup_errors:
                exc.add_note(f"Logging handler cleanup failed: {cleanup_error}")
            raise

        with _configuration_lock:
            previous_handlers = self._logger.handlers[:]
            self._logger.handlers.clear()
            for handler in configured_handlers:
                self._logger.addHandler(handler)
            self._logger.setLevel(
                min(
                    (handler.level for handler in configured_handlers),
                    default=stdlib_logging.INFO,
                )
            )
            self._logger.propagate = False

        cleanup_errors = []
        for handler in previous_handlers:
            try:
                handler.close()
            except BaseException as exc:  # noqa: BLE001 - close every handler
                cleanup_errors.append(exc)
        raise_cleanup_errors("Logging handler cleanup failed", cleanup_errors)

    def _create_handler(
        self, configuration: dict[str, object]
    ) -> stdlib_logging.Handler:
        sink = configuration.get("sink", "stderr")
        match sink:
            case "stderr":
                handler: stdlib_logging.Handler = stdlib_logging.StreamHandler(
                    sys.stderr
                )
            case "stdout":
                handler = stdlib_logging.StreamHandler(sys.stdout)
            case str() | os.PathLike():
                handler = stdlib_logging.FileHandler(
                    Path(cast("str | os.PathLike[str]", sink)), encoding="utf-8"
                )
            case _ if hasattr(sink, "write"):
                handler = stdlib_logging.StreamHandler(cast("TextIO", sink))
            case _:
                msg = f"Unsupported logging sink: {sink!r}"
                raise TypeError(msg)

        raw_level = str(configuration.get("level", "INFO"))
        level = _STDLIB_LEVELS[_normalized_level(raw_level)]
        handler.setLevel(level)
        if bool(configuration.get("serialize", False)):
            handler.setFormatter(_JsonFormatter())
        else:
            handler.setFormatter(
                _TextFormatter(colorize=bool(configuration.get("colorize", False)))
            )
        return handler

    def bind(self, **context: object) -> _LoggerAdapter:
        return _LoggerAdapter(self._logger, {**self._context, **context})

    def _log(self, level: int, message: str, **context: object) -> None:
        exc_info = cast("_ExcInfo", context.pop("exc_info", None))
        stack_info = cast("bool", context.pop("stack_info", False))
        stacklevel = cast("int", context.pop("stacklevel", 3))
        self._logger.log(
            level,
            message,
            exc_info=exc_info,
            stack_info=stack_info,
            stacklevel=stacklevel,
            extra={"silkworm_context": {**self._context, **context}},
        )

    def info(self, message: str, **context: object) -> None:
        self._log(stdlib_logging.INFO, message, **context)

    def debug(self, message: str, **context: object) -> None:
        self._log(stdlib_logging.DEBUG, message, **context)

    def warning(self, message: str, **context: object) -> None:
        self._log(stdlib_logging.WARNING, message, **context)

    def error(self, message: str, **context: object) -> None:
        self._log(stdlib_logging.ERROR, message, **context)

    def exception(self, message: str, **context: object) -> None:
        """Log at ERROR level with the active exception's traceback."""
        context.setdefault("exc_info", True)
        self._log(stdlib_logging.ERROR, message, **context)

    def complete(self) -> None:
        errors: list[BaseException] = []
        for handler in self._logger.handlers:
            try:
                handler.flush()
            except BaseException as exc:  # noqa: BLE001 - flush every handler
                errors.append(exc)
        raise_cleanup_errors("Logging handler flush failed", errors)


_configuration_lock = RLock()
_stdlib_logger = stdlib_logging.getLogger("silkworm")
# Backwards-compatible alias for the protocol's former private name.
_Logger = Logger

_typed_logger: Logger = _LoggerAdapter(_stdlib_logger)
_configured = False


def _configure_if_needed() -> Logger:
    """
    Configure the shared logger once using environment overrides and return it.
    """
    global _configured
    with _configuration_lock:
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


def get_logger(**context: object) -> Logger:
    """Return the shared structured logger, optionally bound to ``context``.

    The first call configures stderr output using ``SILKWORM_LOG_LEVEL``.
    Bound context is copied into every record without mutating the shared base
    logger.
    """
    base = _configure_if_needed()
    return base.bind(**context) if context else base


def log_at_level(
    logger: Logger,
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
            assert_never(normalized)


def complete_logs() -> None:
    """Flush configured log handlers if logging has been initialized."""
    if not _configured:
        return
    _typed_logger.complete()


__all__ = ["LogLevel", "Logger", "complete_logs", "get_logger", "log_at_level"]
