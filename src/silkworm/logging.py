from __future__ import annotations

import json
import logging as stdlib_logging
import os
import sys
from dataclasses import dataclass, field
from pathlib import Path
from threading import RLock
from types import TracebackType
from typing import Literal, Protocol, assert_never, cast, runtime_checkable

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
    return cast("_NormalizedLogLevel", level) if level in _LEVELS else "INFO"


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
    _context: dict[str, object] = field(default_factory=dict)

    def configure(
        self,
        *,
        handlers: list[dict[str, object]] | None = None,
    ) -> None:
        configurations = (
            handlers if handlers is not None else [{"sink": "stderr", "level": "INFO"}]
        )
        configured_handlers = [
            self._create_handler(configuration) for configuration in configurations
        ]

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

        for handler in previous_handlers:
            handler.close()

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
                handler = stdlib_logging.FileHandler(Path(sink), encoding="utf-8")
            case _ if hasattr(sink, "write"):
                handler = stdlib_logging.StreamHandler(sink)
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

    def complete(self) -> None:
        for handler in self._logger.handlers:
            handler.flush()


_configuration_lock = RLock()
_stdlib_logger = stdlib_logging.getLogger("silkworm")
_typed_logger: _Logger = _LoggerAdapter(_stdlib_logger)
_configured = False


def _configure_if_needed() -> _Logger:
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


def get_logger(**context: object) -> _Logger:
    """
    Grab the shared logger with optional bound context fields.
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
            assert_never(normalized)


def complete_logs() -> None:
    """
    Flush configured log handlers.
    """
    if not _configured:
        return
    _typed_logger.complete()
