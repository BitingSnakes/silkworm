"""Layered engine settings for runners and the command line.

Engine options resolve from four layers, lowest precedence first:

1. :class:`~silkworm.Engine` defaults.
2. Environment variables named ``SILKWORM_<OPTION>`` (for example
   ``SILKWORM_CONCURRENCY=32`` or ``SILKWORM_MAX_DEPTH=3``).
3. The spider's :attr:`~silkworm.Spider.custom_settings` entries whose keys
   name engine options.
4. Options passed explicitly to a runner or on the command line.

Only scalar options (numbers, booleans, strings, and paths) can come from the
environment or ``custom_settings``; middlewares, pipelines, and clients are
always passed in code. Values are validated and converted, so a typo such as
``SILKWORM_MAX_DEPTH=three`` fails with a clear error instead of being ignored.
:class:`~silkworm.Engine` itself never reads these layers; use
:func:`resolve_options` (as the runners do) to apply them.
"""

from __future__ import annotations

import os
from collections.abc import Callable, Mapping
from typing import TYPE_CHECKING, cast

from .httpcache import HttpCache

if TYPE_CHECKING:
    from .engine import EngineOptions
    from .spiders import Spider

ENV_PREFIX = "SILKWORM_"

_TRUE = frozenset({"1", "true", "yes", "on"})
_FALSE = frozenset({"0", "false", "no", "off"})
_NONE = frozenset({"", "none", "null"})


def _as_int(value: object) -> int:
    if isinstance(value, bool):
        raise TypeError("expected an integer, got a boolean")
    if isinstance(value, int):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, str):
        return int(value.strip())
    raise TypeError(f"expected an integer, got {type(value).__name__}")


def _as_float(value: object) -> float:
    if isinstance(value, bool):
        raise TypeError("expected a number, got a boolean")
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        return float(value.strip())
    raise TypeError(f"expected a number, got {type(value).__name__}")


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str) and value.strip().lower() in _TRUE | _FALSE:
        return value.strip().lower() in _TRUE
    raise TypeError("expected a boolean (true/false, 1/0, yes/no, on/off)")


def _as_str(value: object) -> str:
    if isinstance(value, str) and value.strip():
        return value.strip()
    raise TypeError("expected a non-empty string")


def _as_http_cache(value: object) -> HttpCache:
    if isinstance(value, HttpCache):
        return value
    return HttpCache(_as_str(value))


def _optional(convert: Callable[[object], object]) -> Callable[[object], object]:
    def convert_optional(value: object) -> object:
        if value is None or (isinstance(value, str) and value.strip().lower() in _NONE):
            return None
        return convert(value)

    return convert_optional


# Engine options that may be configured from the environment or custom_settings.
SETTING_CONVERTERS: dict[str, Callable[[object], object]] = {
    "concurrency": _as_int,
    "max_pending_requests": _optional(_as_int),
    "request_timeout": _optional(_as_float),
    "html_max_size_bytes": _as_int,
    "max_response_size_bytes": _optional(_as_int),
    "item_batch_size": _as_int,
    "item_batch_wait": _as_float,
    "log_stats_interval": _optional(_as_float),
    "keep_alive": _as_bool,
    "concurrency_per_domain": _optional(_as_int),
    "max_depth": _optional(_as_int),
    "max_requests": _optional(_as_int),
    "max_items": _optional(_as_int),
    "max_errors": _optional(_as_int),
    "max_duration": _optional(_as_float),
    "max_error_rate": _optional(_as_float),
    "min_items": _optional(_as_int),
    "max_item_drop_rate": _optional(_as_float),
    "job_dir": _optional(_as_str),
    "http_cache": _optional(_as_http_cache),
    "metrics_port": _optional(_as_int),
    "metrics_host": _as_str,
}


def coerce_setting(name: str, value: object, *, source: str = "setting") -> object:
    """Validate and convert ``value`` for engine option ``name``.

    Args:
        name: Engine option name, e.g. ``"max_depth"``.
        value: Raw value (a string from the environment or command line, or a
            JSON value from ``custom_settings``).
        source: Where the value came from, used in error messages.

    Raises:
        KeyError: If ``name`` is not a configurable engine option.
        ValueError: If the value cannot be converted.
    """
    try:
        convert = SETTING_CONVERTERS[name]
    except KeyError:
        known = ", ".join(sorted(SETTING_CONVERTERS))
        msg = f"Unknown engine setting {name!r} from {source}; known settings: {known}"
        raise KeyError(msg) from None
    try:
        return convert(value)
    except (TypeError, ValueError) as exc:
        msg = (
            f"Invalid value {value!r} for engine setting {name!r} from {source}: {exc}"
        )
        raise ValueError(msg) from exc


def settings_from_env(environ: Mapping[str, str] | None = None) -> dict[str, object]:
    """Return engine options configured through ``SILKWORM_<OPTION>`` variables."""
    environ = os.environ if environ is None else environ
    options: dict[str, object] = {}
    for name in SETTING_CONVERTERS:
        variable = f"{ENV_PREFIX}{name.upper()}"
        if variable in environ:
            options[name] = coerce_setting(
                name, environ[variable], source=f"environment variable {variable}"
            )
    return options


def settings_from_spider(spider: Spider) -> dict[str, object]:
    """Return engine options configured in ``spider.custom_settings``."""
    return {
        name: coerce_setting(
            name, value, source=f"{type(spider).__name__}.custom_settings"
        )
        for name, value in spider.custom_settings.items()
        if name in SETTING_CONVERTERS
    }


def resolve_options(
    spider: Spider,
    options: EngineOptions | Mapping[str, object],
    *,
    environ: Mapping[str, str] | None = None,
) -> EngineOptions:
    """Merge the settings layers into the options passed to :class:`Engine`.

    Args:
        spider: Spider whose ``custom_settings`` form the third layer.
        options: Explicit options; these always win.
        environ: Environment mapping; defaults to :data:`os.environ`.
    """
    merged: dict[str, object] = {
        **settings_from_env(environ),
        **settings_from_spider(spider),
        **options,
    }
    return cast("EngineOptions", merged)


__all__ = [
    "ENV_PREFIX",
    "SETTING_CONVERTERS",
    "coerce_setting",
    "resolve_options",
    "settings_from_env",
    "settings_from_spider",
]
