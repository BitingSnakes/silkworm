from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence

type JSONScalar = str | int | float | bool | None
type JSONValue = JSONScalar | dict[str, JSONValue] | list[JSONValue]
# Read-only counterpart of JSONValue. ``dict`` and ``list`` are invariant, so a
# ``dict[str, str]`` is not a JSONValue; it is a JSONLike.
type JSONLike = JSONScalar | Mapping[str, JSONLike] | Sequence[JSONLike]

type Headers = dict[str, str]
type QueryValue = (
    str | int | float | bool | None | Iterable[str | int | float | bool | None]
)
type QueryParams = dict[str, QueryValue]
type MetaData = dict[str, JSONValue]
type BodyData = (
    bytes
    | bytearray
    | memoryview
    | str
    | Mapping[str, JSONValue]
    | Iterable[tuple[str, str]]
    | list[JSONValue]
    | None
)

__all__ = [
    "BodyData",
    "Headers",
    "JSONLike",
    "JSONScalar",
    "JSONValue",
    "MetaData",
    "QueryParams",
    "QueryValue",
]
