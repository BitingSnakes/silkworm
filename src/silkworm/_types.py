from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from typing import TypeAlias

JSONScalar: TypeAlias = str | int | float | bool | None
JSONValue: TypeAlias = JSONScalar | dict[str, "JSONValue"] | list["JSONValue"]
# Read-only counterpart of JSONValue. ``dict`` and ``list`` are invariant, so a
# ``dict[str, str]`` is not a JSONValue; it is a JSONLike.
JSONLike: TypeAlias = JSONScalar | Mapping[str, "JSONLike"] | Sequence["JSONLike"]

Headers: TypeAlias = dict[str, str]
QueryValue: TypeAlias = (
    str | int | float | bool | None | Iterable[str | int | float | bool | None]
)
QueryParams: TypeAlias = dict[str, QueryValue]
MetaData: TypeAlias = dict[str, JSONValue]
BodyData: TypeAlias = (
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
