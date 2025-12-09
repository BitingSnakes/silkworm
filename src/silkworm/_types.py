from __future__ import annotations

from collections.abc import Iterable, Mapping

JSONScalar = str | int | float | bool | None
JSONValue = JSONScalar | dict[str, "JSONValue"] | list["JSONValue"]

Headers = dict[str, str]
QueryValue = str | int | float | bool | None | Iterable[str | int | float | bool | None]
QueryParams = dict[str, QueryValue]
MetaData = dict[str, JSONValue]
BodyData = (
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
    "JSONScalar",
    "JSONValue",
    "MetaData",
    "QueryParams",
    "QueryValue",
]
