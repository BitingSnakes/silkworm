from __future__ import annotations

import types
from enum import Enum, auto
from typing import Any, get_args, get_origin

from .exceptions import DeclarativeConfigurationError


class Cardinality(Enum):
    ONE = auto()
    OPTIONAL = auto()
    MANY = auto()


def analyze_annotation(
    annotation: object,
    *,
    item_name: str,
    field_name: str,
) -> tuple[Cardinality, object]:
    origin = get_origin(annotation)
    if origin is list:
        arguments = get_args(annotation)
        if len(arguments) != 1:
            raise DeclarativeConfigurationError(
                f"{item_name}.{field_name}: list fields must declare an item type"
            )
        return Cardinality.MANY, arguments[0]

    arguments = get_args(annotation)
    if origin is types.UnionType:
        non_none = tuple(
            argument for argument in arguments if argument is not type(None)
        )
        if len(non_none) == 1 and len(non_none) != len(arguments):
            if get_origin(non_none[0]) is list:
                raise DeclarativeConfigurationError(
                    f"{item_name}.{field_name}: optional list fields are ambiguous; "
                    "use list[T], which yields an empty list when there are no matches"
                )
            return Cardinality.OPTIONAL, non_none[0]

    return Cardinality.ONE, annotation


def allows_none(annotation: object) -> bool:
    if annotation in (Any, object, None, type(None)):
        return True
    return type(None) in get_args(annotation)


def matches_annotation(value: object, annotation: object) -> bool:
    if annotation in (Any, object):
        return True
    if annotation in (None, type(None)):
        return value is None

    origin = get_origin(annotation)
    arguments = get_args(annotation)
    if origin is types.UnionType:
        return any(matches_annotation(value, argument) for argument in arguments)
    if origin is list:
        return isinstance(value, list) and all(
            matches_annotation(element, arguments[0]) for element in value
        )

    if origin is not None:
        try:
            return isinstance(value, origin)
        except TypeError:
            return True

    try:
        return isinstance(value, annotation)  # type: ignore[arg-type]
    except TypeError:
        # Some typing constructs cannot participate in isinstance(). They remain
        # useful documentation, but runtime validation cannot safely inspect them.
        return True


def annotation_label(annotation: object) -> str:
    return getattr(annotation, "__name__", str(annotation).replace("typing.", ""))


__all__ = ["Cardinality"]
