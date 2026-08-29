from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Mapping
from typing import TYPE_CHECKING, ClassVar, Protocol, Self, dataclass_transform

from .._types import JSONValue
from .exceptions import (
    DeclarativeConfigurationError,
    DeclarativeSerializationError,
    FieldExtractionError,
    FieldTransformError,
    MissingFieldError,
)
from .fields import MISSING, Attr, Field, Text, _AttrField, _TextField
from .plans import ExtractionPlan, FieldPlan, get_extraction_plan
from .typing import Cardinality, allows_none, annotation_label, matches_annotation

if TYPE_CHECKING:
    from collections.abc import Callable

    from ..response import HTMLResponse


class _SelectorSource(Protocol):
    async def select(self, selector: str) -> list[object]: ...

    async def select_first(self, selector: str) -> object | None: ...


@dataclass_transform(field_specifiers=(Field, Text, Attr))
class _ItemMeta(type):
    pass


class Item(metaclass=_ItemMeta):
    """Base class for a compiled declarative extraction item."""

    __selector__: ClassVar[str | None] = None
    __slots__ = ("_values",)

    def __init__(self, **values: object) -> None:
        plan = self.extraction_plan()
        expected = {field.name for field in plan.fields}
        received = set(values)
        if missing := expected - received:
            names = ", ".join(sorted(missing))
            raise TypeError(f"missing values for {type(self).__name__}: {names}")
        if unexpected := received - expected:
            names = ", ".join(sorted(unexpected))
            raise TypeError(f"unexpected values for {type(self).__name__}: {names}")
        self._values = dict(values)

    @classmethod
    def extraction_plan(cls) -> ExtractionPlan:
        """Return the cached extraction plan for this item class."""
        return get_extraction_plan(cls)

    @classmethod
    async def extract(cls, response: HTMLResponse) -> AsyncIterator[Self]:
        """Extract every matching item from an HTML response."""
        plan = cls.extraction_plan()
        source = response  # HTMLResponse satisfies the internal selector protocol.
        if plan.root_selector is None:
            roots: list[object] = [source]
        else:
            roots = await source.select(plan.root_selector)

        for root_index, root in enumerate(roots):
            selector_source = _as_selector_source(
                root,
                item_name=cls.__name__,
                url=response.url,
            )
            values: dict[str, object] = {}
            for field_plan in plan.fields:
                values[field_plan.name] = await _extract_field(
                    field_plan,
                    source=selector_source,
                    response=response,
                    item_name=cls.__name__,
                    root_index=root_index,
                )

            item = cls(**values)
            hook_result = item.after_extract(response)
            if inspect.isawaitable(hook_result):
                await hook_result
            else:
                raise DeclarativeConfigurationError(
                    f"{cls.__name__}.after_extract() must be async"
                )
            yield item

    async def after_extract(self, response: HTMLResponse) -> None:
        """Hook invoked after all fields have been extracted."""

    def to_dict(self) -> dict[str, JSONValue]:
        """Recursively convert this item into a pipeline-compatible mapping."""
        converted = _to_json_value(self, path=type(self).__name__)
        if not isinstance(converted, dict):
            raise AssertionError("Item serialization must produce a mapping")
        return converted

    def __repr__(self) -> str:
        fields = ", ".join(f"{name}={value!r}" for name, value in self._values.items())
        return f"{type(self).__name__}({fields})"

    def __eq__(self, other: object) -> bool:
        return type(self) is type(other) and self._values == other._values  # type: ignore[attr-defined]


def _as_selector_source(
    value: object,
    *,
    item_name: str,
    url: str,
) -> _SelectorSource:
    if callable(getattr(value, "select", None)) and callable(
        getattr(value, "select_first", None)
    ):
        return value  # type: ignore[return-value]
    raise FieldExtractionError(
        f"{item_name}: root selected from {url} does not support nested CSS selectors"
    )


async def _extract_field(
    plan: FieldPlan,
    *,
    source: _SelectorSource,
    response: HTMLResponse,
    item_name: str,
    root_index: int,
) -> object:
    try:
        match plan.cardinality:
            case Cardinality.MANY:
                elements = await source.select(plan.field.selector)
                return [
                    await _extract_element_value(
                        plan,
                        element,
                        response=response,
                        item_name=item_name,
                        root_index=root_index,
                        match_index=match_index,
                    )
                    for match_index, element in enumerate(elements)
                ]
            case Cardinality.ONE | Cardinality.OPTIONAL:
                element = await source.select_first(plan.field.selector)
                if element is None:
                    return _missing_value(
                        plan,
                        item_name=item_name,
                        url=response.url,
                        root_index=root_index,
                    )
                return await _extract_element_value(
                    plan,
                    element,
                    response=response,
                    item_name=item_name,
                    root_index=root_index,
                    match_index=None,
                )
    except (MissingFieldError, FieldTransformError):
        raise
    except Exception as exc:
        raise FieldExtractionError(
            _field_context(plan, item_name, response.url, root_index)
            + f": selector evaluation failed: {exc}"
        ) from exc


async def _extract_element_value(
    plan: FieldPlan,
    element: object,
    *,
    response: HTMLResponse,
    item_name: str,
    root_index: int,
    match_index: int | None,
) -> object:
    field = plan.field
    match field:
        case _TextField():
            value = getattr(element, "text", None)
            if value is not None and field.strip:
                value = value.strip()
        case _AttrField():
            attr = getattr(element, "attr", None)
            if not callable(attr):
                raise FieldExtractionError(
                    _field_context(plan, item_name, response.url, root_index)
                    + ": selected element does not expose attr()"
                )
            value = attr(field.attribute)
            if value is not None and field.absolute:
                value = response.url_join(value)
        case _:
            raise DeclarativeConfigurationError(
                f"{item_name}.{plan.name}: unsupported field type {type(field).__name__}"
            )

    if value is None:
        if plan.cardinality is Cardinality.MANY and allows_none(plan.value_type):
            return None
        detail = "selected element has no text"
        if isinstance(field, _AttrField):
            detail = f"attribute {field.attribute!r} is missing"
        return _missing_value(
            plan,
            item_name=item_name,
            url=response.url,
            root_index=root_index,
            match_index=match_index,
            detail=detail,
        )

    transform: Callable[[str], object] | None = field.transform
    if transform is not None:
        try:
            value = transform(value)
            if inspect.isawaitable(value):
                if inspect.iscoroutine(value):
                    value.close()
                raise TypeError(
                    "async transforms are not supported in declarative v0.1"
                )
        except Exception as exc:
            raise FieldTransformError(
                _field_context(plan, item_name, response.url, root_index)
                + f": transform failed: {exc}"
            ) from exc

    if not matches_annotation(value, plan.value_type):
        raise FieldTransformError(
            _field_context(plan, item_name, response.url, root_index)
            + f": extracted {type(value).__name__}, expected "
            + annotation_label(plan.value_type)
        )
    return value


def _missing_value(
    plan: FieldPlan,
    *,
    item_name: str,
    url: str,
    root_index: int,
    match_index: int | None = None,
    detail: str = "matched no element",
) -> object:
    if plan.field.default is not MISSING:
        return plan.field.default
    if plan.cardinality is Cardinality.OPTIONAL:
        return None
    suffix = ""
    if match_index is not None:
        suffix = f", match {match_index}"
    raise MissingFieldError(
        _field_context(plan, item_name, url, root_index)
        + f": required value is missing ({detail}{suffix})"
    )


def _field_context(
    plan: FieldPlan,
    item_name: str,
    url: str,
    root_index: int,
) -> str:
    return (
        f"{item_name}.{plan.name}: selector {plan.field.selector!r} "
        f"at {url} (root {root_index})"
    )


def _to_json_value(value: object, *, path: str) -> JSONValue:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Item):
        return {
            name: _to_json_value(field_value, path=f"{path}.{name}")
            for name, field_value in value._values.items()
        }
    if isinstance(value, Mapping):
        result: dict[str, JSONValue] = {}
        for key, child in value.items():
            if not isinstance(key, str):
                raise DeclarativeSerializationError(
                    f"{path}: mapping keys must be strings, got {type(key).__name__}"
                )
            result[key] = _to_json_value(child, path=f"{path}.{key}")
        return result
    if isinstance(value, (list, tuple)):
        return [
            _to_json_value(child, path=f"{path}[{index}]")
            for index, child in enumerate(value)
        ]
    raise DeclarativeSerializationError(
        f"{path}: {type(value).__name__} is not JSON-serializable"
    )


__all__ = ["Item"]
