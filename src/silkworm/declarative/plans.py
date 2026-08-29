from __future__ import annotations

from dataclasses import dataclass
from threading import RLock
from typing import TYPE_CHECKING, get_type_hints
from weakref import WeakKeyDictionary

from .exceptions import DeclarativeConfigurationError
from .fields import MISSING, Field
from .typing import Cardinality, analyze_annotation, matches_annotation

if TYPE_CHECKING:
    from .items import Item


@dataclass(frozen=True, slots=True)
class FieldPlan:
    name: str
    field: Field[object]
    cardinality: Cardinality
    value_type: object
    annotation: object


@dataclass(frozen=True, slots=True)
class ExtractionPlan:
    item_type: type[Item]
    root_selector: str | None
    fields: tuple[FieldPlan, ...]


_PLAN_CACHE: WeakKeyDictionary[type[Item], ExtractionPlan] = WeakKeyDictionary()
_PLAN_LOCK = RLock()


def get_extraction_plan(item_type: type[Item]) -> ExtractionPlan:
    with _PLAN_LOCK:
        cached = _PLAN_CACHE.get(item_type)
        if cached is not None:
            return cached

        plan = _compile_extraction_plan(item_type)
        _PLAN_CACHE[item_type] = plan
        return plan


def _compile_extraction_plan(item_type: type[Item]) -> ExtractionPlan:
    item_name = item_type.__name__
    selector = getattr(item_type, "__selector__", None)
    if selector is not None and (not isinstance(selector, str) or not selector.strip()):
        raise DeclarativeConfigurationError(
            f"{item_name}.__selector__ must be a non-empty string or None"
        )

    try:
        annotations = get_type_hints(item_type, include_extras=True)
    except (NameError, TypeError) as exc:
        raise DeclarativeConfigurationError(
            f"{item_name}: could not resolve annotations: {exc}"
        ) from exc

    declarations = _field_declarations(item_type)
    plans: list[FieldPlan] = []
    for name, field in declarations.items():
        if name not in annotations:
            raise DeclarativeConfigurationError(
                f"{item_name}.{name}: declarative fields require a type annotation"
            )

        annotation = annotations[name]
        cardinality, value_type = analyze_annotation(
            annotation,
            item_name=item_name,
            field_name=name,
        )
        if cardinality is Cardinality.MANY and field.default is not MISSING:
            raise DeclarativeConfigurationError(
                f"{item_name}.{name}: list fields cannot define a scalar default"
            )
        if field.transform is None and not matches_annotation("", value_type):
            raise DeclarativeConfigurationError(
                f"{item_name}.{name}: {field.kind} produces str, but the "
                f"annotation does not accept str; add a transform"
            )
        if field.default is not MISSING and not matches_annotation(
            field.default, annotation
        ):
            raise DeclarativeConfigurationError(
                f"{item_name}.{name}: default value does not match its annotation"
            )

        plans.append(
            FieldPlan(
                name=name,
                field=field,
                cardinality=cardinality,
                value_type=value_type,
                annotation=annotation,
            )
        )

    if not plans:
        raise DeclarativeConfigurationError(
            f"{item_name}: Item must declare at least one Text or Attr field"
        )

    return ExtractionPlan(
        item_type=item_type,
        root_selector=selector,
        fields=tuple(plans),
    )


def _field_declarations(item_type: type[Item]) -> dict[str, Field[object]]:
    declarations: dict[str, Field[object]] = {}
    for base in reversed(item_type.__mro__):
        for name, value in vars(base).items():
            if isinstance(value, Field):
                declarations[name] = value
            elif name in declarations:
                declarations.pop(name)
    return declarations


__all__ = ["ExtractionPlan", "FieldPlan"]
