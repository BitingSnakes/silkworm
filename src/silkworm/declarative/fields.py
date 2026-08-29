from __future__ import annotations

from collections.abc import Callable
from typing import Any, Protocol, Self, overload


class _Missing:
    __slots__ = ()

    def __repr__(self) -> str:
        return "MISSING"


MISSING = _Missing()


class _ItemInstance(Protocol):
    _values: dict[str, object]


class Field[T = str]:
    """Base descriptor for a field in a declarative item."""

    __slots__ = ("_name", "default", "selector", "transform")

    def __init__(
        self,
        selector: str,
        *,
        transform: Callable[[str], object] | None = None,
        default: object | _Missing = MISSING,
    ) -> None:
        if not isinstance(selector, str) or not selector.strip():
            raise ValueError("selector must be a non-empty string")
        if transform is not None and not callable(transform):
            raise TypeError("transform must be callable")

        self.selector = selector
        self.transform = transform
        self.default = default
        self._name: str | None = None

    @property
    def name(self) -> str:
        if self._name is None:
            raise RuntimeError("field is not bound to an Item class")
        return self._name

    @property
    def kind(self) -> str:
        return type(self).__name__.removeprefix("_").removesuffix("Field")

    def __set_name__(self, owner: type[object], name: str) -> None:
        if self._name is not None and self._name != name:
            raise TypeError(
                f"field already bound as {self._name!r}; cannot bind it as {name!r}"
            )
        self._name = name

    @overload
    def __get__(self, instance: None, owner: type[object] | None = None) -> Self: ...

    @overload
    def __get__(
        self,
        instance: _ItemInstance,
        owner: type[object] | None = None,
    ) -> T: ...

    def __get__(
        self,
        instance: _ItemInstance | None,
        owner: type[object] | None = None,
    ) -> Self | T:
        if instance is None:
            return self
        try:
            return instance._values[self.name]  # type: ignore[return-value]
        except KeyError as exc:
            raise AttributeError(self.name) from exc

    def __set__(self, instance: _ItemInstance, value: T) -> None:
        instance._values[self.name] = value


class _TextField[T = str](Field[T]):
    __slots__ = ("strip",)

    def __init__(
        self,
        selector: str,
        *,
        transform: Callable[[str], object] | None = None,
        default: object | _Missing = MISSING,
        strip: bool = False,
    ) -> None:
        super().__init__(selector, transform=transform, default=default)
        self.strip = strip


class _AttrField[T = str](Field[T]):
    __slots__ = ("absolute", "attribute")

    def __init__(
        self,
        selector: str,
        name: str,
        *,
        absolute: bool = False,
        transform: Callable[[str], object] | None = None,
        default: object | _Missing = MISSING,
    ) -> None:
        if not isinstance(name, str) or not name.strip():
            raise ValueError("attribute name must be a non-empty string")
        super().__init__(selector, transform=transform, default=default)
        self.attribute = name
        self.absolute = absolute


@overload
def Text[T](
    selector: str,
    *,
    transform: Callable[[str], T],
    default: T | None | _Missing = MISSING,
    strip: bool = False,
) -> Any: ...


@overload
def Text(
    selector: str,
    *,
    transform: None = None,
    default: str | None | _Missing = MISSING,
    strip: bool = False,
) -> Any: ...


def Text(
    selector: str,
    *,
    transform: Callable[[str], object] | None = None,
    default: object | _Missing = MISSING,
    strip: bool = False,
) -> Any:
    """Declare a field that extracts text with a CSS selector."""
    return _TextField(
        selector,
        transform=transform,
        default=default,
        strip=strip,
    )


@overload
def Attr[T](
    selector: str,
    name: str,
    *,
    absolute: bool = False,
    transform: Callable[[str], T],
    default: T | None | _Missing = MISSING,
) -> Any: ...


@overload
def Attr(
    selector: str,
    name: str,
    *,
    absolute: bool = False,
    transform: None = None,
    default: str | None | _Missing = MISSING,
) -> Any: ...


def Attr(
    selector: str,
    name: str,
    *,
    absolute: bool = False,
    transform: Callable[[str], object] | None = None,
    default: object | _Missing = MISSING,
) -> Any:
    """Declare a field that extracts an HTML attribute with a CSS selector."""
    return _AttrField(
        selector,
        name,
        absolute=absolute,
        transform=transform,
        default=default,
    )


__all__ = ["Attr", "Field", "Text"]
