"""Coverage checks for the documented public API."""

from __future__ import annotations

import importlib
import inspect
from collections.abc import Iterator
from types import ModuleType

import pytest

PUBLIC_MODULE_NAMES = (
    "silkworm",
    "silkworm.declarative",
    "silkworm.logging",
    "silkworm.markdown",
    "silkworm.middlewares",
    "silkworm.pipelines",
    "silkworm.request",
    "silkworm.runner",
    "silkworm.servo",
    "silkworm.types",
)


def _public_modules() -> Iterator[ModuleType]:
    for module_name in PUBLIC_MODULE_NAMES:
        yield importlib.import_module(module_name)


def _exported_framework_objects() -> Iterator[tuple[str, object]]:
    seen: set[int] = set()
    for module in _public_modules():
        for name in module.__all__:
            exported = getattr(module, name)
            if id(exported) in seen:
                continue
            seen.add(id(exported))
            if not (inspect.isclass(exported) or inspect.isfunction(exported)):
                continue
            if not getattr(exported, "__module__", "").startswith("silkworm"):
                continue
            yield f"{module.__name__}.{name}", exported


def _public_members(cls: type[object]) -> Iterator[tuple[str, object]]:
    for name, member in vars(cls).items():
        if name.startswith("_"):
            continue
        if isinstance(member, property):
            if member.fget is not None:
                yield name, member.fget
            continue
        if isinstance(member, (classmethod, staticmethod)):
            member = member.__func__
        if inspect.isroutine(member):
            yield name, member


@pytest.mark.parametrize(
    "module", list(_public_modules()), ids=lambda mod: mod.__name__
)
def test_public_modules_have_docstrings(module: ModuleType) -> None:
    """Every public facade module explains the API group it exposes."""
    assert inspect.getdoc(module), f"{module.__name__} has no module docstring"


def test_exported_objects_and_members_have_docstrings() -> None:
    """Every exported framework callable has discoverable reference help."""
    missing: list[str] = []
    for qualified_name, exported in _exported_framework_objects():
        if not inspect.getdoc(exported):
            missing.append(qualified_name)
        if inspect.isclass(exported):
            missing.extend(
                f"{qualified_name}.{name}"
                for name, member in _public_members(exported)
                if not inspect.getdoc(member)
            )

    assert not missing, "Missing public docstrings:\n" + "\n".join(sorted(missing))
