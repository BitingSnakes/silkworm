"""Internal helpers for deterministic synchronous and asynchronous cleanup."""

from __future__ import annotations

import inspect
from collections.abc import Iterable


async def close_resource(resource: object | None) -> None:
    """Call an available ``aclose`` or ``close`` hook and await its result."""
    if resource is None:
        return
    closer = getattr(resource, "aclose", None) or getattr(resource, "close", None)
    if closer is None or not callable(closer):
        return
    result = closer()
    if inspect.isawaitable(result):
        await result


def raise_cleanup_errors(
    message: str,
    errors: Iterable[BaseException],
) -> None:
    """Raise one cleanup error directly or aggregate several failures."""
    collected = list(errors)
    if not collected:
        return
    if len(collected) == 1:
        raise collected[0]
    raise BaseExceptionGroup(message, collected)
