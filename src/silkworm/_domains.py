"""Per-domain concurrency slots for the engine's fetch step."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import TYPE_CHECKING
from urllib.parse import urlsplit

from ._validation import require_positive_int

if TYPE_CHECKING:
    from collections.abc import AsyncIterator


def url_host(url: str) -> str:
    """Return the lowercase host of ``url`` (empty when it has none)."""
    try:
        return (urlsplit(url).hostname or "").lower()
    except ValueError:
        return ""


class DomainSlots:
    """Limit how many requests to the same host are fetched at once.

    Hosts are tracked only while a request to them is active or waiting, so
    memory stays proportional to the hosts being crawled right now rather than
    every host ever seen.
    """

    __slots__ = ("_limit", "_slots")

    def __init__(self, limit: int) -> None:
        require_positive_int(limit, "concurrency_per_domain")
        self._limit = limit
        # host -> (semaphore, number of holders and waiters)
        self._slots: dict[str, tuple[asyncio.Semaphore, int]] = {}

    @property
    def limit(self) -> int:
        """Return the maximum concurrent fetches per host."""
        return self._limit

    @asynccontextmanager
    async def acquire(self, url: str) -> AsyncIterator[None]:
        """Hold one of ``url``'s host slots for the duration of the block."""
        host = url_host(url)
        semaphore, users = self._slots.get(host, (None, 0))
        if semaphore is None:
            semaphore = asyncio.Semaphore(self._limit)
        self._slots[host] = (semaphore, users + 1)
        try:
            async with semaphore:
                yield
        finally:
            semaphore, users = self._slots[host]
            if users <= 1:
                del self._slots[host]
            else:
                self._slots[host] = (semaphore, users - 1)

    def active_hosts(self) -> int:
        """Return the number of hosts with an active or waiting request."""
        return len(self._slots)
