"""On-disk HTTP response cache for fast, polite development re-runs.

Wrap the engine's HTTP client with an :class:`HttpCache` so repeated runs of a
spider (while writing selectors, for example) read pages from disk instead of
downloading them again::

    run_spider(MySpider, http_cache=HttpCache(".silkworm/httpcache"))

Entries are keyed by :func:`~silkworm.request_fingerprint`, so the same method,
canonical URL, and body share one entry regardless of headers or metadata.
Cached responses carry an ``x-silkworm-cache: hit`` header.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from pathlib import Path
from typing import TYPE_CHECKING, Self, cast

from ._timeouts import to_seconds
from ._urls import request_fingerprint
from .http import build_response
from .logging import Logger, get_logger

if TYPE_CHECKING:
    from collections.abc import Iterable
    from datetime import timedelta

    from .http import FetchClient
    from .request import Request
    from .response import Response

CACHE_HEADER = "x-silkworm-cache"

# Request ``meta`` key that bypasses the cache (neither read nor written).
DONT_CACHE_META_KEY = "dont_cache"


class HttpCache:
    """Configuration and storage for cached HTTP responses.

    Args:
        directory: Cache directory; created when missing.
        expiration: Maximum entry age (seconds or ``timedelta``); ``None``
            keeps entries forever.
        ignore_statuses: Response statuses that are never stored (e.g. 5xx).
        methods: HTTP methods eligible for caching.

    Set ``request.meta["dont_cache"] = True`` to bypass the cache for one
    request.
    """

    def __init__(
        self,
        directory: str | os.PathLike[str],
        *,
        expiration: float | timedelta | None = None,
        ignore_statuses: Iterable[int] = (500, 502, 503, 504, 522, 524, 408, 429),
        methods: Iterable[str] = ("GET", "HEAD"),
    ) -> None:
        expiration_seconds = to_seconds(expiration)
        if expiration_seconds is not None and expiration_seconds <= 0:
            msg = "expiration must be positive"
            raise ValueError(msg)
        self.directory: Path = Path(directory)
        self.expiration_seconds: float | None = expiration_seconds
        self.ignore_statuses: frozenset[int] = frozenset(ignore_statuses)
        self.methods: frozenset[str] = frozenset(method.upper() for method in methods)
        self.hits: int = 0
        self.misses: int = 0
        self.stored: int = 0

    def wrap(self, client: FetchClient) -> CachingHttpClient:
        """Return a client that serves ``client``'s responses from this cache."""
        return CachingHttpClient(client, self)

    def is_cacheable(self, request: Request) -> bool:
        """Return whether ``request`` may be read from or written to the cache."""
        return request.method.upper() in self.methods and not request.meta.get(
            DONT_CACHE_META_KEY
        )

    def _paths(self, key: str) -> tuple[Path, Path]:
        folder = self.directory / key[:2]
        return folder / f"{key}.json", folder / f"{key}.body"

    def load(self, request: Request) -> tuple[dict[str, object], bytes] | None:
        """Return ``(metadata, body)`` for a fresh entry, else ``None``."""
        meta_path, body_path = self._paths(request_fingerprint(request))
        try:
            metadata = cast("dict[str, object]", json.loads(meta_path.read_text()))
            body = body_path.read_bytes()
        except (OSError, ValueError):
            return None
        stored_at = metadata.get("stored_at")
        if self.expiration_seconds is not None and (
            not isinstance(stored_at, (int, float))
            or time.time() - stored_at > self.expiration_seconds
        ):
            return None
        return metadata, body

    def store(self, request: Request, response: Response) -> bool:
        """Write ``response`` for ``request``; return whether it was stored."""
        if response.status in self.ignore_statuses:
            return False
        meta_path, body_path = self._paths(request_fingerprint(request))
        meta_path.parent.mkdir(parents=True, exist_ok=True)
        metadata = {
            "url": response.url,
            "status": response.status,
            "headers": dict(response.headers),
            "request_url": request.url,
            "method": request.method.upper(),
            "stored_at": time.time(),
        }
        # Write the body first and the metadata last, each atomically, so a
        # reader never sees metadata pointing at a partial body.
        _atomic_write(body_path, response.body)
        _atomic_write(meta_path, json.dumps(metadata).encode())
        return True


def _atomic_write(path: Path, data: bytes) -> None:
    tmp = path.with_name(f"{path.name}.{os.getpid()}.tmp")
    tmp.write_bytes(data)
    tmp.replace(path)


class CachingHttpClient:
    """HTTP client wrapper that reads and writes an :class:`HttpCache`.

    Created by :meth:`HttpCache.wrap`; exposes the same interface as the
    wrapped client so the engine can use it transparently.
    """

    def __init__(self, client: FetchClient, cache: HttpCache) -> None:
        self.client = client
        self.cache = cache
        self.logger: Logger = get_logger(component="httpcache")

    @property
    def concurrency(self) -> int:
        """Return the wrapped client's concurrency."""
        return self.client.concurrency

    @property
    def html_max_size_bytes(self) -> int:
        """Return the wrapped client's HTML parsing limit."""
        return self.client.html_max_size_bytes

    async def __aenter__(self) -> Self:
        """Return this client for use in an async context."""
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        """Close the wrapped client on context exit."""
        await self.close()

    async def fetch(self, req: Request) -> Response:
        """Return a cached response for ``req`` or fetch and store it."""
        if not self.cache.is_cacheable(req):
            return await self.client.fetch(req)

        entry = await asyncio.to_thread(self.cache.load, req)
        if entry is not None:
            metadata, body = entry
            self.cache.hits += 1
            headers = cast("dict[str, str]", metadata.get("headers") or {})
            headers = {**headers, CACHE_HEADER: "hit"}
            self.logger.debug("HTTP cache hit", url=req.url)
            return build_response(
                url=str(metadata.get("url", req.url)),
                status=int(cast("int", metadata.get("status", 200))),
                headers=headers,
                body=body,
                request=req,
                html_max_size_bytes=self.client.html_max_size_bytes,
            )

        self.cache.misses += 1
        response = await self.client.fetch(req)
        if await asyncio.to_thread(self.cache.store, req, response):
            self.cache.stored += 1
        return response

    async def close(self) -> None:
        """Close the wrapped client and log cache effectiveness."""
        self.logger.info(
            "HTTP cache statistics",
            hits=self.cache.hits,
            misses=self.cache.misses,
            stored=self.cache.stored,
            directory=str(self.cache.directory),
        )
        await self.client.close()
