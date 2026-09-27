"""Helpers for testing spider callbacks offline, without an engine or network.

Build responses from strings or saved files, run a callback exactly as the
engine would (same ``emit``/``follow`` scope and callback contract checks), and
assert on what it produced::

    from silkworm.testing import response_from_file, run_callback

    async def test_parse_extracts_quotes():
        response = response_from_file(
            "tests/fixtures/quotes.html", url="https://quotes.toscrape.com/"
        )
        result = await run_callback(QuotesSpider().parse, response)

        assert result.items[0] == {"text": "...", "author": "Albert Einstein"}
        assert [r.url for r in result.requests] == [
            "https://quotes.toscrape.com/page/2/"
        ]
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import TYPE_CHECKING

from ._scope import CrawlScope, enter_scope, invoke_callback
from .http import build_response
from .request import Request
from .response import HTMLResponse, Response

if TYPE_CHECKING:
    import os
    from collections.abc import Callable

    from ._types import JSONLike, MetaData
    from .request import Callback, Errback
    from .spiders import Spider

DEFAULT_URL = "https://example.com/"
DEFAULT_HTML_MAX_SIZE_BYTES = 5_000_000


def _new_item_list() -> list[JSONLike]:
    return []


@dataclass(slots=True)
class CallbackResult:
    """Items and requests a callback produced.

    Attributes:
        items: Items passed to ``emit()``, in order.
        requests: Requests passed to ``follow()`` (URLs already resolved and
            callbacks inherited), in order.
    """

    items: list[JSONLike] = field(default_factory=_new_item_list)
    requests: list[Request] = field(default_factory=list[Request])

    @property
    def urls(self) -> list[str]:
        """Return the URLs of :attr:`requests`."""
        return [request.url for request in self.requests]


def html_response(
    body: str | bytes,
    url: str = DEFAULT_URL,
    *,
    status: int = 200,
    headers: dict[str, str] | None = None,
    callback: Callback | None = None,
    meta: MetaData | None = None,
    html_max_size_bytes: int = DEFAULT_HTML_MAX_SIZE_BYTES,
) -> HTMLResponse:
    """Return an :class:`~silkworm.HTMLResponse` for ``body`` served at ``url``.

    ``callback`` and ``meta`` populate the originating request, so followed
    links inherit the callback exactly as during a crawl.
    """
    data = body.encode() if isinstance(body, str) else body
    request = Request(url=url, callback=callback, meta=dict(meta or {}))
    response_headers = {"content-type": "text/html; charset=utf-8"}
    response_headers.update(
        {key.lower(): value for key, value in (headers or {}).items()}
    )
    return HTMLResponse(
        url=url,
        status=status,
        headers=response_headers,
        body=data,
        request=request,
        doc_max_size_bytes=html_max_size_bytes,
    )


def response_from_file(
    path: str | os.PathLike[str],
    url: str = DEFAULT_URL,
    *,
    status: int = 200,
    headers: dict[str, str] | None = None,
    callback: Callback | None = None,
    meta: MetaData | None = None,
    html_max_size_bytes: int = DEFAULT_HTML_MAX_SIZE_BYTES,
) -> Response:
    """Return a response whose body is the bytes of ``path``.

    Files ending in ``.html``/``.htm`` (or whose content looks like HTML)
    become :class:`~silkworm.HTMLResponse`; JSON, XML, and other files become
    plain :class:`~silkworm.Response` objects, as the HTTP client would return.
    """
    file_path = Path(path)
    body = file_path.read_bytes()
    response_headers = {key.lower(): value for key, value in (headers or {}).items()}
    if "content-type" not in response_headers:
        response_headers["content-type"] = _guess_content_type(file_path)
    request = Request(url=url, callback=callback, meta=dict(meta or {}))
    return build_response(
        url=url,
        status=status,
        headers=response_headers,
        body=body,
        request=request,
        html_max_size_bytes=html_max_size_bytes,
    )


def _guess_content_type(path: Path) -> str:
    match path.suffix.lower():
        case ".html" | ".htm":
            return "text/html; charset=utf-8"
        case ".json":
            return "application/json"
        case ".xml":
            return "application/xml"
        case ".txt":
            return "text/plain; charset=utf-8"
        case _:
            return "application/octet-stream"


def _recording_scope(
    owner: str, response: Response | None
) -> tuple[CrawlScope, CallbackResult]:
    result = CallbackResult()

    async def emit_item(item: JSONLike) -> None:
        result.items.append(item)

    async def schedule_request(request: Request) -> None:
        result.requests.append(request)

    scope = CrawlScope(
        owner=owner,
        emit_item=emit_item,
        schedule_request=schedule_request,
        response=response,
    )
    return scope, result


async def _run(
    invoke: Callable[[], object], name: str, response: Response | None
) -> CallbackResult:
    scope, result = _recording_scope(name, response)
    async with enter_scope(scope):
        await invoke_callback(invoke, name)
    return result


async def run_callback(callback: Callback, response: Response) -> CallbackResult:
    """Run ``callback(response)`` and return what it emitted and followed.

    Exceptions raised by the callback propagate unchanged, so tests see the
    real traceback. A callback that yields, is synchronous, or returns a value
    raises :class:`~silkworm.exceptions.SpiderError`, as in a crawl.
    """
    name = getattr(callback, "__name__", "callback")
    return await _run(lambda: callback(response), name, response)


async def run_errback(
    errback: Errback, request: Request, exception: Exception
) -> CallbackResult:
    """Run ``errback(request, exception)`` and return what it produced."""
    name = getattr(errback, "__name__", "errback")
    return await _run(lambda: errback(request, exception), name, None)


async def run_start_requests(spider: Spider) -> list[Request]:
    """Return the requests ``spider.start_requests()`` schedules."""
    result = await _run(spider.start_requests, "start_requests", None)
    return result.requests


__all__ = [
    "CallbackResult",
    "html_response",
    "response_from_file",
    "run_callback",
    "run_errback",
    "run_start_requests",
]
