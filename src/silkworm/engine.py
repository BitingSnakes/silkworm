from __future__ import annotations
import asyncio
import inspect
import sys
import time
from collections.abc import AsyncIterable, AsyncIterator, Callable, Iterable
from typing import cast

try:  # resource is POSIX-only
    import resource
except ImportError:  # pragma: no cover - platform dependent
    resource = None  # type: ignore[assignment]

from .exceptions import SpiderError
from ._types import JSONValue
from .http import HttpClient
from .request import CallbackOutput, CallbackResult, Request
from .response import HTMLResponse, Response
from .spiders import Spider
from .middlewares import RequestMiddleware, ResponseMiddleware
from .pipelines import ItemPipeline
from .logging import complete_logs, get_logger


class Engine:
    def __init__(
        self,
        spider: Spider,
        *,
        concurrency: int = 16,
        max_pending_requests: int | None = None,
        emulation=None,
        request_timeout: float | None = None,
        html_max_size_bytes: int = 5_000_000,
        request_middlewares: Iterable[RequestMiddleware] | None = None,
        response_middlewares: Iterable[ResponseMiddleware] | None = None,
        item_pipelines: Iterable[ItemPipeline] | None = None,
        log_stats_interval: float | None = None,
        keep_alive: bool = False,
    ) -> None:
        self.spider = spider
        self.http = HttpClient(
            concurrency=concurrency,
            emulation=emulation,
            timeout=request_timeout,
            html_max_size_bytes=html_max_size_bytes,
            keep_alive=keep_alive,
        )
        # Bound the queue to avoid unbounded growth when many requests are scheduled.
        default_queue_size = concurrency * 10
        queue_size = (
            max_pending_requests
            if max_pending_requests is not None
            else default_queue_size
        )
        self._queue: asyncio.Queue[Request] = asyncio.Queue(maxsize=queue_size)
        self._seen: set[str] = set()
        self._stop_event = asyncio.Event()
        self.logger = get_logger(component="engine", spider=self.spider.name)

        self.request_middlewares = list(request_middlewares or [])
        self.response_middlewares = list(response_middlewares or [])
        self.item_pipelines = list(item_pipelines or [])

        # Statistics tracking
        self.log_stats_interval = log_stats_interval
        self._start_time: float = 0.0
        self._event_loop_type: str | None = None
        self._stats: dict[str, int] = {
            "requests_sent": 0,
            "responses_received": 0,
            "items_scraped": 0,
            "errors": 0,
        }

    async def open_spider(self) -> None:
        self.logger.info("Opening spider", spider=self.spider.name)
        await self.spider.open()
        for pipe in self.item_pipelines:
            await pipe.open(self.spider)

        async for req in self.spider.start_requests():
            await self._enqueue(req)

    async def close_spider(self) -> None:
        self.logger.info("Closing spider", spider=self.spider.name)
        for pipe in self.item_pipelines:
            await pipe.close(self.spider)
        await self.spider.close()

    async def _apply_request_mw(self, req: Request) -> Request:
        for mw in self.request_middlewares:
            req = await mw.process_request(req, self.spider)
        return req

    async def _enqueue(self, req: Request) -> None:
        if not req.dont_filter:
            if req.url in self._seen:
                self.logger.debug("Skipping already seen request", url=req.url)
                return
            self._seen.add(req.url)
        self.logger.debug("Enqueued request", url=req.url, dont_filter=req.dont_filter)
        await self._queue.put(req)

    async def _worker(self) -> None:
        while not self._stop_event.is_set():
            try:
                async with asyncio.timeout(1.0):
                    req = await self._queue.get()
            except TimeoutError:
                if self._stop_event.is_set():
                    break
                continue
            except asyncio.CancelledError:
                break

            try:
                req = await self._apply_request_mw(req)
                self.logger.debug(
                    "Fetching request",
                    url=req.url,
                    method=req.method,
                    callback=getattr(req.callback, "__name__", None),
                )
                self._stats["requests_sent"] += 1
                resp = await self.http.fetch(req)
                self._stats["responses_received"] += 1
                self.logger.info(
                    "Fetched response",
                    url=req.url,
                    status=resp.status,
                    spider=self.spider.name,
                )
                await self._handle_response(resp)
            except Exception as exc:
                self._stats["errors"] += 1
                cause = exc.__cause__ or exc.__context__
                error_context = {
                    "url": req.url,
                    "error": str(exc),
                    "error_type": exc.__class__.__name__,
                    "spider": self.spider.name,
                }
                if cause is not None:
                    error_context["cause"] = self._safe_repr(cause)
                    error_context["cause_type"] = cause.__class__.__name__
                self.logger.error("Failed to process request", **error_context)
                # Keep the worker alive so other requests can continue to be processed.
                continue
            finally:
                self._queue.task_done()

    async def _apply_response_mw(self, resp: Response) -> Response | Request:
        current: Response | Request = resp
        for mw in self.response_middlewares:
            if isinstance(current, Request):
                # already converted to a retry Request by a previous mw
                break
            current = await mw.process_response(current, self.spider)
        return current

    async def _handle_response(self, resp: Response) -> None:
        original_resp = resp
        try:
            processed = await self._apply_response_mw(resp)
        except Exception:
            original_resp.close()
            raise

        if isinstance(processed, Request):
            # e.g. RetryMiddleware wants a retry
            self.logger.debug("Retrying request from middleware", url=processed.url)
            original_resp.close()
            await self._enqueue(processed)
            return

        resp = processed
        callback = resp.request.callback

        produced: CallbackResult
        try:
            effective_callback = callback or self.spider.parse
            if self._expects_html(callback):
                html_resp = self._ensure_html_response(resp)
                produced = effective_callback(html_resp)
            else:
                produced = effective_callback(resp)
        except Exception as exc:
            name = getattr(callback, "__name__", "parse") if callback else "parse"
            raise SpiderError(
                f"Spider callback '{name}' failed for {self.spider.name}"
            ) from exc

        last_yielded: Request | JSONValue | None = None

        try:
            async for x in self._iterate_callback_results(produced):
                last_yielded = x
                if isinstance(x, Request):
                    await self._enqueue(x)
                else:
                    self.logger.debug(
                        "Processing scraped item",
                        spider=self.spider.name,
                        pipelines=len(self.item_pipelines),
                    )
                    await self._process_item(x)
        except Exception as exc:
            name = getattr(callback, "__name__", "parse") if callback else "parse"
            self.logger.error(
                "Callback yielded invalid results",
                callback=name,
                produced_type=type(produced).__name__,
                last_yielded_type=type(last_yielded).__name__
                if last_yielded is not None
                else None,
                last_yielded_repr=self._safe_repr(last_yielded),
                spider=self.spider.name,
                url=resp.url,
                error=str(exc),
                error_type=exc.__class__.__name__,
                exc_info=True,
            )
            raise SpiderError(
                f"Spider callback '{name}' yielded invalid results"
            ) from exc
        finally:
            resp.close()
            if resp is not original_resp:
                original_resp.close()

    async def _iterate_callback_results(
        self, produced: CallbackResult
    ) -> AsyncIterator[Request | JSONValue]:
        """
        Normalize any supported callback return shape (single item, Request,
        sync/async iterator, or awaitable) into an async iterator.
        """
        results: CallbackOutput
        results = await produced if inspect.isawaitable(produced) else produced

        if results is None:
            return

        if isinstance(results, Request):
            yield results
            return

        if isinstance(results, (AsyncIterator, AsyncIterable)):
            async for x in results:
                yield x
            return

        if isinstance(results, Iterable) and not isinstance(
            results, (str, bytes, bytearray)
        ):
            for x in results:
                yield x
            return

        # Fallback: treat any other value as a single item to avoid confusing
        # TypeError from iterating over non-iterables.
        yield cast(JSONValue, results)

    async def _process_item(self, item: JSONValue) -> None:
        self._stats["items_scraped"] += 1
        for pipe in self.item_pipelines:
            self.logger.debug(
                "Running item pipeline",
                pipeline=pipe.__class__.__name__,
                spider=self.spider.name,
            )
            item = await pipe.process_item(item, self.spider)

    def _get_memory_usage_mb(self) -> float:
        """
        Return memory usage (RSS) in megabytes, normalizing platform differences.
        """
        if resource is None:
            return 0.0
        usage = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        divisor = 1024 * 1024 if sys.platform == "darwin" else 1024
        return usage / divisor

    def _detect_event_loop(self, loop: asyncio.AbstractEventLoop | None = None) -> str:
        """
        Identify which event loop implementation is currently running.
        """
        loop = loop or asyncio.get_running_loop()
        module = loop.__class__.__module__.lower()
        name = loop.__class__.__name__.lower()

        if "uvloop" in module or "uvloop" in name:
            return "uvloop"
        if "trio" in module or "trio" in name:
            return "trio"
        return "asyncio"

    def _stats_payload(self, elapsed: float) -> dict[str, float | int]:
        requests_rate = self._stats["requests_sent"] / elapsed if elapsed > 0 else 0
        return {
            "elapsed_seconds": round(elapsed, 1),
            "requests_sent": self._stats["requests_sent"],
            "responses_received": self._stats["responses_received"],
            "items_scraped": self._stats["items_scraped"],
            "errors": self._stats["errors"],
            "queue_size": self._queue.qsize(),
            "requests_per_second": round(requests_rate, 2),
            "seen_requests": len(self._seen),
            "memory_mb": round(self._get_memory_usage_mb(), 2),
        }

    async def _log_statistics(self) -> None:
        """Periodically log statistics about the crawl progress."""
        if self.log_stats_interval is None:
            return

        interval = self.log_stats_interval
        if interval <= 0:
            return

        while not self._stop_event.is_set():
            try:
                async with asyncio.timeout(interval):
                    await self._stop_event.wait()
                    break
            except TimeoutError:
                self.logger.info(
                    "Crawl statistics",
                    spider=self.spider.name,
                    **self._stats_payload(time.time() - self._start_time),
                )

    async def run(self) -> None:
        self.logger.info("Starting engine", spider=self.spider.name)
        self._start_time = time.time()
        self._event_loop_type = self._detect_event_loop()

        try:
            async with asyncio.TaskGroup() as tg:
                for _ in range(self.http.concurrency):
                    tg.create_task(self._worker())

                if self.log_stats_interval is not None and self.log_stats_interval > 0:
                    tg.create_task(self._log_statistics())

                # Open spider and seed initial requests while workers are already waiting.
                await self.open_spider()
                await self._queue.join()
                self._stop_event.set()
        finally:
            self._stop_event.set()

            self.logger.info(
                "Final crawl statistics",
                spider=self.spider.name,
                event_loop=self._event_loop_type,
                **self._stats_payload(time.time() - self._start_time),
            )

            await self.http.close()
            await self.close_spider()
            complete_logs()

    def _expects_html(
        self, callback: Callable[[Response], CallbackResult] | None
    ) -> bool:
        if callback is None:
            return True

        cb_self = getattr(callback, "__self__", None)
        cb_func = getattr(callback, "__func__", None)
        parse_func = getattr(self.spider.parse, "__func__", None)
        if cb_self is self.spider and cb_func is parse_func:
            return True

        return False

    def _ensure_html_response(self, resp: Response) -> HTMLResponse:
        if isinstance(resp, HTMLResponse):
            return resp
        return HTMLResponse(
            url=resp.url,
            status=resp.status,
            headers=dict(resp.headers),
            body=resp.body,
            request=resp.request,
            doc_max_size_bytes=self.http.html_max_size_bytes,
        )

    def _safe_repr(self, value: object, limit: int = 200) -> str:
        if value is None:
            return "None"
        text = repr(value)
        return text if len(text) <= limit else f"{text[:limit]}…"
