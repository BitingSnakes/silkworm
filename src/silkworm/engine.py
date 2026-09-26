"""Asynchronous crawl scheduler, lifecycle manager, and callback dispatcher."""

from __future__ import annotations

import asyncio
import inspect
import reprlib
import sys
import time
from collections import Counter, deque
from collections.abc import Awaitable, Callable, Iterable
from contextlib import suppress
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import timedelta
from itertools import count
from typing import TYPE_CHECKING, TypedDict, cast

try:  # resource is POSIX-only
    import resource
except ImportError:  # pragma: no cover - platform dependent
    resource = None

from ._scope import CrawlScope, enter_scope
from ._types import JSONLike, JSONValue
from ._validation import require_positive_int
from .exceptions import SilkwormError, SpiderError
from .http import DEFAULT_EMULATION, HttpClient
from .logging import Logger, LogLevel, complete_logs, get_logger, log_at_level
from .request import Callback, Request
from .response import HTMLResponse, Response

if TYPE_CHECKING:
    from wreq import Emulation, Profile

    from .middlewares import (
        ExceptionMiddleware,
        RequestMiddleware,
        ResponseMiddleware,
    )
    from .pipelines import ItemPipeline
    from .spiders import Spider


@dataclass(slots=True)
class EngineLogger:
    """
    Customizable engine event logger.

    Subclass this when you need to redact or reshape selected engine log events.
    Set an event level to ``None`` to suppress that event.
    """

    fetched_response_level: LogLevel = "INFO"
    fetching_request_level: LogLevel = "DEBUG"
    item_pipeline_level: LogLevel = "DEBUG"
    retry_request_level: LogLevel = "DEBUG"
    include_request_url: bool = True

    def fetching_request(
        self,
        logger: Logger,
        request: Request,
        spider: Spider,
    ) -> None:
        """Log that ``request`` is about to be sent."""
        context: dict[str, object] = {
            "method": request.method,
            "callback": getattr(request.callback, "__name__", None),
            "spider": spider.name,
        }
        if self.include_request_url:
            context["url"] = request.url
        log_at_level(logger, self.fetching_request_level, "Fetching request", **context)

    def fetched_response(
        self,
        logger: Logger,
        request: Request,
        response: Response,
        spider: Spider,
    ) -> None:
        """Log a completed response with status and optional request URL."""
        context: dict[str, object] = {
            "status": response.status,
            "spider": spider.name,
        }
        if self.include_request_url:
            context["url"] = request.url
        log_at_level(logger, self.fetched_response_level, "Fetched response", **context)

    def retrying_request(
        self,
        logger: Logger,
        request: Request,
        spider: Spider,
        *,
        source: str,
    ) -> None:
        """Log that a middleware or response requested another attempt."""
        context: dict[str, object] = {"source": source, "spider": spider.name}
        if self.include_request_url:
            context["url"] = request.url
        log_at_level(
            logger,
            self.retry_request_level,
            "Retrying request",
            **context,
        )

    def running_item_pipeline(
        self,
        logger: Logger,
        pipeline: ItemPipeline,
        spider: Spider,
    ) -> None:
        """Log item dispatch using the pipeline's effective log level."""
        log_level = cast(
            "LogLevel", getattr(pipeline, "log_level", self.item_pipeline_level)
        )
        log_at_level(
            logger,
            log_level,
            "Running item pipeline",
            pipeline=pipeline.__class__.__name__,
            spider=spider.name,
        )


_SAFE_REPR = reprlib.Repr()
_SAFE_REPR.maxstring = 120
_SAFE_REPR.maxother = 120
_SAFE_REPR.maxlist = 8
_SAFE_REPR.maxdict = 8
_SAFE_REPR.maxset = 8
_SAFE_REPR.maxtuple = 8

type DedupKey = Callable[[Request], str]
type PrioritizedRequest = tuple[int, int, Request]
type LifecycleCloser = tuple[str, Callable[[], Awaitable[object]]]

# Index of the engine worker whose callback (or a task it spawned) is running;
# ``None`` outside workers, e.g. while ``start_requests()`` seeds the queue.
_CURRENT_WORKER: ContextVar[int | None] = ContextVar(
    "silkworm_engine_worker",
    default=None,
)


def default_dedup_key(req: Request) -> str:
    """Return the request URL used by the engine's default deduplicator.

    Method, body, headers, and query parameters stored separately in
    :attr:`~silkworm.Request.params` do not affect this key.
    """
    return req.url


class EngineOptions(TypedDict, total=False):
    """Keyword options for :class:`Engine`, also accepted by every runner.

    ``run_spider(MySpider, concurrency=32, request_timeout=10)`` forwards these
    to ``Engine``; omitted keys use the ``Engine`` defaults. Supply
    ``http_client`` to inject a compatible client; its concurrency then controls
    worker count and default queue capacity.
    """

    concurrency: int
    max_pending_requests: int | None
    emulation: Emulation | Profile | None
    request_timeout: float | timedelta | None
    html_max_size_bytes: int
    request_middlewares: Iterable[RequestMiddleware] | None
    response_middlewares: Iterable[ResponseMiddleware] | None
    item_pipelines: Iterable[ItemPipeline] | None
    log_stats_interval: float | None
    keep_alive: bool
    http_client: HttpClient | None
    engine_logger: EngineLogger | None
    dedup_key: DedupKey | None


class Engine:
    """Coordinate request scheduling, HTTP I/O, callbacks, and item pipelines.

    Args:
        spider: Spider instance to execute.
        concurrency: Maximum simultaneous HTTP requests for the default client.
        max_pending_requests: Queue capacity used for backpressure. Defaults to
            ten times the effective HTTP client concurrency. ``start_requests()``
            waits while the queue is full; callbacks wait only when they are the
            sole producer, otherwise they enqueue past the bound so workers keep
            crawling and can never deadlock.
        emulation: Browser profile used by the default ``wreq`` client; pass
            ``None`` to disable impersonation.
        request_timeout: Default per-request timeout.
        html_max_size_bytes: Maximum document size parsed by HTML responses.
        request_middlewares: Request processors applied in list order.
        response_middlewares: Response processors applied in list order.
        item_pipelines: Item processors applied in list order, passing each
            returned value to the next pipeline.
        log_stats_interval: Seconds between statistics messages, or ``None`` to
            disable periodic summaries.
        keep_alive: Request connection reuse from the default client when
            supported by ``wreq``.
        http_client: Preconfigured client replacing the default client.
        engine_logger: Event logger customization.
        dedup_key: Function mapping a request to its deduplication key. Defaults
            to URL-only deduplication.

    Requests with :attr:`~silkworm.Request.dont_filter` bypass deduplication.
    Higher request priorities are dequeued before lower ones, while insertion
    order breaks ties.
    """

    def __init__(
        self,
        spider: Spider,
        *,
        concurrency: int = 16,
        max_pending_requests: int | None = None,
        emulation: Emulation | Profile | None = DEFAULT_EMULATION,
        request_timeout: float | timedelta | None = None,
        html_max_size_bytes: int = 5_000_000,
        request_middlewares: Iterable[RequestMiddleware] | None = None,
        response_middlewares: Iterable[ResponseMiddleware] | None = None,
        item_pipelines: Iterable[ItemPipeline] | None = None,
        log_stats_interval: float | None = None,
        keep_alive: bool = False,
        http_client: HttpClient | None = None,
        engine_logger: EngineLogger | None = None,
        dedup_key: DedupKey | None = None,
    ) -> None:
        require_positive_int(concurrency, "concurrency")
        self.spider = spider
        self.http: HttpClient = (
            http_client
            if http_client is not None
            else HttpClient(
                concurrency=concurrency,
                emulation=emulation,
                timeout=request_timeout,
                html_max_size_bytes=html_max_size_bytes,
                keep_alive=keep_alive,
            )
        )
        require_positive_int(self.http.concurrency, "http_client.concurrency")
        # Bound the queue to avoid unbounded growth when many requests are scheduled.
        default_queue_size = self.http.concurrency * 10
        if max_pending_requests is not None:
            require_positive_int(max_pending_requests, "max_pending_requests")
        self.max_pending_requests: int = (
            max_pending_requests
            if max_pending_requests is not None
            else default_queue_size
        )
        self._request_order = count()
        # The queue itself is unbounded: ``_wait_for_queue_capacity`` enforces
        # ``max_pending_requests`` so it can let a worker overflow the bound
        # rather than deadlock (see that method).
        self._queue: asyncio.PriorityQueue[PrioritizedRequest] = asyncio.PriorityQueue()
        self._capacity_waiters: deque[tuple[asyncio.Future[bool], int | None]] = deque()
        self._stalled_workers: Counter[int] = Counter()
        self._worker_count = 0
        self._seen: set[str] = set()
        self.dedup_key: DedupKey = dedup_key or default_dedup_key
        self._stop_event = asyncio.Event()
        self.logger: Logger = get_logger(component="engine", spider=self.spider.name)
        self.engine_logger: EngineLogger = engine_logger or EngineLogger()

        self.request_middlewares: list[RequestMiddleware] = list(
            request_middlewares or []
        )
        self.response_middlewares: list[ResponseMiddleware] = list(
            response_middlewares or []
        )
        self.item_pipelines: list[ItemPipeline] = list(item_pipelines or [])
        self._lifecycle_closers: list[LifecycleCloser] = []

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
        """Open middleware, spider, and pipelines, then enqueue initial requests.

        Middleware opens before the spider; pipelines open afterward in their
        configured order.
        """
        if self._lifecycle_closers:
            raise RuntimeError("Spider lifecycle is already open")

        self.logger.info("Opening spider", spider=self.spider.name)
        try:
            await self._open_middlewares()
            self._register_lifecycle_close(
                f"spider {self.spider.name}",
                self.spider.close,
            )
            await self.spider.open()
            for pipe in self.item_pipelines:
                self._register_lifecycle_close(
                    f"pipeline {pipe.__class__.__name__}",
                    lambda pipe=pipe: pipe.close(self.spider),
                )
                await pipe.open(self.spider)

            await self._run_callback(
                self.spider.start_requests,
                name="start_requests",
                url=None,
                response=None,
            )
        except BaseException as exc:
            cleanup_errors = await self._close_lifecycle_components()
            self._record_cleanup_failures(exc, cleanup_errors)
            raise

    async def close_spider(self) -> None:
        """Close pipelines, the spider, and middleware lifecycle hooks.

        Components close in reverse startup order. Middleware instances close
        once even when registered for both request and response work.
        """
        self.logger.info("Closing spider", spider=self.spider.name)
        self._raise_cleanup_errors(await self._close_lifecycle_components())

    def _iter_middlewares(self) -> Iterable[object]:
        seen_ids: set[int] = set()
        for middleware in [*self.request_middlewares, *self.response_middlewares]:
            middleware_id = id(middleware)
            if middleware_id in seen_ids:
                continue
            seen_ids.add(middleware_id)
            yield middleware

    def _iter_exception_middlewares(self) -> Iterable[ExceptionMiddleware]:
        for middleware in self._iter_middlewares():
            process_exception = getattr(middleware, "process_exception", None)
            if callable(process_exception):
                yield cast("ExceptionMiddleware", middleware)

    async def _open_middlewares(self) -> None:
        for middleware in self._iter_middlewares():
            close_hook = getattr(middleware, "close", None)
            if callable(close_hook):
                self._register_lifecycle_close(
                    f"middleware {middleware.__class__.__name__}",
                    lambda close_hook=close_hook: cast(
                        "Awaitable[object]", close_hook(self.spider)
                    ),
                )
            open_hook = getattr(middleware, "open", None)
            if callable(open_hook):
                await cast("Awaitable[object]", open_hook(self.spider))

    def _register_lifecycle_close(
        self,
        name: str,
        closer: Callable[[], Awaitable[object]],
    ) -> None:
        self._lifecycle_closers.append((name, closer))

    async def _close_lifecycle_components(self) -> list[BaseException]:
        closers = self._lifecycle_closers
        self._lifecycle_closers = []
        errors: list[BaseException] = []
        for name, closer in reversed(closers):
            try:
                await closer()
            except BaseException as exc:
                errors.append(exc)
                self.logger.exception(
                    "Lifecycle cleanup failed",
                    component=name,
                    error=str(exc),
                    error_type=exc.__class__.__name__,
                )
        return errors

    def _record_cleanup_failures(
        self,
        primary: BaseException,
        errors: Iterable[BaseException],
    ) -> None:
        for error in errors:
            primary.add_note(f"Cleanup failed with {error.__class__.__name__}: {error}")

    def _raise_cleanup_errors(self, errors: list[BaseException]) -> None:
        if not errors:
            return
        if len(errors) == 1:
            raise errors[0]
        raise BaseExceptionGroup("Multiple resource cleanup failures", errors)

    async def _shutdown(self) -> list[BaseException]:
        errors = await self._close_lifecycle_components()
        try:
            await self.http.close()
        except BaseException as exc:
            errors.append(exc)
            self.logger.exception(
                "HTTP client cleanup failed",
                error=str(exc),
                error_type=exc.__class__.__name__,
            )
        try:
            complete_logs()
        except BaseException as exc:  # noqa: BLE001 - logging flush is final cleanup
            errors.append(exc)
        return errors

    async def _apply_request_mw(self, req: Request) -> Request:
        for mw in self.request_middlewares:
            req = await mw.process_request(req, self.spider)
        return req

    async def _handle_request_exception(
        self,
        req: Request,
        exc: Exception,
    ) -> bool:
        for mw in self._iter_exception_middlewares():
            self.logger.debug(
                "Calling exception middleware",
                url=req.url,
                middleware=mw.__class__.__name__,
                error=str(exc),
                error_type=exc.__class__.__name__,
            )
            retry_request = await mw.process_exception(req, exc, self.spider)
            if retry_request is None:
                self.logger.debug(
                    "Exception middleware did not retry request",
                    url=req.url,
                    middleware=mw.__class__.__name__,
                    error_type=exc.__class__.__name__,
                )
                continue
            if not isinstance(retry_request, Request):
                self.logger.warning(
                    "Ignoring invalid exception middleware result",
                    url=req.url,
                    middleware=mw.__class__.__name__,
                    result_type=retry_request.__class__.__name__,
                    error_type=exc.__class__.__name__,
                )
                continue

            self.engine_logger.retrying_request(
                self.logger,
                retry_request,
                self.spider,
                source=f"exception middleware {mw.__class__.__name__}",
            )
            await self._enqueue(retry_request)
            return True

        return False

    async def _handle_request_errback(
        self,
        req: Request,
        exc: Exception,
    ) -> bool:
        errback = req.errback
        if errback is None:
            return False

        name = getattr(errback, "__name__", errback.__class__.__name__)
        self.logger.debug(
            "Calling request errback",
            url=req.url,
            errback=name,
            error=str(exc),
            error_type=exc.__class__.__name__,
        )
        await self._run_callback(
            lambda: errback(req, exc),
            name=name,
            url=req.url,
            response=None,
        )
        return True

    async def _enqueue(self, req: Request) -> None:
        if not req.dont_filter:
            key = self.dedup_key(req)
            if key in self._seen:
                self.logger.debug(
                    "Skipping already seen request",
                    url=req.url,
                    dedup_key=key,
                )
                return
            self._seen.add(key)
        await self._wait_for_queue_capacity(req)
        self._queue.put_nowait(self._priority_entry(req))
        self.logger.debug(
            "Enqueued request",
            url=req.url,
            dont_filter=req.dont_filter,
            priority=req.priority,
        )

    async def _wait_for_queue_capacity(self, req: Request) -> None:
        """Apply ``max_pending_requests`` backpressure to one enqueue.

        Requests scheduled outside workers (``start_requests()``) wait until the
        queue has room. Workers are also the queue's only consumers, so a
        worker's callback (or a task it spawned) may wait only while no other
        worker is waiting and at least one other worker exists. That throttles a
        single heavy producer while the remaining workers keep consuming.

        Any other worker that finds the queue full enqueues past the bound and
        releases waiting workers to do the same. When the frontier grows faster
        than it is consumed, blocking workers would only idle them (and keep
        their responses alive) without bounding the queue, so they keep
        crawling instead. This also guarantees the workers never deadlock.
        """
        worker = _CURRENT_WORKER.get()
        while self._queue.qsize() >= self.max_pending_requests:
            if worker is not None and not self._worker_may_wait(worker):
                self.logger.debug(
                    "Queue full; enqueuing past max_pending_requests to keep "
                    "workers crawling",
                    url=req.url,
                    queue_size=self._queue.qsize(),
                    max_pending_requests=self.max_pending_requests,
                )
                self._release_waiting_workers()
                return

            waiter: asyncio.Future[bool] = asyncio.get_running_loop().create_future()
            self._capacity_waiters.append((waiter, worker))
            if worker is not None:
                self._stalled_workers[worker] += 1
            try:
                overflow = await waiter
            except asyncio.CancelledError:
                if waiter.done() and not waiter.cancelled() and not waiter.result():
                    # Woken for a free slot but cancelled first; pass it on.
                    self._wake_capacity_waiter()
                raise
            finally:
                with suppress(ValueError):
                    self._capacity_waiters.remove((waiter, worker))
                if worker is not None:
                    self._stalled_workers[worker] -= 1
                    if not self._stalled_workers[worker]:
                        del self._stalled_workers[worker]
            if overflow:
                return

    def _worker_may_wait(self, worker: int) -> bool:
        if self._worker_count < 2:
            return False
        return all(stalled == worker for stalled in self._stalled_workers)

    def _wake_capacity_waiter(self) -> None:
        """Wake the oldest waiter because a queue slot was freed."""
        while self._capacity_waiters:
            waiter, _ = self._capacity_waiters.popleft()
            if not waiter.done():
                waiter.set_result(False)
                return

    def _release_waiting_workers(self) -> None:
        """Let every waiting worker enqueue past the bound."""
        remaining: deque[tuple[asyncio.Future[bool], int | None]] = deque()
        for waiter, worker in self._capacity_waiters:
            if worker is not None and not waiter.done():
                waiter.set_result(True)
            elif not waiter.done():
                remaining.append((waiter, worker))
        self._capacity_waiters = remaining

    def _priority_entry(self, req: Request) -> PrioritizedRequest:
        return (-req.priority, next(self._request_order), req)

    async def _worker(self, index: int = 0) -> None:
        # Each worker runs in its own task, so this only tags this worker and the
        # tasks its callbacks spawn.
        _CURRENT_WORKER.set(index)
        while not self._stop_event.is_set():
            try:
                async with asyncio.timeout(1.0):
                    _, _, req = await self._queue.get()
            except TimeoutError:
                if self._stop_event.is_set():
                    break
                continue
            except asyncio.CancelledError:
                break
            self._wake_capacity_waiter()

            try:
                req = await self._apply_request_mw(req)
                self.engine_logger.fetching_request(self.logger, req, self.spider)
                self._stats["requests_sent"] += 1
                resp = await self.http.fetch(req)
                self._stats["responses_received"] += 1
                self.engine_logger.fetched_response(
                    self.logger,
                    req,
                    resp,
                    self.spider,
                )
                await self._handle_response(resp)
            except Exception as exc:
                if await self._handle_request_exception(req, exc):
                    continue

                self._stats["errors"] += 1
                try:
                    if await self._handle_request_errback(req, exc):
                        continue
                except Exception as errback_exc:
                    cause = errback_exc.__cause__ or errback_exc.__context__
                    error_context = {
                        "url": req.url,
                        "error": str(errback_exc),
                        "error_type": errback_exc.__class__.__name__,
                        "original_error": str(exc),
                        "original_error_type": exc.__class__.__name__,
                        "spider": self.spider.name,
                    }
                    if cause is not None:
                        error_context["cause"] = self._safe_repr(cause)
                        error_context["cause_type"] = cause.__class__.__name__
                    self.logger.error(
                        "Request errback failed",
                        **error_context,
                        exc_info=not isinstance(errback_exc, SilkwormError),
                    )
                    continue

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
                # silkworm's own errors are self-explanatory (and callback
                # failures are already logged with a traceback); only unexpected
                # errors, e.g. bugs in middlewares or pipelines, get one here.
                self.logger.error(
                    "Failed to process request",
                    **error_context,
                    exc_info=not isinstance(exc, SilkwormError),
                )
                # Keep the worker alive so other requests can continue to be processed.
                continue
            finally:
                self._queue.task_done()

    async def _apply_response_mw(
        self,
        resp: Response,
        owned_responses: list[Response],
    ) -> Response | Request:
        current: Response | Request = resp
        for mw in self.response_middlewares:
            if isinstance(current, Request):
                # already converted to a retry Request by a previous mw
                break
            previous = current
            current = await mw.process_response(previous, self.spider)
            if isinstance(current, Response) and current is not previous:
                owned_responses.append(current)
        return current

    async def _handle_response(self, resp: Response) -> None:
        owned_responses = [resp]
        try:
            processed = await self._apply_response_mw(resp, owned_responses)
            if isinstance(processed, Request):
                # e.g. RetryMiddleware wants a retry
                self.engine_logger.retrying_request(
                    self.logger,
                    processed,
                    self.spider,
                    source="response middleware",
                )
                await self._enqueue(processed)
                return

            callback = processed.request.callback
            name = getattr(callback, "__name__", "parse") if callback else "parse"
            effective_callback = callback or self.spider.parse
            if self._expects_html(callback):
                callback_resp = self._ensure_html_response(processed)
                if callback_resp is not processed:
                    owned_responses.append(callback_resp)
            else:
                callback_resp = processed

            await self._run_callback(
                lambda: effective_callback(callback_resp),
                name=name,
                url=processed.url,
                response=callback_resp,
            )
        finally:
            primary = sys.exception()
            closed_ids: set[int] = set()
            cleanup_errors: list[BaseException] = []
            for owned_response in reversed(owned_responses):
                if id(owned_response) in closed_ids:
                    continue
                closed_ids.add(id(owned_response))
                try:
                    owned_response.close()
                except BaseException as exc:  # noqa: BLE001 - close every response
                    cleanup_errors.append(exc)
            if primary is not None:
                self._record_cleanup_failures(primary, cleanup_errors)
            else:
                self._raise_cleanup_errors(cleanup_errors)

    async def _run_callback(
        self,
        invoke: Callable[[], object],
        *,
        name: str,
        url: str | None,
        response: Response | None,
    ) -> None:
        """Run a callback coroutine inside a scope wired to the engine sinks.

        Items reported with ``emit`` reach the pipelines and requests reported
        with ``follow`` reach the queue while the callback runs.
        """
        scope = CrawlScope(
            owner=name,
            emit_item=self._emit_item,
            schedule_request=self._enqueue,
            response=response,
        )
        async with enter_scope(scope):
            try:
                produced = invoke()
            except Exception as exc:
                raise self._callback_failure(name, url, exc) from exc

            if inspect.isasyncgen(produced):
                raise SpiderError(
                    f"Spider callback '{name}' is an async generator; callbacks "
                    "must not yield. Replace `yield item` with "
                    "`await self.emit(item)` and `yield request` with "
                    "`await self.follow(request)`",
                )
            if not inspect.isawaitable(produced):
                raise SpiderError(
                    f"Spider callback '{name}' must be an async function, "
                    f"got a {type(produced).__name__} result",
                )

            try:
                returned: object = await produced
            except Exception as exc:
                raise self._callback_failure(name, url, exc) from exc

        if returned is not None:
            raise SpiderError(
                f"Spider callback '{name}' returned a {type(returned).__name__}; "
                "callbacks must return None and report results with "
                "`await self.emit(item)` / `await self.follow(request)`",
            )

    def _callback_failure(
        self,
        name: str,
        url: str | None,
        exc: Exception,
    ) -> SpiderError:
        self.logger.exception(
            "Spider callback failed",
            callback=name,
            spider=self.spider.name,
            url=url,
            error=str(exc),
            error_type=exc.__class__.__name__,
        )
        return SpiderError(f"Spider callback '{name}' failed for {self.spider.name}")

    async def _emit_item(self, item: JSONLike) -> None:
        self.logger.debug(
            "Processing scraped item",
            spider=self.spider.name,
            pipelines=len(self.item_pipelines),
        )
        # Pipelines take JSONValue; callbacks may emit read-only JSONLike
        # shapes, which are the same objects at runtime.
        await self._process_item(cast(JSONValue, item))

    async def _process_item(self, item: JSONValue) -> None:
        self._stats["items_scraped"] += 1
        for pipe in self.item_pipelines:
            self.engine_logger.running_item_pipeline(
                self.logger,
                pipe,
                self.spider,
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

        if "rsloop" in module or "rsloop" in name:
            return "rsloop"
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

    def _statistics_log_context(
        self,
        elapsed: float,
        *,
        include_event_loop: bool = False,
    ) -> dict[str, object]:
        context: dict[str, object] = {
            "spider": self.spider.name,
            **self._stats_payload(elapsed),
            **self.spider.stats_payload,
        }
        if include_event_loop:
            context["event_loop"] = self._event_loop_type
        return context

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
                    **self._statistics_log_context(time.time() - self._start_time),
                )

    async def run(self) -> None:
        """Run the crawl to queue exhaustion and release all resources.

        Worker tasks, periodic statistics, and lifecycle hooks are managed as a
        task group. The HTTP client and spider components are closed in a
        ``finally`` block, and a final statistics record is always emitted.
        Exceptions from requests, callbacks, middleware, pipelines, or cleanup
        propagate to the caller after structured error logging.
        """
        self.logger.info("Starting engine", spider=self.spider.name)
        self._start_time = time.time()
        self._event_loop_type = self._detect_event_loop()

        try:
            async with asyncio.TaskGroup() as tg:
                self._worker_count = self.http.concurrency
                for index in range(self._worker_count):
                    tg.create_task(self._worker(index))

                if self.log_stats_interval is not None and self.log_stats_interval > 0:
                    tg.create_task(self._log_statistics())

                # Open spider and seed initial requests while workers are already waiting.
                await self.open_spider()
                await self._queue.join()
                self._stop_event.set()
        except BaseException as exc:
            self._stop_event.set()
            self.logger.info(
                "Final crawl statistics",
                **self._statistics_log_context(
                    time.time() - self._start_time,
                    include_event_loop=True,
                ),
            )
            cleanup_errors = await self._shutdown()
            self._record_cleanup_failures(exc, cleanup_errors)
            raise
        else:
            self._stop_event.set()
            self.logger.info(
                "Final crawl statistics",
                **self._statistics_log_context(
                    time.time() - self._start_time,
                    include_event_loop=True,
                ),
            )
            self._raise_cleanup_errors(await self._shutdown())

    def _expects_html(self, callback: Callback | None) -> bool:
        if callback is None:
            return True

        cb_self = getattr(callback, "__self__", None)
        cb_func = getattr(callback, "__func__", None)
        parse_func = getattr(self.spider.parse, "__func__", None)
        return cb_self is self.spider and cb_func is parse_func

    def _ensure_html_response(self, resp: Response) -> HTMLResponse:
        if isinstance(resp, HTMLResponse):
            return resp
        return HTMLResponse(
            url=resp.url,
            status=resp.status,
            headers=resp.headers,
            body=resp.body,
            request=resp.request,
            doc_max_size_bytes=self.http.html_max_size_bytes,
        )

    def _safe_repr(self, value: object, limit: int = 200) -> str:
        if value is None:
            return "None"
        text = _SAFE_REPR.repr(value)
        return text if len(text) <= limit else f"{text[:limit]}..."
