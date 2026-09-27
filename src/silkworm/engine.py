"""Asynchronous crawl scheduler, lifecycle manager, and callback dispatcher."""

from __future__ import annotations

import asyncio
import hashlib
import reprlib
import sys
import time
from collections import Counter, deque
from collections.abc import Awaitable, Callable, Iterable
from contextlib import suppress
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import timedelta
from functools import partial
from itertools import count
from typing import TYPE_CHECKING, TypedDict, cast

try:  # resource is POSIX-only
    import resource
except ImportError:  # pragma: no cover - platform dependent
    resource = None

from ._domains import DomainSlots, url_host
from ._jobs import JobState
from ._metrics import MetricsServer, render_metrics
from ._scope import CallbackContractError, CrawlScope, enter_scope, invoke_callback
from ._stats import CrawlResult, CrawlStats, freeze_result
from ._timeouts import to_seconds
from ._types import JSONLike, JSONValue
from ._urls import host_in_domains, normalize_domains, request_fingerprint
from ._validation import require_positive_int
from .exceptions import (
    CloseSpider,
    CrawlFailedError,
    DropItem,
    IgnoreRequest,
    SilkwormError,
    SpiderError,
)
from .http import DEFAULT_EMULATION, DEFAULT_MAX_RESPONSE_SIZE_BYTES, HttpClient
from .logging import Logger, LogLevel, complete_logs, get_logger, log_at_level
from .request import Callback, Request
from .response import HTMLResponse, Response

if TYPE_CHECKING:
    import os

    from wreq import Emulation, Profile

    from .http import FetchClient
    from .httpcache import HttpCache
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

# ``items_dropped_by_reason`` label for items discarded because ``max_items``
# was reached; excluded from the item drop rate used by the failure policy.
MAX_ITEMS_DROP_REASON = "max_items"


class _CrawlClosed(BaseException):
    """Aborts ``start_requests()`` once the crawl is stopping.

    Derives from ``BaseException`` so ``except Exception`` blocks in spider code
    cannot swallow it.
    """


def default_dedup_key(req: Request) -> str:
    """Return the engine's default deduplication key for ``req``.

    This is :func:`~silkworm.request_fingerprint`: the HTTP method, the
    canonical URL (normalized case, default port, sorted query, no fragment)
    with :attr:`~silkworm.Request.params` merged in, and the request body.
    Headers and metadata do not affect it.
    """
    return request_fingerprint(req)


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
    max_response_size_bytes: int | None
    request_middlewares: Iterable[RequestMiddleware] | None
    response_middlewares: Iterable[ResponseMiddleware] | None
    item_pipelines: Iterable[ItemPipeline] | None
    log_stats_interval: float | None
    keep_alive: bool
    http_client: FetchClient | None
    engine_logger: EngineLogger | None
    dedup_key: DedupKey | None
    concurrency_per_domain: int | None
    max_depth: int | None
    max_requests: int | None
    max_items: int | None
    max_errors: int | None
    max_duration: float | timedelta | None
    max_error_rate: float | None
    min_items: int | None
    max_item_drop_rate: float | None
    job_dir: str | os.PathLike[str] | None
    http_cache: HttpCache | None
    metrics_port: int | None
    metrics_host: str


def _require_optional_positive(value: int | None, name: str) -> None:
    if value is not None:
        require_positive_int(value, name)


def _require_optional_rate(value: float | None, name: str) -> None:
    if value is not None and not 0.0 <= value <= 1.0:
        msg = f"{name} must be between 0.0 and 1.0"
        raise ValueError(msg)


def _meta_depth(request: Request) -> int:
    depth = request.meta.get("depth", 0)
    return depth if isinstance(depth, int) and not isinstance(depth, bool) else 0


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
        max_response_size_bytes: Largest response body the default client
            downloads (``None`` for no limit); larger bodies fail with
            :class:`~silkworm.exceptions.ResponseTooLargeError`.
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
        dedup_key: Function mapping a request to its deduplication key.
            Defaults to :func:`default_dedup_key`.
        concurrency_per_domain: Maximum simultaneous fetches per host, or
            ``None`` for no per-host limit.
        max_depth: Drop requests more than this many links away from a start
            request (start requests have depth ``0``).
        max_requests: Stop after sending this many requests.
        max_items: Stop after this many items passed every pipeline; later
            items are dropped.
        max_errors: Stop after this many unrecovered failures.
        max_duration: Stop after this much wall-clock time.
        max_error_rate: Fail the crawl when ``errors / requests_sent`` exceeds
            this fraction.
        min_items: Fail the crawl when fewer items were scraped.
        max_item_drop_rate: Fail the crawl when the share of items dropped by
            pipelines exceeds this fraction.
        job_dir: Directory persisting the seen-set and unfinished requests so
            an interrupted crawl resumes where it stopped.
        http_cache: Serve and store responses through this on-disk cache.
        metrics_port: Serve Prometheus metrics at ``/metrics`` on this port
            while crawling (``0`` picks a free port).
        metrics_host: Interface for the metrics server.

    Requests with :attr:`~silkworm.Request.dont_filter` bypass deduplication
    and off-site filtering. Higher request priorities are dequeued before lower
    ones, while insertion order breaks ties. The stop limits end the crawl
    gracefully: pending requests are discarded (or kept in ``job_dir``),
    in-flight requests finish, and :meth:`run` reports the limit as the close
    reason. Failure-policy violations make :meth:`run` raise
    :class:`~silkworm.exceptions.CrawlFailedError`; they are not evaluated for
    crawls stopped with :meth:`stop`.
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
        max_response_size_bytes: int | None = DEFAULT_MAX_RESPONSE_SIZE_BYTES,
        request_middlewares: Iterable[RequestMiddleware] | None = None,
        response_middlewares: Iterable[ResponseMiddleware] | None = None,
        item_pipelines: Iterable[ItemPipeline] | None = None,
        log_stats_interval: float | None = None,
        keep_alive: bool = False,
        http_client: FetchClient | None = None,
        engine_logger: EngineLogger | None = None,
        dedup_key: DedupKey | None = None,
        concurrency_per_domain: int | None = None,
        max_depth: int | None = None,
        max_requests: int | None = None,
        max_items: int | None = None,
        max_errors: int | None = None,
        max_duration: float | timedelta | None = None,
        max_error_rate: float | None = None,
        min_items: int | None = None,
        max_item_drop_rate: float | None = None,
        job_dir: str | os.PathLike[str] | None = None,
        http_cache: HttpCache | None = None,
        metrics_port: int | None = None,
        metrics_host: str = "127.0.0.1",
    ) -> None:
        require_positive_int(concurrency, "concurrency")
        for name, value in (
            ("max_requests", max_requests),
            ("max_items", max_items),
            ("max_errors", max_errors),
        ):
            _require_optional_positive(value, name)
        if max_depth is not None and max_depth < 0:
            msg = "max_depth must be non-negative"
            raise ValueError(msg)
        if min_items is not None and min_items < 0:
            msg = "min_items must be non-negative"
            raise ValueError(msg)
        _require_optional_rate(max_error_rate, "max_error_rate")
        _require_optional_rate(max_item_drop_rate, "max_item_drop_rate")
        max_duration_seconds = to_seconds(max_duration)
        if max_duration_seconds is not None and max_duration_seconds <= 0:
            msg = "max_duration must be positive"
            raise ValueError(msg)
        if metrics_port is not None and not 0 <= metrics_port <= 65535:
            msg = "metrics_port must be between 0 and 65535"
            raise ValueError(msg)

        self.spider = spider
        client: FetchClient = (
            http_client
            if http_client is not None
            else HttpClient(
                concurrency=concurrency,
                emulation=emulation,
                timeout=request_timeout,
                html_max_size_bytes=html_max_size_bytes,
                keep_alive=keep_alive,
                max_response_size_bytes=max_response_size_bytes,
            )
        )
        require_positive_int(client.concurrency, "http_client.concurrency")
        self.http: FetchClient = (
            http_cache.wrap(client) if http_cache is not None else client
        )
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
        # 16-byte digests of dedup keys; far smaller than the keys themselves.
        self._seen: set[bytes] = set()
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

        # Scheduling policy
        self._allowed_domains = normalize_domains(
            getattr(spider, "allowed_domains", ())
        )
        self._offsite_hosts_logged: set[str] = set()
        self._domain_slots = (
            DomainSlots(concurrency_per_domain)
            if concurrency_per_domain is not None
            else None
        )
        self.max_depth: int | None = max_depth
        self.max_requests: int | None = max_requests
        self.max_items: int | None = max_items
        self.max_errors: int | None = max_errors
        self.max_duration: float | None = max_duration_seconds
        self.max_error_rate: float | None = max_error_rate
        self.min_items: int | None = min_items
        self.max_item_drop_rate: float | None = max_item_drop_rate
        self._close_reason: str | None = None
        self._fetches_started = 0
        self._items_reserved = 0
        self._in_flight = 0

        # Persistence and observability
        self._job_dir = job_dir
        self._job: JobState | None = None
        self._metrics_port = metrics_port
        self._metrics_host = metrics_host
        self.metrics_server: MetricsServer | None = None

        # Statistics tracking
        self.log_stats_interval: float | None = log_stats_interval
        self._start_time: float = 0.0
        self._event_loop_type: str | None = None
        self.stats: CrawlStats = CrawlStats()
        self._stats: dict[str, int] = self.stats.counters

    @property
    def close_reason(self) -> str | None:
        """Return why the crawl is stopping, or ``None`` while it runs normally."""
        return self._close_reason

    @property
    def in_flight(self) -> int:
        """Return the number of requests currently being processed."""
        return self._in_flight

    def stop(self, reason: str = "shutdown") -> None:
        """Stop the crawl gracefully.

        New requests are no longer scheduled and queued ones are discarded
        (they stay saved when a job directory is configured, so the crawl can
        resume). Requests already being processed finish, pipelines close
        normally, and :meth:`run` returns with ``reason`` as the close reason.
        Calling it again has no effect.
        """
        if self._close_reason is not None:
            return
        self._close_reason = reason
        self.logger.info(
            "Stopping crawl",
            spider=self.spider.name,
            reason=reason,
            pending_requests=self._queue.qsize(),
            in_flight=self._in_flight,
        )
        # Let producers blocked on queue capacity observe the close.
        for waiter, _ in self._capacity_waiters:
            if not waiter.done():
                waiter.set_result(True)
        self._capacity_waiters.clear()
        while True:
            try:
                self._queue.get_nowait()
            except asyncio.QueueEmpty:
                break
            self.stats.inc("dropped_requests")
            self._queue.task_done()

    async def open_spider(self) -> None:
        """Open middleware, spider, and pipelines, then enqueue initial requests.

        Middleware opens before the spider; pipelines open afterward in their
        configured order. When resuming a job, saved requests are queued before
        ``start_requests()`` runs (already-seen start requests are skipped).
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

            self._restore_pending_requests()
            try:
                await self._run_callback(
                    self.spider.start_requests,
                    name="start_requests",
                    url=None,
                    response=None,
                    parent=None,
                )
            except _CrawlClosed:
                self.logger.debug(
                    "Stopped start_requests because the crawl is closing",
                    reason=self._close_reason,
                )
        except BaseException as exc:
            cleanup_errors = await self._close_lifecycle_components()
            self._record_cleanup_failures(exc, cleanup_errors)
            raise

    def _restore_pending_requests(self) -> None:
        if self._job is None or not self._job.resumed:
            return
        restored = self._job.load_pending()
        for seq, request in restored:
            self._queue.put_nowait((-request.priority, seq, request))
        if restored:
            self._request_order = count(max(seq for seq, _ in restored) + 1)
        self.logger.info(
            "Resuming job",
            spider=self.spider.name,
            job_dir=str(self._job.directory),
            restored_requests=len(restored),
            seen_requests=self._job.seen_count(),
        )

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

    async def _shutdown(self, *, finished: bool) -> list[BaseException]:
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
        if self._job is not None:
            job, self._job = self._job, None
            try:
                job.close(finished=finished)
                self.logger.info(
                    "Saved job state",
                    job_dir=str(job.directory),
                    status="finished" if finished else "paused",
                )
            except BaseException as exc:
                errors.append(exc)
                self.logger.exception(
                    "Job state cleanup failed",
                    error=str(exc),
                    error_type=exc.__class__.__name__,
                )
        if self.metrics_server is not None:
            try:
                await self.metrics_server.close()
            except BaseException as exc:  # noqa: BLE001 - attempt every cleanup
                errors.append(exc)
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
            self.stats.inc("retries")
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
            parent=req,
        )
        return True

    async def _enqueue(self, req: Request, parent: Request | None = None) -> None:
        """Filter, deduplicate, and queue ``req`` (scheduled from ``parent``)."""
        req = self._with_depth(req, parent)
        if not self._passes_scheduling_filters(req):
            return
        # Serialize before marking the request seen so an unrestorable request
        # fails loudly without polluting the seen-set.
        payload = self._job.serialize(req) if self._job is not None else None
        if not req.dont_filter and not self._mark_seen(req):
            self.stats.inc("dupe_filtered")
            self.logger.debug("Skipping already seen request", url=req.url)
            return
        if self._close_reason is None:
            await self._wait_for_queue_capacity(req)
        seq = next(self._request_order)
        if self._job is not None and payload is not None:
            self._job.add_pending(seq, req, payload)
        if self._close_reason is not None:
            self.stats.inc("dropped_requests")
            if _CURRENT_WORKER.get() is None:
                raise _CrawlClosed
            return
        self._queue.put_nowait((-req.priority, seq, req))
        self.logger.debug(
            "Enqueued request",
            url=req.url,
            dont_filter=req.dont_filter,
            priority=req.priority,
            depth=_meta_depth(req),
        )

    def _with_depth(self, req: Request, parent: Request | None) -> Request:
        if parent is not None:
            depth = _meta_depth(parent) + 1
        elif "depth" in req.meta:
            return req
        else:
            depth = 0
        return req.replace(meta={**req.meta, "depth": depth})

    def _passes_scheduling_filters(self, req: Request) -> bool:
        if self._allowed_domains and not req.dont_filter:
            host = url_host(req.url)
            if not host_in_domains(host, self._allowed_domains):
                self.stats.inc("offsite_filtered")
                if (
                    host not in self._offsite_hosts_logged
                    and len(self._offsite_hosts_logged) < 1000
                ):
                    self._offsite_hosts_logged.add(host)
                    self.logger.debug(
                        "Filtered offsite request",
                        url=req.url,
                        host=host,
                        allowed_domains=list(self._allowed_domains),
                    )
                return False
        if self.max_depth is not None and _meta_depth(req) > self.max_depth:
            self.stats.inc("depth_filtered")
            self.logger.debug(
                "Filtered request beyond max_depth",
                url=req.url,
                depth=_meta_depth(req),
                max_depth=self.max_depth,
            )
            return False
        return True

    def _mark_seen(self, req: Request) -> bool:
        """Record ``req``'s dedup key; return ``False`` when already seen."""
        digest = hashlib.blake2b(self.dedup_key(req).encode(), digest_size=16).digest()
        if self._job is not None:
            return self._job.seen_add(digest)
        if digest in self._seen:
            return False
        self._seen.add(digest)
        return True

    def _seen_count(self) -> int:
        return self._job.seen_count() if self._job is not None else len(self._seen)

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

    async def _worker(self, index: int = 0) -> None:
        # Each worker runs in its own task, so this only tags this worker and the
        # tasks its callbacks spawn.
        _CURRENT_WORKER.set(index)
        while not self._stop_event.is_set():
            try:
                async with asyncio.timeout(1.0):
                    _, seq, req = await self._queue.get()
            except TimeoutError:
                if self._stop_event.is_set():
                    break
                continue
            except asyncio.CancelledError:
                break
            self._wake_capacity_waiter()

            completed = True
            try:
                if self._close_reason is not None or not self._reserve_fetch():
                    # Stopping: leave the request journaled so a job can resume.
                    completed = False
                    self.stats.inc("dropped_requests")
                    continue
                self._in_flight += 1
                try:
                    await self._process_request(req)
                finally:
                    self._in_flight -= 1
            except IgnoreRequest as exc:
                self.stats.inc("ignored_requests")
                self.stats.inc_labeled("ignored_by_reason", exc.reason)
                self.logger.debug(
                    "Ignored request",
                    url=req.url,
                    reason=exc.reason,
                    detail=str(exc),
                )
            except CloseSpider as exc:
                self.stop(exc.reason)
            except Exception as exc:  # noqa: BLE001 - logged by the failure handler
                await self._handle_request_failure(req, exc)
            finally:
                if self._job is not None and completed:
                    self._job.remove_pending(seq)
                self._queue.task_done()

    def _reserve_fetch(self) -> bool:
        """Claim one of ``max_requests`` fetches; stop the crawl when exhausted."""
        if self.max_requests is None:
            return True
        if self._fetches_started >= self.max_requests:
            self.stop("max_requests")
            return False
        self._fetches_started += 1
        return True

    async def _process_request(self, req: Request) -> None:
        req = await self._apply_request_mw(req)
        self.engine_logger.fetching_request(self.logger, req, self.spider)
        self.stats.inc("requests_sent")
        self.stats.inc_labeled("requests_by_domain", url_host(req.url) or "-")
        if self._domain_slots is not None:
            async with self._domain_slots.acquire(req.url):
                resp = await self.http.fetch(req)
        else:
            resp = await self.http.fetch(req)
        self.stats.inc("responses_received")
        self.stats.inc_labeled("responses_by_status", str(resp.status))
        self.engine_logger.fetched_response(self.logger, req, resp, self.spider)
        await self._handle_response(resp)

    async def _handle_request_failure(self, req: Request, exc: Exception) -> None:
        if await self._handle_request_exception(req, exc):
            return

        self.stats.inc("errors")
        cause = exc.__cause__ if isinstance(exc, SpiderError) else None
        self.stats.inc_labeled("errors_by_type", type(cause or exc).__name__)
        if self.max_errors is not None and self.stats.get("errors") >= self.max_errors:
            self.stop("max_errors")
        try:
            if await self._handle_request_errback(req, exc):
                return
        except Exception as errback_exc:
            errback_cause = errback_exc.__cause__ or errback_exc.__context__
            error_context = {
                "url": req.url,
                "error": str(errback_exc),
                "error_type": errback_exc.__class__.__name__,
                "original_error": str(exc),
                "original_error_type": exc.__class__.__name__,
                "spider": self.spider.name,
            }
            if errback_cause is not None:
                error_context["cause"] = self._safe_repr(errback_cause)
                error_context["cause_type"] = errback_cause.__class__.__name__
            self.logger.error(
                "Request errback failed",
                **error_context,
                exc_info=not isinstance(errback_exc, SilkwormError),
            )
            return

        failure_cause = exc.__cause__ or exc.__context__
        error_context = {
            "url": req.url,
            "error": str(exc),
            "error_type": exc.__class__.__name__,
            "spider": self.spider.name,
        }
        if failure_cause is not None:
            error_context["cause"] = self._safe_repr(failure_cause)
            error_context["cause_type"] = failure_cause.__class__.__name__
        # silkworm's own errors are self-explanatory (and callback failures are
        # already logged with a traceback); only unexpected errors, e.g. bugs in
        # middlewares or pipelines, get one here.
        self.logger.error(
            "Failed to process request",
            **error_context,
            exc_info=not isinstance(exc, SilkwormError),
        )

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
                self.stats.inc("retries")
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
                parent=processed.request,
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
        parent: Request | None,
    ) -> None:
        """Run a callback coroutine inside a scope wired to the engine sinks.

        Items reported with ``emit`` reach the pipelines and requests reported
        with ``follow`` reach the queue (one level deeper than ``parent``) while
        the callback runs. :class:`~silkworm.exceptions.CloseSpider` stops the
        crawl instead of failing the callback.
        """
        scope = CrawlScope(
            owner=name,
            emit_item=self._emit_item,
            schedule_request=partial(self._enqueue, parent=parent),
            response=response,
        )
        async with enter_scope(scope):
            try:
                await invoke_callback(invoke, name)
            except CallbackContractError:
                raise
            except CloseSpider as exc:
                self.stop(exc.reason)
            except Exception as exc:
                raise self._callback_failure(name, url, exc) from exc

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
        if self.max_items is not None and self._items_reserved >= self.max_items:
            self._count_dropped_item(MAX_ITEMS_DROP_REASON)
            return
        self._items_reserved += 1
        accepted = False
        try:
            self.logger.debug(
                "Processing scraped item",
                spider=self.spider.name,
                pipelines=len(self.item_pipelines),
            )
            # Pipelines take JSONValue; callbacks may emit read-only JSONLike
            # shapes, which are the same objects at runtime.
            accepted = bool(await self._process_item(cast(JSONValue, item)))
        finally:
            if not accepted:
                self._items_reserved -= 1
        if (
            accepted
            and self.max_items is not None
            and self.stats.get("items_scraped") >= self.max_items
        ):
            self.stop(MAX_ITEMS_DROP_REASON)

    async def _process_item(self, item: JSONValue) -> bool:
        """Run ``item`` through the pipelines; return whether it was kept."""
        for pipe in self.item_pipelines:
            self.engine_logger.running_item_pipeline(
                self.logger,
                pipe,
                self.spider,
            )
            try:
                item = await pipe.process_item(item, self.spider)
            except DropItem as exc:
                self._count_dropped_item(exc.reason)
                self.logger.debug(
                    "Dropped item",
                    pipeline=pipe.__class__.__name__,
                    reason=exc.reason,
                    detail=str(exc),
                )
                return False
        self.stats.inc("items_scraped")
        return True

    def _count_dropped_item(self, reason: str) -> None:
        self.stats.inc("items_dropped")
        self.stats.inc_labeled("items_dropped_by_reason", reason)

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

    def _stats_payload(self, elapsed: float) -> dict[str, object]:
        requests_rate = self.stats.get("requests_sent") / elapsed if elapsed > 0 else 0
        payload: dict[str, object] = {
            "elapsed_seconds": round(elapsed, 1),
            **self.stats.counters,
            "queue_size": self._queue.qsize(),
            "in_flight": self._in_flight,
            "requests_per_second": round(requests_rate, 2),
            "seen_requests": self._seen_count(),
            "memory_mb": round(self._get_memory_usage_mb(), 2),
        }
        for name, counter in self.stats.labeled.items():
            if counter:
                payload[name] = dict(counter)
        return payload

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

    def metrics_text(self) -> str:
        """Return current statistics in the Prometheus text exposition format."""
        elapsed = time.time() - self._start_time if self._start_time else 0.0
        return render_metrics(
            spider=self.spider.name,
            stats=self.stats,
            gauges={
                "queue_size": self._queue.qsize(),
                "in_flight": self._in_flight,
                "seen_requests": self._seen_count(),
                "elapsed_seconds": round(elapsed, 3),
                "memory_mb": round(self._get_memory_usage_mb(), 2),
                "running": 0 if self._stop_event.is_set() else 1,
            },
            custom=self.spider.stats_payload,
        )

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

    async def _enforce_max_duration(self, seconds: float) -> None:
        try:
            async with asyncio.timeout(seconds):
                await self._stop_event.wait()
        except TimeoutError:
            self.stop("max_duration")

    def _evaluate_failure_policy(self, close_reason: str) -> tuple[str, ...]:
        if close_reason == "shutdown":
            return ()
        failures: list[str] = []
        sent = self.stats.get("requests_sent")
        errors = self.stats.get("errors")
        if (
            self.max_error_rate is not None
            and sent
            and errors / sent > self.max_error_rate
        ):
            failures.append(
                f"error rate {errors / sent:.1%} exceeds max_error_rate "
                f"{self.max_error_rate:.1%} ({errors} errors / {sent} requests)"
            )
        scraped = self.stats.get("items_scraped")
        if self.min_items is not None and scraped < self.min_items:
            failures.append(
                f"scraped {scraped} items, fewer than min_items={self.min_items}"
            )
        if self.max_item_drop_rate is not None:
            dropped = self.stats.get("items_dropped") - self.stats.labeled[
                "items_dropped_by_reason"
            ].get(MAX_ITEMS_DROP_REASON, 0)
            total = scraped + dropped
            if total and dropped / total > self.max_item_drop_rate:
                failures.append(
                    f"item drop rate {dropped / total:.1%} exceeds "
                    f"max_item_drop_rate {self.max_item_drop_rate:.1%} "
                    f"({dropped} dropped / {total} items)"
                )
        return tuple(failures)

    def _final_statistics(self, close_reason: str | None) -> None:
        self.logger.info(
            "Final crawl statistics",
            close_reason=close_reason,
            **self._statistics_log_context(
                time.time() - self._start_time,
                include_event_loop=True,
            ),
        )

    async def run(self) -> CrawlResult:
        """Run the crawl until the queue drains or a stop condition, then clean up.

        Worker tasks, periodic statistics, and lifecycle hooks are managed as a
        task group. The HTTP client, spider components, job state, and metrics
        server are always closed, and a final statistics record is emitted.

        Returns:
            The crawl's :class:`~silkworm.CrawlResult`.

        Raises:
            CrawlFailedError: If the crawl violated its failure policy
                (``max_error_rate``, ``min_items``, ``max_item_drop_rate``).
            Exception: Errors from lifecycle hooks, pipelines' ``open``/``close``,
                or cleanup propagate after structured error logging.
        """
        self.logger.info("Starting engine", spider=self.spider.name)
        self._start_time = time.time()
        self._event_loop_type = self._detect_event_loop()

        try:
            if self._job_dir is not None and self._job is None:
                self._job = JobState(self._job_dir, self.spider)
            if self._metrics_port is not None:
                self.metrics_server = MetricsServer(
                    self.metrics_text,
                    host=self._metrics_host,
                    port=self._metrics_port,
                )
                await self.metrics_server.start()
            async with asyncio.TaskGroup() as tg:
                self._worker_count = self.http.concurrency
                for index in range(self._worker_count):
                    tg.create_task(self._worker(index))

                if self.log_stats_interval is not None and self.log_stats_interval > 0:
                    tg.create_task(self._log_statistics())
                if self.max_duration is not None:
                    tg.create_task(self._enforce_max_duration(self.max_duration))

                # Open spider and seed initial requests while workers are already waiting.
                await self.open_spider()
                await self._queue.join()
                self._stop_event.set()
        except BaseException as exc:
            self._stop_event.set()
            self._final_statistics(self._close_reason or "error")
            cleanup_errors = await self._shutdown(finished=False)
            self._record_cleanup_failures(exc, cleanup_errors)
            raise

        close_reason = self._close_reason or "finished"
        self._stop_event.set()
        self._final_statistics(close_reason)
        self._raise_cleanup_errors(
            await self._shutdown(finished=close_reason == "finished")
        )
        result = freeze_result(
            spider=self.spider.name,
            close_reason=close_reason,
            elapsed_seconds=time.time() - self._start_time,
            stats=self.stats,
            custom_stats=self.spider.stats_payload,
            failures=self._evaluate_failure_policy(close_reason),
        )
        if result.failures:
            self.logger.error(
                "Crawl failed its failure policy",
                spider=self.spider.name,
                failures=list(result.failures),
            )
            raise CrawlFailedError(result)
        return result

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
