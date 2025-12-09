from __future__ import annotations
import asyncio
import inspect
import time
from typing import Any, AsyncIterator, Iterable, Set

from .exceptions import SpiderError
from .http import HttpClient
from .request import Request
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
        request_middlewares: Iterable[RequestMiddleware] | None = None,
        response_middlewares: Iterable[ResponseMiddleware] | None = None,
        item_pipelines: Iterable[ItemPipeline] | None = None,
        log_stats_interval: float | None = None,
    ) -> None:
        self.spider = spider
        self.http = HttpClient(  # type: ignore[arg-type]
            concurrency=concurrency, emulation=emulation, timeout=request_timeout
        )
        # Bound the queue to avoid unbounded growth when many requests are scheduled.
        default_queue_size = concurrency * 10
        queue_size = (
            max_pending_requests if max_pending_requests is not None else default_queue_size
        )
        self._queue: asyncio.Queue[Request] = asyncio.Queue(maxsize=queue_size)
        self._seen: Set[str] = set()
        self._stopping = False
        self._tasks: list[asyncio.Task] = []
        self.logger = get_logger(component="engine", spider=self.spider.name)

        self.request_middlewares = list(request_middlewares or [])
        self.response_middlewares = list(response_middlewares or [])
        self.item_pipelines = list(item_pipelines or [])

        # Statistics tracking
        self.log_stats_interval = log_stats_interval
        self._stats_task: asyncio.Task | None = None
        self._start_time: float = 0.0
        self._stats = {
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
        if not req.dont_filter and req.url in self._seen:
            self.logger.debug("Skipping already seen request", url=req.url)
            return
        self._seen.add(req.url)
        self.logger.debug("Enqueued request", url=req.url, dont_filter=req.dont_filter)
        await self._queue.put(req)

    async def _worker(self) -> None:
        while True:
            if self._stopping:
                break
            try:
                req = await asyncio.wait_for(self._queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                if self._stopping:
                    break
                continue

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
                self.logger.error(
                    "Failed to process request",
                    url=req.url,
                    error=str(exc),
                    error_type=exc.__class__.__name__,
                    spider=self.spider.name,
                )
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
            current = await mw.process_response(current, self.spider)  # type: ignore[arg-type]
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

        produced: Any
        try:
            if callback is None:
                if not isinstance(resp, HTMLResponse):
                    raise TypeError("Spider.parse requires an HTMLResponse")
                produced = self.spider.parse(resp)
            else:
                produced = callback(resp)
        except Exception as exc:
            name = getattr(callback, "__name__", "parse") if callback else "parse"
            raise SpiderError(
                f"Spider callback '{name}' failed for {self.spider.name}"
            ) from exc
        try:
            async for x in self._iterate_callback_results(produced):
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
            raise SpiderError(
                f"Spider callback '{name}' yielded invalid results"
            ) from exc
        finally:
            resp.close()
            if resp is not original_resp:
                original_resp.close()

    async def _iterate_callback_results(self, produced: Any) -> AsyncIterator[Any]:
        """
        Normalize any supported callback return shape (single item, Request,
        sync/async iterator, or awaitable) into an async iterator.
        """
        results = await produced if inspect.isawaitable(produced) else produced

        if results is None:
            return

        if isinstance(results, Request):
            yield results
            return

        if hasattr(results, "__aiter__"):
            async for x in results:  # type: ignore[operator]
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
        yield results

    async def _process_item(self, item: Any) -> None:
        self._stats["items_scraped"] += 1
        for pipe in self.item_pipelines:
            self.logger.debug(
                "Running item pipeline",
                pipeline=pipe.__class__.__name__,
                spider=self.spider.name,
            )
            item = await pipe.process_item(item, self.spider)

    async def _log_statistics(self) -> None:
        """Periodically log statistics about the crawl progress."""
        if self.log_stats_interval is None:
            return

        while not self._stopping:
            await asyncio.sleep(self.log_stats_interval)
            if self._stopping:
                break

            elapsed = time.time() - self._start_time
            requests_rate = self._stats["requests_sent"] / elapsed if elapsed > 0 else 0

            self.logger.info(
                "Crawl statistics",
                spider=self.spider.name,
                elapsed_seconds=round(elapsed, 1),
                requests_sent=self._stats["requests_sent"],
                responses_received=self._stats["responses_received"],
                items_scraped=self._stats["items_scraped"],
                errors=self._stats["errors"],
                queue_size=self._queue.qsize(),
                requests_per_second=round(requests_rate, 2),
            )

    async def run(self) -> None:
        self.logger.info("Starting engine", spider=self.spider.name)
        self._start_time = time.time()

        # number of workers = client concurrency
        concurrency = self.http._sem._value  # type: ignore[attr-defined]
        for _ in range(concurrency):
            task = asyncio.create_task(self._worker())
            self._tasks.append(task)

        # Open spider and seed initial requests while workers are already waiting.
        try:
            await self.open_spider()

            # Start statistics logging if interval is configured
            if self.log_stats_interval is not None and self.log_stats_interval > 0:
                self._stats_task = asyncio.create_task(self._log_statistics())

            await self._queue.join()
        finally:
            self._stopping = True
            await asyncio.gather(*self._tasks, return_exceptions=True)

            # Stop statistics logging task if it was started
            if self._stats_task is not None:
                self._stats_task.cancel()
                try:
                    await self._stats_task
                except asyncio.CancelledError:
                    pass

            # Log final statistics
            elapsed = time.time() - self._start_time
            requests_rate = self._stats["requests_sent"] / elapsed if elapsed > 0 else 0
            self.logger.info(
                "Final crawl statistics",
                spider=self.spider.name,
                elapsed_seconds=round(elapsed, 1),
                requests_sent=self._stats["requests_sent"],
                responses_received=self._stats["responses_received"],
                items_scraped=self._stats["items_scraped"],
                errors=self._stats["errors"],
                requests_per_second=round(requests_rate, 2),
            )

            await self.http.close()
            await self.close_spider()
            complete_logs()
