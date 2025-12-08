from __future__ import annotations
import asyncio
import inspect
from typing import Any, Iterable, Set

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
        emulation=None,
        request_timeout: float | None = None,
        request_middlewares: Iterable[RequestMiddleware] | None = None,
        response_middlewares: Iterable[ResponseMiddleware] | None = None,
        item_pipelines: Iterable[ItemPipeline] | None = None,
    ) -> None:
        self.spider = spider
        self.http = HttpClient(  # type: ignore[arg-type]
            concurrency=concurrency, emulation=emulation, timeout=request_timeout
        )
        self._queue: asyncio.Queue[Request] = asyncio.Queue()
        self._seen: Set[str] = set()
        self._stopping = False
        self._tasks: list[asyncio.Task] = []
        self.logger = get_logger(component="engine", spider=self.spider.name)

        self.request_middlewares = list(request_middlewares or [])
        self.response_middlewares = list(response_middlewares or [])
        self.item_pipelines = list(item_pipelines or [])

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
        req = await self._apply_request_mw(req)
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
                self.logger.debug(
                    "Fetching request",
                    url=req.url,
                    method=req.method,
                    callback=getattr(req.callback, "__name__", None),
                )
                resp = await self.http.fetch(req)
                self.logger.info(
                    "Fetched response",
                    url=req.url,
                    status=resp.status,
                    spider=self.spider.name,
                )
                await self._handle_response(resp)
            except Exception as exc:
                self.logger.error(
                    "Failed to process request",
                    url=req.url,
                    error=str(exc),
                    spider=self.spider.name,
                )
                raise
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
        processed = await self._apply_response_mw(resp)

        if isinstance(processed, Request):
            # e.g. RetryMiddleware wants a retry
            self.logger.debug("Retrying request from middleware", url=processed.url)
            await self._enqueue(processed)
            return

        resp = processed
        callback = resp.request.callback

        produced: Any
        if callback is None:
            if not isinstance(resp, HTMLResponse):
                raise TypeError("Spider.parse requires an HTMLResponse")
            produced = self.spider.parse(resp)
        else:
            produced = callback(resp)
        results = await produced if inspect.isawaitable(produced) else produced

        if results is None:
            return

        async def iterate():
            if hasattr(results, "__aiter__"):
                async for x in results:
                    yield x
            else:
                for x in results:
                    yield x

        async for x in iterate():
            if isinstance(x, Request):
                await self._enqueue(x)
            else:
                self.logger.debug(
                    "Processing scraped item",
                    spider=self.spider.name,
                    pipelines=len(self.item_pipelines),
                )
                await self._process_item(x)

    async def _process_item(self, item: Any) -> None:
        for pipe in self.item_pipelines:
            self.logger.debug(
                "Running item pipeline",
                pipeline=pipe.__class__.__name__,
                spider=self.spider.name,
            )
            item = await pipe.process_item(item, self.spider)

    async def run(self) -> None:
        self.logger.info("Starting engine", spider=self.spider.name)
        await self.open_spider()
        # number of workers = client concurrency
        concurrency = self.http._sem._value  # type: ignore[attr-defined]
        for _ in range(concurrency):
            task = asyncio.create_task(self._worker())
            self._tasks.append(task)

        await self._queue.join()
        self._stopping = True
        await asyncio.gather(*self._tasks, return_exceptions=True)
        await self.http.close()
        await self.close_spider()
        complete_logs()
