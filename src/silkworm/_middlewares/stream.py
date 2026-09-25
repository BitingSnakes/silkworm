from __future__ import annotations

import asyncio
import base64
import time
from collections.abc import Mapping
from datetime import timedelta
from typing import TYPE_CHECKING, Any, cast
from uuid import uuid4

from wreq import Client, Method  # type: ignore[import]

from .._types import JSONValue
from ..logging import get_logger
from ..request import Request
from ..response import Response
from .base import _callback_name, _timeout_seconds, _utc_timestamp

if TYPE_CHECKING:
    from ..spiders import Spider


class RequestResponseStreamMiddleware:
    """
    Stream request/response telemetry to a remote HTTP endpoint.

    Each outbound request receives a unique `exchange_id`. The middleware emits
    a `request` event before fetch and a `response` event after fetch, both
    carrying the same identifier so downstream analysis can join them.

    Use the same middleware instance in both `request_middlewares` and
    `response_middlewares` so a single sender queue can stream the full
    exchange lifecycle:

        stream = RequestResponseStreamMiddleware(
            "https://collector.example.com/events",
            auth_token="secret-token",
            batch_size=50,
        )

        run_spider(
            MySpider,
            request_middlewares=[stream],
            response_middlewares=[stream],
        )
    """

    _EXCHANGE_ID_META_KEY = "_stream_exchange_id"
    _PARENT_EXCHANGE_ID_META_KEY = "_stream_parent_exchange_id"
    _STARTED_AT_META_KEY = "_stream_started_at"
    _STOP = object()

    def __init__(
        self,
        url: str,
        *,
        method: str = "POST",
        headers: dict[str, str] | None = None,
        timeout: float | timedelta | None = 10.0,
        max_body_bytes: int = 64_000,
        queue_size: int = 1_000,
        auth_token: str | None = None,
        auth_scheme: str = "Bearer",
        batch_size: int = 1,
        batch_envelope_key: str = "events",
    ) -> None:
        if not url.strip():
            msg = "url must not be empty"
            raise ValueError(msg)
        if max_body_bytes < 0:
            msg = "max_body_bytes must be non-negative"
            raise ValueError(msg)
        if queue_size <= 0:
            msg = "queue_size must be greater than 0"
            raise ValueError(msg)
        if auth_token is not None and not auth_token.strip():
            msg = "auth_token must not be empty when provided"
            raise ValueError(msg)
        if not auth_scheme.strip():
            msg = "auth_scheme must not be empty"
            raise ValueError(msg)
        if batch_size <= 0:
            msg = "batch_size must be greater than 0"
            raise ValueError(msg)
        if not batch_envelope_key.strip():
            msg = "batch_envelope_key must not be empty"
            raise ValueError(msg)

        self.url = url
        self.method = method
        self.headers = dict(headers or {})
        self.timeout = timeout
        self.max_body_bytes = max_body_bytes
        self.queue_size = queue_size
        self.auth_token = auth_token
        self.auth_scheme = auth_scheme
        self.batch_size = batch_size
        self.batch_envelope_key = batch_envelope_key

        self._client: Client | None = None  # type: ignore[name-defined]
        self._queue: asyncio.Queue[JSONValue | object] | None = None
        self._sender_task: asyncio.Task[None] | None = None
        self._dropped_events = 0
        self.logger = get_logger(component="RequestResponseStreamMiddleware")

        if self.auth_token is not None and "Authorization" not in self.headers:
            self.headers["Authorization"] = (
                f"{self.auth_scheme.strip()} {self.auth_token}"
            )

    async def open(self, spider: Spider) -> None:
        await self._ensure_started()
        self.logger.info(
            "Opened request/response stream middleware",
            spider=spider.name,
            url=self.url,
            method=self.method,
            queue_size=self.queue_size,
            batch_size=self.batch_size,
        )

    async def close(self, spider: Spider) -> None:
        queue = self._queue
        sender_task = self._sender_task

        if queue is not None and sender_task is not None:
            await queue.put(self._STOP)
            await sender_task

        self._sender_task = None
        self._queue = None
        self._dropped_events = 0

        client = self._client
        self._client = None
        if client is not None:
            closer = getattr(client, "aclose", None) or getattr(client, "close", None)
            if closer and callable(closer):
                result = closer()
                if hasattr(result, "__await__"):
                    await result  # type: ignore[misc]

        self.logger.info(
            "Closed request/response stream middleware",
            spider=spider.name,
            url=self.url,
        )

    async def process_request(self, request: Request, spider: Spider) -> Request:
        previous_exchange_id = request.meta.get(self._EXCHANGE_ID_META_KEY)
        exchange_id = uuid4().hex
        request.meta[self._EXCHANGE_ID_META_KEY] = exchange_id
        request.meta[self._STARTED_AT_META_KEY] = time.perf_counter()
        if isinstance(previous_exchange_id, str):
            request.meta[self._PARENT_EXCHANGE_ID_META_KEY] = previous_exchange_id

        await self._enqueue(
            self._request_event(request, spider, exchange_id, previous_exchange_id),
        )
        return request

    async def process_response(
        self,
        response: Response,
        spider: Spider,
    ) -> Response | Request:
        exchange_id = response.request.meta.get(self._EXCHANGE_ID_META_KEY)
        if not isinstance(exchange_id, str):
            exchange_id = uuid4().hex
            response.request.meta[self._EXCHANGE_ID_META_KEY] = exchange_id

        await self._enqueue(self._response_event(response, spider, exchange_id))
        return response

    async def process_exception(
        self,
        request: Request,
        exception: Exception,
        spider: Spider,
    ) -> Request | None:
        exchange_id = request.meta.get(self._EXCHANGE_ID_META_KEY)
        if not isinstance(exchange_id, str):
            exchange_id = uuid4().hex
            request.meta[self._EXCHANGE_ID_META_KEY] = exchange_id

        await self._enqueue(
            {
                "event": "request_error",
                "exchange_id": exchange_id,
                "parent_exchange_id": self._parent_exchange_id(request),
                "timestamp": _utc_timestamp(),
                "spider": spider.name,
                "duration_ms": self._duration_ms(request),
                "error": str(exception),
                "error_type": exception.__class__.__name__,
                "request": self._serialize_request(request),
            },
        )
        return None

    async def _ensure_started(self) -> None:
        if self._client is None:
            self._client = Client()  # type: ignore[misc]
        if self._queue is None:
            self._queue = asyncio.Queue(maxsize=self.queue_size)
        if self._sender_task is None:
            self._sender_task = asyncio.create_task(self._run_sender())

    async def _enqueue(self, payload: JSONValue) -> None:
        await self._ensure_started()
        queue = self._queue
        if queue is None:
            return

        try:
            queue.put_nowait(payload)
        except asyncio.QueueFull:
            self._dropped_events += 1
            self.logger.warning(
                "Dropping stream event because queue is full",
                dropped_events=self._dropped_events,
                url=self.url,
            )

    async def _run_sender(self) -> None:
        queue = self._queue
        if queue is None:
            return
        batch: list[JSONValue] = []

        while True:
            payload = await queue.get()
            try:
                if payload is self._STOP:
                    if batch:
                        await self._send_payload(self._build_payload(batch))
                    return
                if not isinstance(payload, dict):
                    continue

                batch.append(payload)
                if len(batch) >= self.batch_size:
                    await self._send_payload(self._build_payload(batch))
                    batch.clear()
            finally:
                queue.task_done()

    async def _send_payload(self, payload: JSONValue | object) -> None:
        client = self._client
        if client is None or payload is self._STOP:
            return

        method_upper = self.method.upper()
        if not hasattr(Method, method_upper):  # type: ignore[attr-defined]
            raise ValueError(
                f"Invalid HTTP method '{self.method}'. Must be one of: GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS",
            )

        method = getattr(Method, method_upper)  # type: ignore[attr-defined]
        request_kwargs: dict[str, object] = {
            "headers": self.headers,
            "json": payload,
        }

        timeout = _timeout_seconds(self.timeout)
        if timeout is not None:
            request_kwargs["timeout"] = timedelta(seconds=timeout)

        response = None
        try:
            response = await cast(Any, client).request(
                method, self.url, **request_kwargs
            )
            status = getattr(response, "status", None)
            self.logger.debug(
                "Streamed telemetry event",
                url=self.url,
                status=status,
                event_type=payload.get("event") if isinstance(payload, dict) else None,
            )
        except Exception as exc:
            self.logger.warning(
                "Failed to stream telemetry event",
                url=self.url,
                error=str(exc),
                error_type=exc.__class__.__name__,
                exc_info=True,
            )
        finally:
            if response is not None:
                closer = getattr(response, "aclose", None) or getattr(
                    response,
                    "close",
                    None,
                )
                if closer and callable(closer):
                    result = closer()
                    if hasattr(result, "__await__"):
                        await result  # type: ignore[misc]

    def _build_payload(self, events: list[JSONValue]) -> JSONValue:
        if len(events) == 1:
            return events[0]
        return {
            self.batch_envelope_key: list(events),
            "count": len(events),
        }

    def _request_event(
        self,
        request: Request,
        spider: Spider,
        exchange_id: str,
        previous_exchange_id: JSONValue,
    ) -> dict[str, JSONValue]:
        return {
            "event": "request",
            "exchange_id": exchange_id,
            "parent_exchange_id": (
                previous_exchange_id if isinstance(previous_exchange_id, str) else None
            ),
            "timestamp": _utc_timestamp(),
            "spider": spider.name,
            "request": self._serialize_request(request),
        }

    def _response_event(
        self,
        response: Response,
        spider: Spider,
        exchange_id: str,
    ) -> dict[str, JSONValue]:
        return {
            "event": "response",
            "exchange_id": exchange_id,
            "parent_exchange_id": self._parent_exchange_id(response.request),
            "timestamp": _utc_timestamp(),
            "spider": spider.name,
            "duration_ms": self._duration_ms(response.request),
            "request": self._serialize_request(response.request),
            "response": self._serialize_response(response),
        }

    def _parent_exchange_id(self, request: Request) -> str | None:
        parent = request.meta.get(self._PARENT_EXCHANGE_ID_META_KEY)
        return parent if isinstance(parent, str) else None

    def _duration_ms(self, request: Request) -> float | None:
        started_at = request.meta.get(self._STARTED_AT_META_KEY)
        if not isinstance(started_at, (int, float)):
            return None
        return round((time.perf_counter() - float(started_at)) * 1000, 3)

    def _serialize_request(self, request: Request) -> dict[str, JSONValue]:
        return {
            "url": request.url,
            "method": request.method,
            "headers": dict(request.headers),
            "params": self._serialize_params(request.params),
            "body": self._serialize_request_body(request),
            "meta": self._serialize_meta(request.meta),
            "timeout_seconds": _timeout_seconds(request.timeout),
            "callback": _callback_name(request.callback),
            "dont_filter": request.dont_filter,
            "priority": request.priority,
        }

    def _serialize_response(self, response: Response) -> dict[str, JSONValue]:
        return {
            "url": response.url,
            "status": response.status,
            "headers": dict(response.headers),
            "body": self._serialize_bytes(response.body),
            "encoding": response.encoding,
        }

    def _serialize_request_body(self, request: Request) -> JSONValue:
        if request.json is not None:
            return {
                "kind": "json",
                "value": request.json,
            }

        body = request.data
        if body is None:
            return None
        if isinstance(body, str):
            return self._serialize_text(body)
        if isinstance(body, Mapping):
            return {
                "kind": "mapping",
                "value": self._serialize_mapping(cast(Mapping[object, object], body)),
            }
        if isinstance(body, (bytes, bytearray, memoryview)):
            return self._serialize_bytes(bytes(body))

        try:
            items = list(body)
        except TypeError:
            return {"kind": "repr", "value": repr(body)}

        serialized_items: list[JSONValue] = []
        for item in items:
            if isinstance(item, tuple) and len(item) == 2:
                key, value = item
                serialized_items.append([str(key), str(value)])
            else:
                serialized_items.append(str(item))

        return {
            "kind": "sequence",
            "value": serialized_items,
        }

    def _serialize_params(self, params: Mapping[str, object]) -> dict[str, JSONValue]:
        return {key: self._coerce_json_value(value) for key, value in params.items()}

    def _serialize_meta(self, meta: Mapping[str, JSONValue]) -> dict[str, JSONValue]:
        return {
            key: value
            for key, value in meta.items()
            if key
            not in {
                self._EXCHANGE_ID_META_KEY,
                self._PARENT_EXCHANGE_ID_META_KEY,
                self._STARTED_AT_META_KEY,
            }
        }

    def _serialize_mapping(
        self, mapping: Mapping[object, object]
    ) -> dict[str, JSONValue]:
        return {
            str(key): self._coerce_json_value(value) for key, value in mapping.items()
        }

    def _coerce_json_value(self, value: object) -> JSONValue:
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        if isinstance(value, Mapping):
            return self._serialize_mapping(value)
        if isinstance(value, (bytes, bytearray, memoryview)):
            return self._serialize_bytes(bytes(value))
        if isinstance(value, list):
            return [self._coerce_json_value(item) for item in value]
        if isinstance(value, tuple):
            return [self._coerce_json_value(item) for item in value]
        if isinstance(value, set):
            return [self._coerce_json_value(item) for item in value]
        return repr(value)

    def _serialize_text(self, value: str) -> dict[str, JSONValue]:
        encoded = value.encode("utf-8", errors="replace")
        payload = self._serialize_bytes(encoded)
        payload["kind"] = "text"
        return payload

    def _serialize_bytes(self, body: bytes) -> dict[str, JSONValue]:
        truncated = body[: self.max_body_bytes]
        encoded = base64.b64encode(truncated).decode("ascii")
        return {
            "kind": "base64",
            "base64": encoded,
            "size": len(body),
            "truncated": len(truncated) < len(body),
        }
