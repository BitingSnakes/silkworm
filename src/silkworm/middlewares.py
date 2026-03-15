from __future__ import annotations
import asyncio
import json
import random
import time
from collections.abc import Mapping
from enum import Enum, auto
from pathlib import Path
from typing import TYPE_CHECKING, Protocol, assert_never

from ._types import JSONValue
from .exceptions import HttpError
from .http import HttpClient, MOCK_RESPONSE_META_KEY
from .logging import get_logger
from .request import Request
from .response import HTMLResponse, Response

if TYPE_CHECKING:
    from collections.abc import Callable, Iterable, Sequence

    from .spiders import Spider


class RequestMiddleware(Protocol):
    async def process_request(self, request: Request, spider: Spider) -> Request: ...


class ResponseMiddleware(Protocol):
    async def process_response(
        self,
        response: Response,
        spider: Spider,
    ) -> Response | Request: ...


class UserAgentMiddleware:
    def __init__(
        self,
        user_agents: Sequence[str] | None = None,
        *,
        default: str | None = None,
    ) -> None:
        self.user_agents = list(user_agents or [])
        self.default = default or "silkworm/0.1"
        self.logger = get_logger(component="UserAgentMiddleware")

    async def process_request(self, request: Request, spider: Spider) -> Request:
        ua = None
        if self.user_agents:
            ua = random.choice(self.user_agents)
        else:
            ua = self.default
        request.headers.setdefault("User-Agent", ua)
        self.logger.debug("Assigned user agent", user_agent=ua, url=request.url)
        return request


class ProxyMiddleware:
    _FAILED_PROXIES_META_KEY = "_proxy_failed_proxies"
    _PROXY_RETRY_TIMES_META_KEY = "_proxy_retry_times"

    def __init__(
        self,
        proxies: Iterable[str] | None = None,
        proxy_file: str | Path | None = None,
        random_selection: bool = False,
    ) -> None:
        if proxies is not None and proxy_file is not None:
            msg = (
                "Cannot specify both 'proxies' and 'proxy_file'. Use one or the other."
            )
            raise ValueError(msg)
        if proxies is None and proxy_file is None:
            msg = "Must provide either 'proxies' (iterable) or 'proxy_file' (path)."
            raise ValueError(msg)

        if proxy_file is not None:
            proxy_path = Path(proxy_file)
            if not proxy_path.exists():
                msg = f"Proxy file not found: {proxy_file}"
                raise FileNotFoundError(msg)
            with proxy_path.open("r", encoding="utf-8") as f:
                self.proxies = [line.strip() for line in f if line.strip()]
        else:
            # At this point, proxies is guaranteed to be not None due to the check above
            assert proxies is not None
            self.proxies = list(proxies)

        if not self.proxies:
            msg = "ProxyMiddleware requires at least one proxy."
            raise ValueError(msg)

        self.random_selection = random_selection
        self._idx = 0
        self.logger = get_logger(component="ProxyMiddleware")

    async def process_request(self, request: Request, spider: Spider) -> Request:
        proxy = request.meta.get("proxy")
        if isinstance(proxy, str):
            self.logger.debug("Using existing proxy", proxy=proxy, url=request.url)
            return request

        proxy = self._select_proxy()
        if proxy is None:
            return request

        request.meta.setdefault("proxy", proxy)
        self.logger.debug("Assigned proxy", proxy=proxy, url=request.url)
        return request

    async def process_exception(
        self,
        request: Request,
        exception: Exception,
        spider: Spider,
    ) -> Request | None:
        current_proxy = request.meta.get("proxy")
        if not isinstance(current_proxy, str):
            return None

        failed_proxies = self._get_failed_proxies(request)
        failed_proxies.append(current_proxy)
        next_proxy = self._select_proxy(excluded=failed_proxies)
        if next_proxy is None:
            self.logger.warning(
                "No proxy left to retry failed request",
                url=request.url,
                error=str(exception),
                error_type=exception.__class__.__name__,
                failed_proxies=failed_proxies,
            )
            return None

        retry_request = request.replace(
            meta={**request.meta},
            dont_filter=True,
        )
        retry_request.meta["proxy"] = next_proxy
        failed_proxies_meta: list[JSONValue] = [proxy for proxy in failed_proxies]
        retry_request.meta[self._FAILED_PROXIES_META_KEY] = failed_proxies_meta

        retry_raw = retry_request.meta.get(self._PROXY_RETRY_TIMES_META_KEY, 0)
        retry_times = retry_raw if isinstance(retry_raw, int) else 0
        retry_request.meta[self._PROXY_RETRY_TIMES_META_KEY] = retry_times + 1

        self.logger.warning(
            "Retrying failed request with another proxy",
            url=request.url,
            error=str(exception),
            error_type=exception.__class__.__name__,
            old_proxy=current_proxy,
            new_proxy=next_proxy,
            attempt=retry_times + 1,
        )
        return retry_request

    def _select_proxy(self, *, excluded: Sequence[str] = ()) -> str | None:
        available_proxies = [proxy for proxy in self.proxies if proxy not in excluded]
        if not available_proxies:
            return None

        if self.random_selection:
            return random.choice(available_proxies)

        for offset in range(len(self.proxies)):
            candidate_idx = (self._idx + offset) % len(self.proxies)
            proxy = self.proxies[candidate_idx]
            if proxy in excluded:
                continue
            self._idx = (candidate_idx + 1) % len(self.proxies)
            return proxy

        return None

    def _get_failed_proxies(self, request: Request) -> list[str]:
        failed_raw = request.meta.get(self._FAILED_PROXIES_META_KEY, [])
        if not isinstance(failed_raw, list):
            return []
        return [proxy for proxy in failed_raw if isinstance(proxy, str)]


class RetryMiddleware:
    def __init__(
        self,
        max_times: int = 3,
        retry_http_codes: Iterable[int] | None = None,
        backoff_base: float = 0.5,
        sleep_http_codes: Iterable[int] | None = None,
    ) -> None:
        self.max_times = max_times
        base_retry_codes = set(
            retry_http_codes or {500, 502, 503, 504, 522, 524, 408, 429},
        )
        sleep_codes = (
            set(sleep_http_codes)
            if sleep_http_codes is not None
            else set(base_retry_codes)
        )
        # Any code we sleep on should also be retried even if it was not
        # included in retry_http_codes.
        self.retry_http_codes = base_retry_codes | sleep_codes
        self.sleep_http_codes = sleep_codes
        self.backoff_base = backoff_base
        self.logger = get_logger(component="RetryMiddleware")

    async def process_response(
        self,
        response: Response,
        spider: Spider,
    ) -> Response | Request:
        request = response.request
        if response.status not in self.retry_http_codes:
            return response

        retry_raw = request.meta.get("retry_times", 0)
        retry_times = retry_raw if isinstance(retry_raw, int) else 0
        if retry_times >= self.max_times:
            return response  # give up

        retry_times += 1
        request = request.replace(dont_filter=True)
        request.meta["retry_times"] = retry_times

        delay = self.backoff_base * (2 ** (retry_times - 1))
        self.logger.warning(
            "Retrying request",
            url=request.url,
            delay=round(delay, 2),
            attempt=retry_times,
            status=response.status,
        )
        if response.status in self.sleep_http_codes and delay > 0:
            # non-blocking sleep to avoid stalling other concurrent fetches
            await asyncio.sleep(delay)

        return request


class _DelayStrategy(Enum):
    """Internal enum to track which delay strategy is configured."""

    FIXED = auto()
    RANDOM = auto()
    CUSTOM = auto()


class DelayMiddleware:
    """
    Middleware to add configurable delays between requests.

    Supports three delay strategies:
    1. Fixed delay: Always wait the same amount of time
    2. Random delay: Wait a random time between min and max
    3. Custom delay: Use a callable that returns delay duration

    Args:
        delay: Fixed delay in seconds, or None if using delay_func
        min_delay: Minimum delay for random strategy (requires max_delay)
        max_delay: Maximum delay for random strategy (requires min_delay)
        delay_func: Custom callable that returns delay in seconds.
                   Called with (request, spider) and should return float.

    Examples:
        Fixed delay of 1 second:
            DelayMiddleware(delay=1.0)

        Random delay between 0.5 and 2 seconds:
            DelayMiddleware(min_delay=0.5, max_delay=2.0)

        Custom delay function:
            def my_delay(request, spider):
                return 1.0 if "fast" in request.url else 2.0
            DelayMiddleware(delay_func=my_delay)
    """

    def __init__(
        self,
        delay: float | None = None,
        min_delay: float | None = None,
        max_delay: float | None = None,
        delay_func: Callable[[Request, Spider], float] | None = None,
    ) -> None:
        # Validate configuration and determine strategy
        self._delay_func: Callable[[Request, Spider], float] | None = None
        self._min_delay: float | None = None
        self._max_delay: float | None = None
        self._fixed_delay: float | None = None

        if delay_func is not None:
            if delay is not None or min_delay is not None or max_delay is not None:
                msg = "delay_func cannot be used with delay, min_delay, or max_delay"
                raise ValueError(msg)
            self._strategy = _DelayStrategy.CUSTOM
            self._delay_func = delay_func
        elif min_delay is not None or max_delay is not None:
            if delay is not None:
                msg = "Cannot use both delay and min_delay/max_delay"
                raise ValueError(msg)
            if min_delay is None or max_delay is None:
                msg = "Both min_delay and max_delay must be provided"
                raise ValueError(msg)
            if min_delay < 0 or max_delay < 0:
                msg = "min_delay and max_delay must be non-negative"
                raise ValueError(msg)
            if min_delay > max_delay:
                msg = "min_delay must be less than or equal to max_delay"
                raise ValueError(msg)
            self._strategy = _DelayStrategy.RANDOM
            self._min_delay = min_delay
            self._max_delay = max_delay
        elif delay is not None:
            if delay < 0:
                msg = "delay must be non-negative"
                raise ValueError(msg)
            self._strategy = _DelayStrategy.FIXED
            self._fixed_delay = delay
        else:
            msg = "Must provide one of: delay, min_delay/max_delay, or delay_func"
            raise ValueError(msg)

        self.logger = get_logger(component="DelayMiddleware")

    async def process_request(self, request: Request, spider: Spider) -> Request:
        """Calculate and apply delay before processing the request."""
        match self._strategy:
            case _DelayStrategy.CUSTOM:
                assert self._delay_func is not None
                delay = self._delay_func(request, spider)
            case _DelayStrategy.RANDOM:
                assert self._min_delay is not None and self._max_delay is not None
                delay = random.uniform(self._min_delay, self._max_delay)
            case _DelayStrategy.FIXED:
                assert self._fixed_delay is not None
                delay = self._fixed_delay
            case other:
                assert_never(other)

        if delay > 0:
            self.logger.debug(
                "Delaying request",
                url=request.url,
                delay=round(delay, 3),
            )
            await asyncio.sleep(delay)

        return request


class SkipNonHTMLMiddleware:
    """
    Response middleware that drops callbacks for non-HTML payloads.

    It checks the Content-Type header first, then falls back to a quick body
    sniff for "<html". Non-HTML responses keep flowing through the engine but
    execute a no-op callback so spider parse methods are skipped.
    Set `request.meta["allow_non_html"] = True` to bypass filtering for a request
    (useful for XML sitemaps, robots.txt fetches, etc.).
    """

    def __init__(
        self,
        allowed_types: Iterable[str] | None = None,
        sniff_bytes: int = 2048,
    ) -> None:
        if sniff_bytes < 0:
            msg = "sniff_bytes must be non-negative"
            raise ValueError(msg)

        self.allowed_types = [t.lower() for t in (allowed_types or ["html"])]
        self.sniff_bytes = sniff_bytes
        self.logger = get_logger(component="SkipNonHTMLMiddleware")

    async def _skip_response(self, response: Response) -> None:
        return None

    def _looks_like_html(self, response: Response) -> bool:
        if isinstance(response, HTMLResponse):
            return True

        content_type = response.headers.get("content-type", "").lower()
        if any(token in content_type for token in self.allowed_types):
            return True

        if self.sniff_bytes == 0:
            return False

        snippet = response.body[: self.sniff_bytes].lower()
        return b"<html" in snippet

    async def process_response(
        self,
        response: Response,
        spider: Spider,
    ) -> Response | Request:
        # Allow opt-out for requests that intentionally fetch non-HTML content
        if response.request.meta.get("allow_non_html"):
            return response

        if self._looks_like_html(response):
            return response

        self.logger.info(
            "Skipping non-HTML response",
            url=response.url,
            status=response.status,
            content_type=response.headers.get("content-type", "unknown"),
        )
        response.request = response.request.replace(callback=self._skip_response)
        return response


class CloudflareCrawlMiddleware:
    """
    Route opt-in requests through Cloudflare Browser Rendering's crawl API.

    Set `request.meta["cloudflare_crawl"] = True` to crawl a URL with the
    middleware defaults, or assign a dict to provide per-request crawl options.
    The spider callback receives a synthetic JSON `Response` containing the
    final Cloudflare API payload.
    """

    _TRIGGER_META_KEY = "cloudflare_crawl"
    _SKIP_META_KEY = "_cloudflare_crawl_applied"
    _DONE_STATES = {"completed", "complete", "done", "finished", "success"}
    _FAILED_STATES = {"cancelled", "canceled", "error", "failed"}

    def __init__(
        self,
        account_id: str,
        api_token: str,
        *,
        crawl_options: Mapping[str, JSONValue] | None = None,
        api_base_url: str = "https://api.cloudflare.com/client/v4",
        poll_interval: float = 1.0,
        timeout: float = 300.0,
        api_timeout: float = 30.0,
    ) -> None:
        if not account_id.strip():
            msg = "account_id must not be empty"
            raise ValueError(msg)
        if not api_token.strip():
            msg = "api_token must not be empty"
            raise ValueError(msg)
        if poll_interval <= 0:
            msg = "poll_interval must be greater than 0"
            raise ValueError(msg)
        if timeout <= 0:
            msg = "timeout must be greater than 0"
            raise ValueError(msg)
        if api_timeout <= 0:
            msg = "api_timeout must be greater than 0"
            raise ValueError(msg)

        self.account_id = account_id
        self.api_token = api_token
        self.crawl_options = dict(crawl_options or {})
        self.api_base_url = api_base_url.rstrip("/")
        self.poll_interval = poll_interval
        self.timeout = timeout
        self._client = HttpClient(concurrency=1, timeout=api_timeout)
        self.logger = get_logger(component="CloudflareCrawlMiddleware")

    async def process_request(self, request: Request, spider: Spider) -> Request:
        crawl_settings = self._resolve_crawl_settings(request)
        if crawl_settings is None:
            return request

        self.logger.info("Submitting Cloudflare crawl", url=request.url)
        payload = await self._run_crawl(request.url, crawl_settings)
        serialized_payload = json.dumps(payload, ensure_ascii=True)

        meta = dict(request.meta)
        meta[self._SKIP_META_KEY] = True
        meta[MOCK_RESPONSE_META_KEY] = {
            "url": request.url,
            "status": 200,
            "headers": {
                "content-type": "application/json; charset=utf-8",
                "x-silkworm-source": "cloudflare-crawl",
            },
            "body": serialized_payload,
        }

        result = payload.get("result")
        record_count: int | None = None
        if isinstance(result, dict):
            records = result.get("records")
            if isinstance(records, list):
                record_count = len(records)
        self.logger.info(
            "Cloudflare crawl completed",
            url=request.url,
            records=record_count,
        )
        return request.replace(meta=meta)

    def _resolve_crawl_settings(self, request: Request) -> dict[str, JSONValue] | None:
        if request.meta.get(self._SKIP_META_KEY):
            return None

        raw_setting = request.meta.get(self._TRIGGER_META_KEY)
        match raw_setting:
            case True:
                return dict(self.crawl_options)
            case dict() as per_request:
                settings = dict(self.crawl_options)
                settings.update(per_request)
                return settings
            case None | False:
                return None
            case _:
                msg = (
                    "request.meta['cloudflare_crawl'] must be True, False, None, "
                    "or a dict of crawl options"
                )
                raise TypeError(msg)

    async def _run_crawl(
        self,
        url: str,
        crawl_settings: Mapping[str, JSONValue],
    ) -> dict[str, JSONValue]:
        start_payload = {"url": url, **crawl_settings}
        submission = await self._api_request(
            "POST",
            f"/accounts/{self.account_id}/browser-rendering/crawl",
            json_payload=start_payload,
        )
        job_id = self._extract_job_id(submission)
        if job_id is None:
            state = self._extract_job_state(submission)
            if state in self._DONE_STATES:
                return submission

            msg = (
                "Cloudflare crawl submission did not include a recognizable job ID. "
                f"Response keys: {sorted(submission.keys())}"
            )
            raise HttpError(msg)

        deadline = time.monotonic() + self.timeout
        while True:
            status_payload = await self._api_request(
                "GET",
                f"/accounts/{self.account_id}/browser-rendering/crawl/{job_id}?limit=1",
            )
            state = self._extract_job_state(status_payload)
            if state in self._DONE_STATES:
                return await self._api_request(
                    "GET",
                    f"/accounts/{self.account_id}/browser-rendering/crawl/{job_id}",
                )
            if state in self._FAILED_STATES:
                msg = f"Cloudflare crawl job {job_id} failed with state '{state}'"
                raise HttpError(msg)
            if time.monotonic() >= deadline:
                msg = f"Cloudflare crawl job {job_id} timed out after {self.timeout}s"
                raise HttpError(msg)
            await asyncio.sleep(self.poll_interval)

    async def _api_request(
        self,
        method: str,
        path: str,
        *,
        json_payload: Mapping[str, JSONValue] | None = None,
    ) -> dict[str, JSONValue]:
        request = Request(
            url=f"{self.api_base_url}{path}",
            method=method,
            headers={
                "Authorization": f"Bearer {self.api_token}",
                "Content-Type": "application/json",
                "Accept": "application/json",
            },
            json=dict(json_payload) if json_payload is not None else None,
        )
        response = await self._client.fetch(request)
        if response.status >= 400:
            msg = (
                f"Cloudflare crawl API request failed with status {response.status}: "
                f"{response.text[:200]}"
            )
            raise HttpError(msg)

        try:
            payload = json.loads(response.text)
        except json.JSONDecodeError as exc:
            msg = "Cloudflare crawl API returned invalid JSON"
            raise HttpError(msg) from exc

        if not isinstance(payload, dict):
            msg = "Cloudflare crawl API returned a non-object response"
            raise HttpError(msg)
        if payload.get("success") is False:
            errors = payload.get("errors")
            detail = errors if isinstance(errors, list) else payload
            msg = f"Cloudflare crawl API reported an error: {detail}"
            raise HttpError(msg)
        return payload

    def _extract_job_id(self, payload: Mapping[str, JSONValue]) -> str | None:
        candidates = self._mapping_candidates(payload)
        candidates.extend(self._mapping_candidates(payload.get("job")))
        result = payload.get("result")
        if isinstance(result, str) and result:
            return result
        if isinstance(result, (int, float)):
            return str(result)
        if isinstance(result, Mapping):
            candidates.append(result)
            candidates.extend(self._mapping_candidates(result.get("job")))

        for candidate in candidates:
            for key in ("job_id", "jobId", "id"):
                value = candidate.get(key)
                if isinstance(value, str) and value:
                    return value
                if isinstance(value, (int, float)):
                    return str(value)
        return None

    def _extract_job_state(self, payload: Mapping[str, JSONValue]) -> str:
        candidates = self._mapping_candidates(payload)
        result = payload.get("result")
        if isinstance(result, Mapping):
            candidates.append(result)
            candidates.extend(self._mapping_candidates(result.get("state")))
            candidates.extend(self._mapping_candidates(result.get("job")))
        candidates.extend(self._mapping_candidates(payload.get("state")))
        candidates.extend(self._mapping_candidates(payload.get("job")))

        for candidate in candidates:
            for key in ("status", "state"):
                value = candidate.get(key)
                if isinstance(value, str) and value:
                    return value.lower()
        return "pending" if self._extract_job_id(payload) is not None else "completed"

    def _mapping_candidates(
        self,
        value: Mapping[str, JSONValue] | JSONValue | None,
    ) -> list[Mapping[str, JSONValue]]:
        if isinstance(value, Mapping):
            return [value]
        return []
