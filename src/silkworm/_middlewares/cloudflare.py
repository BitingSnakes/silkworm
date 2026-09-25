from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Mapping
from typing import TYPE_CHECKING

from .._types import JSONValue
from ..exceptions import HttpError
from ..http import MOCK_RESPONSE_META_KEY, HttpClient
from ..logging import get_logger
from ..request import Request

if TYPE_CHECKING:
    from ..spiders import Spider


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
    _DONE_STATES = frozenset({"completed", "complete", "done", "finished", "success"})
    _FAILED_STATES = frozenset({"cancelled", "canceled", "error", "failed"})

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

    async def close(self, spider: Spider) -> None:
        await self._client.close()

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
