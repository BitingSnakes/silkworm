from __future__ import annotations

import random
from pathlib import Path
from typing import TYPE_CHECKING

from .._types import JSONValue
from ..logging import Logger, get_logger
from ..request import Request

if TYPE_CHECKING:
    from collections.abc import Iterable, Sequence

    from ..spiders import Spider


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
            self.proxies: list[str] = list(proxies)

        if not self.proxies:
            msg = "ProxyMiddleware requires at least one proxy."
            raise ValueError(msg)

        self.random_selection = random_selection
        self._idx = 0
        self.logger: Logger = get_logger(component="ProxyMiddleware")

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
