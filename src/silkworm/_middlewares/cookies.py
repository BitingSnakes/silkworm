from __future__ import annotations

import asyncio
import re
from collections.abc import Mapping, Sequence
from email.message import Message
from http.cookiejar import Cookie, CookieJar, DefaultCookiePolicy, MozillaCookieJar
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from urllib.parse import urlsplit

from ..logging import Logger, get_logger
from ..request import Request
from ..response import Response

if TYPE_CHECKING:
    from collections.abc import Iterable

    from ..spiders import Spider


class CookiesMiddleware:
    """
    Stateful cookie middleware using Python's standards-compliant CookieJar.

    By default all requests share one cookie jar. Set `request.meta["cookiejar"]`
    to a string or integer to isolate sessions, set `request.meta["cookies"]` to
    add per-request cookies, and set `request.meta["dont_merge_cookies"] = True`
    to bypass cookie handling for a request/response pair.

    Args:
        cookies: Initial cookies placed in the default jar.
        enabled: Whether request and response cookie processing is active.
        allow_domains: Optional domain allowlist passed to the cookie policy.
        block_domains: Optional domain blocklist passed to the cookie policy.
        rfc2965: Enable RFC 2965 cookie handling in addition to Netscape cookies.
        hide_cookie_header: Hide generated cookie headers from redirect handling
            performed by the underlying HTTP client.
    """

    _COOKIEJAR_META_KEY = "cookiejar"
    _COOKIES_META_KEY = "cookies"
    _DONT_MERGE_META_KEY = "dont_merge_cookies"
    _DEFAULT_JAR_KEY = "default"
    _COOKIE_PAIR_RE = re.compile(r"^[!#$%&'*+\-.^_`|~0-9A-Za-z]+=")

    def __init__(
        self,
        *,
        cookies: Mapping[str, object] | None = None,
        enabled: bool = True,
        allow_domains: Iterable[str] | None = None,
        block_domains: Iterable[str] | None = None,
        rfc2965: bool = False,
        hide_cookie_header: bool = True,
    ) -> None:
        self.enabled = enabled
        self.hide_cookie_header = hide_cookie_header
        self._policy = DefaultCookiePolicy(
            allowed_domains=self._normalize_domain_list(allow_domains),
            blocked_domains=self._normalize_domain_list(block_domains),
            rfc2965=rfc2965,
        )
        self._jars: dict[str | int, CookieJar] = {}
        self._lock = asyncio.Lock()
        self.logger: Logger = get_logger(component="CookiesMiddleware")

        if cookies:
            jar = self._jar_for(self._DEFAULT_JAR_KEY)
            for name, value in cookies.items():
                if value is None:
                    continue
                jar.set_cookie(
                    self._make_cookie(
                        name=str(name),
                        value=str(value),
                        domain="",
                        path="/",
                        secure=False,
                    ),
                )

    async def process_request(self, request: Request, spider: Spider) -> Request:
        """Merge explicit and stored cookies into the outgoing request.

        The request is returned unchanged when cookie handling is disabled or
        ``dont_merge_cookies`` metadata is true.
        """
        if not self.enabled or self._dont_merge(request):
            return request

        async with self._lock:
            jar = self._jar_for_request(request)
            self._store_request_cookies(jar, request)
            cookie_request = _CookieRequest(
                request, hide_cookie=self.hide_cookie_header
            )
            cast(Any, jar).add_cookie_header(cookie_request)
            cookie_request.apply()

        if self._has_cookie_header(request.headers):
            self.logger.debug("Applied cookies", url=request.url)
        return request

    async def process_response(
        self,
        response: Response,
        spider: Spider,
    ) -> Response | Request:
        """Store response ``Set-Cookie`` values in the selected cookie jar."""
        if not self.enabled or self._dont_merge(response.request):
            return response

        set_cookie_headers = list(self._iter_set_cookie_headers(response.headers))
        if not set_cookie_headers:
            return response

        async with self._lock:
            jar = self._jar_for_request(response.request)
            cast(Any, jar).extract_cookies(
                _CookieResponse(set_cookie_headers),
                _CookieRequest(response.request, hide_cookie=False),
            )

        self.logger.debug(
            "Stored response cookies",
            url=response.url,
            count=len(set_cookie_headers),
        )
        return response

    def clear(self, cookiejar: str | int | None = None) -> None:
        """Clear all cookies, or only the named cookie jar."""
        if cookiejar is None:
            self._jars.clear()
            return
        self._jars.pop(cookiejar, None)

    def clear_session_cookies(self, cookiejar: str | int | None = None) -> None:
        """Discard session cookies from all jars, or a single named jar."""
        if cookiejar is not None:
            jar = self._jars.get(cookiejar)
            if jar is not None:
                jar.clear_session_cookies()
            return

        for jar in self._jars.values():
            jar.clear_session_cookies()

    def set_cookie(
        self,
        name: str,
        value: str,
        *,
        domain: str,
        path: str = "/",
        secure: bool = False,
        expires: int | None = None,
        cookiejar: str | int | None = None,
    ) -> None:
        """Add or replace one cookie in the selected jar."""
        self._jar_for(cookiejar or self._DEFAULT_JAR_KEY).set_cookie(
            self._make_cookie(
                name=name,
                value=value,
                domain=domain,
                path=path,
                secure=secure,
                expires=expires,
            ),
        )

    def save(
        self,
        path: str | Path,
        *,
        cookiejar: str | int | None = None,
        ignore_discard: bool = True,
        ignore_expires: bool = True,
    ) -> None:
        """Save cookies from one jar to a Netscape/Mozilla cookie file."""
        output_path = Path(path)
        output_path.parent.mkdir(parents=True, exist_ok=True)

        file_jar = MozillaCookieJar(str(output_path))
        file_jar.set_policy(self._policy)
        for cookie in self._jar_for(cookiejar or self._DEFAULT_JAR_KEY):
            file_jar.set_cookie(cookie)
        file_jar.save(
            str(output_path),
            ignore_discard=ignore_discard,
            ignore_expires=ignore_expires,
        )

    def load(
        self,
        path: str | Path,
        *,
        cookiejar: str | int | None = None,
        ignore_discard: bool = True,
        ignore_expires: bool = True,
        clear_existing: bool = False,
    ) -> None:
        """Load cookies from a Netscape/Mozilla cookie file into one jar."""
        input_path = Path(path)
        file_jar = MozillaCookieJar(str(input_path))
        file_jar.set_policy(self._policy)
        file_jar.load(
            str(input_path),
            ignore_discard=ignore_discard,
            ignore_expires=ignore_expires,
        )

        target = self._jar_for(cookiejar or self._DEFAULT_JAR_KEY)
        if clear_existing:
            target.clear()
        for cookie in file_jar:
            target.set_cookie(cookie)

    def _jar_for_request(self, request: Request) -> CookieJar:
        raw_key = request.meta.get(self._COOKIEJAR_META_KEY, self._DEFAULT_JAR_KEY)
        key: str | int
        if isinstance(raw_key, int) or isinstance(raw_key, str) and raw_key:
            key = raw_key
        else:
            key = self._DEFAULT_JAR_KEY
        return self._jar_for(key)

    def _jar_for(self, key: str | int) -> CookieJar:
        jar = self._jars.get(key)
        if jar is None:
            jar = CookieJar(policy=self._policy)
            self._jars[key] = jar
        return jar

    def _store_request_cookies(self, jar: CookieJar, request: Request) -> None:
        raw_cookies = request.meta.get(self._COOKIES_META_KEY)
        if not isinstance(raw_cookies, Mapping):
            return

        split = urlsplit(request.url)
        host = split.hostname or ""
        path = self._default_cookie_path(split.path)
        secure = split.scheme.lower() == "https"
        for name, value in raw_cookies.items():
            if not isinstance(name, str) or value is None:
                continue
            jar.set_cookie(
                self._make_cookie(
                    name=name,
                    value=str(value),
                    domain=host,
                    path=path,
                    secure=secure,
                ),
            )

    def _iter_set_cookie_headers(
        self,
        headers: Mapping[str, object],
    ) -> Iterable[str]:
        for key, raw_value in headers.items():
            if str(key).lower() != "set-cookie":
                continue
            if isinstance(raw_value, (list, tuple)):
                for value in cast("Sequence[object]", raw_value):
                    yield from self._split_set_cookie_header(str(value))
            else:
                yield from self._split_set_cookie_header(str(raw_value))

    def _split_set_cookie_header(self, value: str) -> Iterable[str]:
        value = value.strip()
        if not value:
            return []

        parts: list[str] = []
        start = 0
        for index, char in enumerate(value):
            if char != ",":
                continue
            candidate = value[index + 1 :].lstrip()
            if self._COOKIE_PAIR_RE.match(candidate):
                parts.append(value[start:index].strip())
                start = index + 1
        parts.append(value[start:].strip())
        return [part for part in parts if part]

    def _make_cookie(
        self,
        *,
        name: str,
        value: str,
        domain: str,
        path: str,
        secure: bool,
        expires: int | None = None,
    ) -> Cookie:
        domain_specified = bool(domain)
        return Cookie(
            version=0,
            name=name,
            value=value,
            port=None,
            port_specified=False,
            domain=domain,
            domain_specified=domain_specified,
            domain_initial_dot=domain.startswith("."),
            path=path or "/",
            path_specified=True,
            secure=secure,
            expires=expires,
            discard=expires is None,
            comment=None,
            comment_url=None,
            rest={},
            rfc2109=False,
        )

    def _dont_merge(self, request: Request) -> bool:
        return request.meta.get(self._DONT_MERGE_META_KEY) is True

    def _has_cookie_header(self, headers: Mapping[str, object]) -> bool:
        return any(str(key).lower() == "cookie" for key in headers)

    def _default_cookie_path(self, path: str) -> str:
        if not path or not path.startswith("/"):
            return "/"
        if path.count("/") <= 1:
            return "/"
        return path.rsplit("/", 1)[0] or "/"

    def _normalize_domain_list(
        self,
        domains: Iterable[str] | None,
    ) -> tuple[str, ...] | None:
        if domains is None:
            return None
        return tuple(domain.strip().lower() for domain in domains if domain.strip())


class _CookieRequest:
    def __init__(self, request: Request, *, hide_cookie: bool) -> None:
        self._request = request
        self._hide_cookie = hide_cookie
        split = urlsplit(request.url)
        self.type = split.scheme
        self.host = split.netloc
        self.origin_req_host = split.hostname or split.netloc
        self.unverifiable = False
        self._headers = dict(request.headers)

    def get_full_url(self) -> str:
        return self._request.url

    def get_host(self) -> str:
        return self.host

    def has_header(self, name: str) -> bool:
        if self._hide_cookie and name.lower() == "cookie":
            return False
        return any(key.lower() == name.lower() for key in self._headers)

    def get_header(self, name: str, default: str | None = None) -> str | None:
        for key, value in self._headers.items():
            if key.lower() == name.lower():
                return value
        return default

    def header_items(self) -> list[tuple[str, str]]:
        return list(self._headers.items())

    def add_unredirected_header(self, name: str, value: str) -> None:
        for key in list(self._headers):
            if key.lower() == name.lower():
                del self._headers[key]
        self._headers[name] = value

    def apply(self) -> None:
        for key in list(self._request.headers):
            if key.lower() == "cookie":
                del self._request.headers[key]
        cookie = self.get_header("Cookie")
        if cookie:
            self._request.headers["Cookie"] = cookie


class _CookieResponse:
    def __init__(self, set_cookie_headers: Iterable[str]) -> None:
        self._message = Message()
        for value in set_cookie_headers:
            self._message.add_header("Set-Cookie", value)

    def info(self) -> Message:
        return self._message
