from __future__ import annotations
import asyncio
import codecs
import re
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, override
from urllib.parse import urljoin

from scraper_rs.asyncio import (  # type: ignore[import-untyped]
    select as select_async,
    select_first as select_first_async,
    xpath as xpath_async,
    xpath_first as xpath_first_async,
)

from .exceptions import SelectorError


_CHARSET_RE = re.compile(r"charset=([^\s;\"'>]+)", re.I)
_META_CHARSET_RE = re.compile(rb"<meta\s+charset\s*=\s*['\"]?([a-zA-Z0-9._:-]+)", re.I)
_META_CONTENT_TYPE_RE = re.compile(
    rb"<meta\s+http-equiv\s*=\s*['\"]?content-type['\"]?[^>]*charset\s*=\s*['\"]?([a-zA-Z0-9._:-]+)",
    re.I,
)
_XML_DECLARATION_RE = re.compile(
    rb"<\?xml[^>]+encoding\s*=\s*['\"]([a-zA-Z0-9._:-]+)['\"]", re.I
)
_BOM_SEQUENCE = (
    (codecs.BOM_UTF32_LE, "utf-32"),
    (codecs.BOM_UTF32_BE, "utf-32"),
    (codecs.BOM_UTF16_LE, "utf-16"),
    (codecs.BOM_UTF16_BE, "utf-16"),
    (codecs.BOM_UTF8, "utf-8-sig"),
)
_PREFERRED_WEB_ENCODINGS = {
    "utf-8",
    "utf-8-sig",
    "utf-16",
    "utf-16-be",
    "utf-16-le",
    "utf-32",
    "utf-32-be",
    "utf-32-le",
    "big5",
    "cp1250",
    "cp1251",
    "cp1252",
    "cp1253",
    "cp1254",
    "cp1255",
    "cp1256",
    "cp1257",
    "cp1258",
    "euc-jp",
    "euc-kr",
    "gb18030",
    "gb2312",
    "gbk",
    "ibm866",
    "iso-8859-1",
    "iso-8859-2",
    "iso-8859-3",
    "iso-8859-4",
    "iso-8859-5",
    "iso-8859-6",
    "iso-8859-7",
    "iso-8859-8",
    "iso-8859-10",
    "iso-8859-13",
    "iso-8859-14",
    "iso-8859-15",
    "iso-8859-16",
    "koi8-r",
    "koi8-u",
    "mac_cyrillic",
    "macintosh",
    "shift_jis",
    "windows-1250",
    "windows-1251",
    "windows-1252",
    "windows-1253",
    "windows-1254",
    "windows-1255",
    "windows-1256",
    "windows-1257",
    "windows-1258",
    "windows-874",
}


if TYPE_CHECKING:
    from scraper_rs import Element  # type: ignore[import]
    from .request import Callback, Request


@dataclass(slots=True)
class Response:
    url: str
    status: int
    headers: dict[str, str]
    body: bytes
    request: "Request"
    _closed: bool = field(default=False, init=False, repr=False, compare=False)
    _decoded_text: str | None = field(
        default=None, init=False, repr=False, compare=False
    )
    _detected_encoding: str | None = field(
        default=None, init=False, repr=False, compare=False
    )

    @property
    def text(self) -> str:
        if self._decoded_text is None:
            self._decoded_text, self._detected_encoding = self._decode_text()
        return self._decoded_text

    @property
    def encoding(self) -> str:
        if self._decoded_text is None:
            self._decoded_text, self._detected_encoding = self._decode_text()
        return self._detected_encoding or "utf-8"

    def _decode_text(self) -> tuple[str, str]:
        body = self.body or b""
        if not body:
            return "", self._detected_encoding or "utf-8"

        candidates = [
            self._encoding_from_bom(body),
            self._encoding_from_headers(),
            self._encoding_from_meta(body),
        ]
        for encoding in self._unique_encodings(candidates):
            decoded = self._try_decode(body, encoding)
            if decoded is not None:
                return decoded

        normalized = self._decode_with_charset_normalizer(body)
        if normalized is not None:
            return normalized

        fallback = "utf-8"
        return body.decode(fallback, errors="replace"), fallback

    def _unique_encodings(self, candidates: list[str | None]) -> list[str]:
        seen: set[str] = set()
        unique: list[str] = []
        for enc in candidates:
            normalized = self._normalize_encoding(enc)
            if normalized is None or normalized in seen:
                continue
            seen.add(normalized)
            unique.append(normalized)
        return unique

    def _normalize_encoding(self, encoding: str | bytes | None) -> str | None:
        if encoding is None:
            return None
        if isinstance(encoding, bytes):
            try:
                encoding = encoding.decode("ascii", errors="ignore")
            except Exception:
                return None
        normalized = encoding.strip().strip("\"'").lower()
        normalized = normalized.replace("_", "-")
        if not normalized:
            return None
        try:
            codecs.lookup(normalized)
        except LookupError:
            return None
        return normalized

    def _try_decode(self, body: bytes, encoding: str | None) -> tuple[str, str] | None:
        normalized = self._normalize_encoding(encoding)
        if normalized is None:
            return None
        try:
            return body.decode(normalized), normalized
        except (LookupError, UnicodeDecodeError):
            return None
        except Exception:
            return None

    def _encoding_from_headers(self) -> str | None:
        content_type = self.headers.get("content-type")
        if not content_type:
            return None
        match = _CHARSET_RE.search(content_type)
        if not match:
            return None
        return self._normalize_encoding(match.group(1))

    def _encoding_from_bom(self, body: bytes) -> str | None:
        for bom, encoding in _BOM_SEQUENCE:
            if body.startswith(bom):
                return encoding
        return None

    def _encoding_from_meta(self, body: bytes) -> str | None:
        # Peek at the first few KB to find common charset declarations without
        # decoding the full document.
        head = body[:4096]
        for regex in (_XML_DECLARATION_RE, _META_CHARSET_RE, _META_CONTENT_TYPE_RE):
            match = regex.search(head)
            if match:
                return self._normalize_encoding(match.group(1))
        return None

    def _decode_with_charset_normalizer(self, body: bytes) -> tuple[str, str] | None:
        try:
            from charset_normalizer import from_bytes
        except Exception:
            return None

        try:
            matches = from_bytes(body)
        except Exception:
            return None

        best: tuple[str, str] | None = None
        best_score = float("-inf")

        for match in matches:
            encoding = self._normalize_encoding(getattr(match, "encoding", None))
            if encoding is None:
                continue

            decoded = self._try_decode(body, encoding)
            if decoded is None:
                continue

            alphabets = [
                alphabet.lower()
                for alphabet in getattr(match, "alphabets", []) or []
                if isinstance(alphabet, str)
            ]
            language = str(getattr(match, "language", "") or "").lower()

            score = 0.0
            # Bias toward common web charsets and away from console-focused code pages.
            if encoding.startswith("utf-"):
                score += 5.0
            if encoding in _PREFERRED_WEB_ENCODINGS:
                score += 2.0
            if language and language != "unknown":
                score += 0.5
            if any("cyrillic" in alphabet for alphabet in alphabets):
                score += 0.5
            if any("box drawing" in alphabet for alphabet in alphabets):
                score -= 1.5

            if score > best_score:
                best_score = score
                best = decoded

        return best

    def follow(
        self, href: str, callback: "Callback | None" = None, **kwargs: object
    ) -> "Request":
        from .request import Request  # local import to avoid cycle

        url = urljoin(self.url, href)
        return Request(
            url=url,
            callback=callback or self.request.callback,
            **kwargs,  # type: ignore[arg-type]
        )

    def close(self) -> None:
        """
        Release payload references so responses don't pin memory if they linger.
        """
        if self._closed:
            return

        self._closed = True
        self._decoded_text = None
        self._detected_encoding = None
        self.body = b""
        self.headers.clear()


@dataclass(slots=True)
class HTMLResponse(Response):
    doc_max_size_bytes: int = 5_000_000

    async def _run_selector[T](
        self,
        func: Callable[..., Awaitable[T]],
        query: str,
        *,
        kind: str,
    ) -> T:
        try:
            return await func(self.text, query, max_size_bytes=self.doc_max_size_bytes)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            label = query if len(query) <= 120 else f"{query[:120]}...(truncated)"
            detail = str(exc)
            suffix = f": {detail}" if detail else ""
            raise SelectorError(
                f"{kind} selector '{label}' failed for {self.url}{suffix}"
            ) from exc

    async def select(self, selector: str) -> list[Element]:
        return await self._run_selector(select_async, selector, kind="CSS")

    async def select_first(self, selector: str) -> Element | None:
        return await self._run_selector(select_first_async, selector, kind="CSS")

    async def xpath(self, xpath: str) -> list[Element]:
        return await self._run_selector(xpath_async, xpath, kind="XPath")

    async def xpath_first(self, xpath: str) -> Element | None:
        return await self._run_selector(xpath_first_async, xpath, kind="XPath")

    @override
    def follow(
        self, href: str, callback: "Callback | None" = None, **kwargs: object
    ) -> "Request":
        # Explicit base call avoids zero-arg super issues with slotted dataclasses.
        return Response.follow(self, href, callback=callback, **kwargs)

    @override
    def close(self) -> None:
        """
        Release the underlying Document when it is no longer needed.
        """
        if self._closed:
            return

        # Explicitly call base class to avoid zero-arg super issues with slotted dataclasses.
        Response.close(self)
