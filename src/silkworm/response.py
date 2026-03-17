from __future__ import annotations

import asyncio
import codecs
import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, override
from urllib.parse import urljoin

from scraper_rs.asyncio import (  # type: ignore[import-untyped]
    parse as parse_async,
)

from .exceptions import SelectorError

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Iterable

    from scraper_rs.asyncio import AsyncDocument, AsyncElement  # type: ignore[import]

    from .request import Callback, Request


_CHARSET_RE = re.compile(r"charset=([^\s;\"'>]+)", re.I)
_META_CHARSET_RE = re.compile(rb"<meta\s+charset\s*=\s*['\"]?([a-zA-Z0-9._:-]+)", re.I)
_META_CONTENT_TYPE_RE = re.compile(
    rb"<meta\s+http-equiv\s*=\s*['\"]?content-type['\"]?[^>]*charset\s*=\s*['\"]?([a-zA-Z0-9._:-]+)",
    re.I,
)
_XML_DECLARATION_RE = re.compile(
    rb"<\?xml[^>]+encoding\s*=\s*['\"]([a-zA-Z0-9._:-]+)['\"]",
    re.I,
)
_CHARSET_DETECTION_SAMPLE_BYTES = 128 * 1024
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


@dataclass(slots=True)
class Response:
    url: str
    status: int
    headers: dict[str, str]
    body: bytes
    request: Request
    _closed: bool = field(default=False, init=False, repr=False, compare=False)
    _decoded_text: str | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )
    _detected_encoding: str | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
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

    def url_join(self, href: str) -> str:
        return urljoin(self.url, href)

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

        # Charset detection is heuristic; a bounded prefix is typically enough and
        # avoids building large analyzer state for multi-megabyte payloads.
        sample = (
            body[:_CHARSET_DETECTION_SAMPLE_BYTES]
            if len(body) > _CHARSET_DETECTION_SAMPLE_BYTES
            else body
        )

        try:
            matches = from_bytes(sample)
        except Exception:
            return None

        best_scores: dict[str, float] = {}

        for match in matches:
            encoding = self._normalize_encoding(getattr(match, "encoding", None))
            if encoding is None:
                continue

            alphabets = getattr(match, "alphabets", None) or ()
            language = str(getattr(match, "language", "") or "").lower()

            score = 0.0
            # Bias toward common web charsets and away from console-focused code pages.
            if encoding.startswith("utf-"):
                score += 5.0
            if encoding in _PREFERRED_WEB_ENCODINGS:
                score += 2.0
            if language and language != "unknown":
                score += 0.5
            if any(
                isinstance(alphabet, str) and "cyrillic" in alphabet.lower()
                for alphabet in alphabets
            ):
                score += 0.5
            if any(
                isinstance(alphabet, str) and "box drawing" in alphabet.lower()
                for alphabet in alphabets
            ):
                score -= 1.5

            previous = best_scores.get(encoding)
            if previous is None or score > previous:
                best_scores[encoding] = score

        for encoding, _score in sorted(
            best_scores.items(),
            key=lambda entry: entry[1],
            reverse=True,
        ):
            decoded = self._try_decode(body, encoding)
            if decoded is not None:
                return decoded

        return None

    def follow(
        self,
        href: str,
        callback: Callback | None = None,
        **kwargs: object,
    ) -> Request:
        from .request import Request  # local import to avoid cycle

        url = self.url_join(href)
        return Request(
            url=url,
            callback=callback or self.request.callback,
            **kwargs,  # type: ignore[arg-type]
        )

    def follow_all(
        self,
        hrefs: Iterable[str | None],
        callback: Callback | None = None,
        **kwargs: object,
    ) -> list[Request]:
        return [
            self.follow(href, callback=callback, **kwargs)
            for href in hrefs
            if href is not None
        ]

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
    _document: AsyncDocument | None = field(
        default=None,
        init=False,
        repr=False,
        compare=False,
    )
    _document_lock: asyncio.Lock = field(
        default_factory=asyncio.Lock,
        init=False,
        repr=False,
        compare=False,
    )

    async def _get_document(self) -> AsyncDocument:
        if self._document is not None:
            return self._document

        async with self._document_lock:
            if self._document is None:
                self._document = await self._run_parse(
                    self.text,
                )
        return self._document

    async def _run_parse(self, html: str) -> AsyncDocument:
        return await self._run_document_op(
            parse_async,
            html,
            kind="HTML parse",
            include_max_size=True,
        )

    async def _run_document_op[T](
        self,
        func: Callable[..., Awaitable[T]],
        *args: object,
        kind: str,
        label: str | None = None,
        include_max_size: bool = False,
    ) -> T:
        try:
            kwargs = (
                {"max_size_bytes": self.doc_max_size_bytes} if include_max_size else {}
            )
            return await func(*args, **kwargs)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            detail = str(exc)
            suffix = f": {detail}" if detail else ""
            if label is None:
                raise SelectorError(
                    f"{kind} failed for {self.url}{suffix}",
                ) from exc
            raise SelectorError(
                f"{kind} '{label}' failed for {self.url}{suffix}",
            ) from exc

    async def _run_selector[T](
        self,
        func: Callable[..., Awaitable[T]],
        query: str,
        *,
        kind: str,
    ) -> T:
        label = query if len(query) <= 120 else f"{query[:120]}...(truncated)"
        return await self._run_document_op(
            func,
            query,
            kind=f"{kind} selector",
            label=label,
        )

    async def select(self, selector: str) -> list[AsyncElement]:
        document = await self._get_document()
        return await self._run_selector(document.select, selector, kind="CSS")

    async def select_first(self, selector: str) -> AsyncElement | None:
        document = await self._get_document()
        return await self._run_selector(document.select_first, selector, kind="CSS")

    async def find(self, selector: str) -> AsyncElement | None:
        return await self.select_first(selector)

    async def css(self, selector: str) -> list[AsyncElement]:
        return await self.select(selector)

    async def css_first(self, selector: str) -> AsyncElement | None:
        return await self.select_first(selector)

    async def xpath(self, xpath: str) -> list[AsyncElement]:
        document = await self._get_document()
        return await self._run_selector(document.xpath, xpath, kind="XPath")

    async def xpath_first(self, xpath: str) -> AsyncElement | None:
        document = await self._get_document()
        return await self._run_selector(document.xpath_first, xpath, kind="XPath")

    async def prettify(self) -> str:
        document = await self._get_document()
        return await self._run_document_op(document.prettify, kind="HTML prettify")

    @override
    def follow(
        self,
        href: str,
        callback: Callback | None = None,
        **kwargs: object,
    ) -> Request:
        # Explicit base call avoids zero-arg super issues with slotted dataclasses.
        return Response.follow(self, href, callback=callback, **kwargs)

    @override
    def close(self) -> None:
        """
        Release the underlying AsyncDocument when it is no longer needed.
        """
        if self._closed:
            return

        if self._document is not None:
            self._document.close()
            self._document = None

        # Explicitly call base class to avoid zero-arg super issues with slotted dataclasses.
        Response.close(self)
