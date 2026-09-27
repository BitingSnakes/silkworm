"""URL canonicalization, request fingerprints, and domain matching."""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Mapping
from typing import TYPE_CHECKING, cast
from urllib.parse import parse_qsl, quote, unquote, urlencode, urlsplit, urlunsplit

if TYPE_CHECKING:
    from ._types import QueryValue
    from .request import Request

_DEFAULT_PORTS = {"http": 80, "https": 443, "ws": 80, "wss": 443}
# Characters kept literal in paths; everything else is percent-encoded once.
_PATH_SAFE = "/:@!$&'()*+,;=-._~"
_QUERY_SAFE = "/:@!$'()*,;-._~"


def merge_query_params(url: str, params: Mapping[str, QueryValue]) -> str:
    """Return ``url`` with ``params`` merged over its existing query string."""
    if not params:
        return url
    parts = urlsplit(url)
    merged: dict[str, QueryValue] = dict(parse_qsl(parts.query, keep_blank_values=True))
    merged.update(params)
    query = urlencode(cast("Mapping[str, object]", merged), doseq=True)
    return parts._replace(query=query).geturl()


_ENCODED_SLASH = re.compile("%2f", re.IGNORECASE)


def _normalize_path(path: str) -> str:
    # Decode then re-encode so ``%7E`` and ``~`` produce the same canonical
    # form, but keep encoded slashes: ``a%2Fb`` and ``a/b`` are different paths.
    return "%2F".join(
        quote(unquote(segment), safe=_PATH_SAFE)
        for segment in _ENCODED_SLASH.split(path)
    )


def canonicalize_url(url: str, *, keep_fragments: bool = False) -> str:
    """Return a normalized form of ``url`` for deduplication and caching.

    The scheme and host are lowercased, default ports and (unless
    ``keep_fragments``) fragments are removed, percent-encoding is normalized,
    an empty path becomes ``/``, and query parameters are sorted while keeping
    blank values and repeated keys. Two URLs that address the same resource in
    these respects map to the same string.

    Example:
        >>> canonicalize_url("HTTP://Example.com:80/a?b=2&a=1#top")
        'http://example.com/a?a=1&b=2'
    """
    parts = urlsplit(url.strip())
    scheme = parts.scheme.lower()
    host = (parts.hostname or "").lower()
    if ":" in host:  # IPv6 literal
        host = f"[{host}]"
    netloc = host
    if parts.username is not None:
        userinfo = parts.username
        if parts.password is not None:
            userinfo = f"{userinfo}:{parts.password}"
        netloc = f"{userinfo}@{netloc}"
    try:
        port = parts.port
    except ValueError:
        port = None
    if port is not None and port != _DEFAULT_PORTS.get(scheme):
        netloc = f"{netloc}:{port}"

    path = _normalize_path(parts.path) or "/"
    query_pairs = sorted(parse_qsl(parts.query, keep_blank_values=True))
    query = urlencode(query_pairs, quote_via=quote, safe=_QUERY_SAFE)
    fragment = parts.fragment if keep_fragments else ""
    return urlunsplit((scheme, netloc, path, query, fragment))


def _body_bytes(request: Request) -> bytes:
    if request.json is not None:
        return json.dumps(request.json, sort_keys=True, separators=(",", ":")).encode()
    data = request.data
    if data is None:
        return b""
    if isinstance(data, (bytes, bytearray, memoryview)):
        return bytes(data)
    if isinstance(data, str):
        return data.encode()
    if isinstance(data, Mapping):
        return json.dumps(dict(data), sort_keys=True, default=str).encode()
    return json.dumps(
        [list(pair) if isinstance(pair, tuple) else pair for pair in data],
        default=str,
    ).encode()


def request_fingerprint(request: Request) -> str:
    """Return a stable hex digest identifying what ``request`` fetches.

    The fingerprint covers the HTTP method, the canonical URL with
    :attr:`~silkworm.Request.params` merged in, and the request body (form
    data or JSON). Headers, metadata, callbacks, and URL fragments are ignored,
    so two requests with the same fingerprint are treated as duplicates by the
    engine's default deduplicator and share HTTP cache entries.
    """
    url = canonicalize_url(merge_query_params(request.url, request.params))
    digest = hashlib.sha1(usedforsecurity=False)
    digest.update(request.method.upper().encode())
    digest.update(b"\0")
    digest.update(url.encode())
    digest.update(b"\0")
    digest.update(_body_bytes(request))
    return digest.hexdigest()


def normalize_domains(domains: Iterable[str]) -> tuple[str, ...]:
    """Normalize domain names for :func:`host_in_domains` matching."""
    normalized: list[str] = []
    for domain in domains:
        value = domain.strip().lower().lstrip(".")
        if "://" in value:
            value = urlsplit(value).hostname or ""
        value = value.split("/", 1)[0].rsplit(":", 1)[0] if value else value
        if value:
            normalized.append(value)
    return tuple(dict.fromkeys(normalized))


def host_in_domains(host: str, domains: tuple[str, ...]) -> bool:
    """Return whether ``host`` equals or is a subdomain of any of ``domains``."""
    host = host.lower().rstrip(".")
    return any(host == domain or host.endswith(f".{domain}") for domain in domains)
