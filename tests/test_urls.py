"""Regression tests for query merging, crawl scope, and body fingerprints."""

from __future__ import annotations

from urllib.parse import parse_qsl, urlsplit

import pytest

from silkworm import Request, request_fingerprint
from silkworm._urls import host_in_domains, merge_query_params, normalize_domains
from silkworm.http import HttpClient


def test_query_merge_preserves_unrelated_repeated_and_blank_values() -> None:
    request = Request(
        url="https://x.test/?tag=a&tag=b&empty=&page=1&page=2#section",
        params={"page": 3, "lang": ["uk", "en"]},
    )
    merged = HttpClient()._build_url(request)
    assert parse_qsl(urlsplit(merged).query, keep_blank_values=True) == [
        ("tag", "a"),
        ("tag", "b"),
        ("empty", ""),
        ("page", "3"),
        ("lang", "uk"),
        ("lang", "en"),
    ]
    assert urlsplit(merged).fragment == "section"


def test_query_merge_can_remove_an_overridden_key_with_an_empty_sequence() -> None:
    assert merge_query_params("https://x.test/?tag=a&tag=b&q=keep", {"tag": []}) == (
        "https://x.test/?q=keep"
    )


def test_fingerprint_distinguishes_repeated_query_values_when_merging() -> None:
    repeated = Request(url="https://x.test/?tag=a&tag=b", params={"page": 2})
    single = Request(url="https://x.test/?tag=b", params={"page": 2})
    assert request_fingerprint(repeated) != request_fingerprint(single)
    assert request_fingerprint(repeated) == request_fingerprint(
        Request(url="https://x.test/?page=2&tag=b&tag=a")
    )


@pytest.mark.parametrize(
    ("domain", "host"),
    [
        ("https://[::1]:8080/path", "::1"),
        ("[::1]:8080", "::1"),
        ("[::1]", "::1"),
        ("::1", "::1"),
        ("2001:db8::1", "2001:db8::1"),
        (".Example.COM.:8080/path", "example.com"),
        ("https://Example.COM.:8080/path", "example.com"),
        ("127.0.0.1:8080", "127.0.0.1"),
    ],
)
def test_normalize_domains_matches_the_url_host(domain: str, host: str) -> None:
    domains = normalize_domains([domain])
    assert domains == (host,)
    assert host_in_domains(host, domains)
    assert not host_in_domains("other.test", domains)


def test_normalize_domains_removes_empty_and_duplicate_entries() -> None:
    assert normalize_domains(["", " . ", "Example.COM", "example.com."]) == (
        "example.com",
    )


@pytest.mark.parametrize(
    "data",
    [
        b"abcdef",
        bytearray(b"abcdef"),
        memoryview(b"abcdef"),
        memoryview(b"abcdef")[1:5],
        memoryview(b"abcdef")[::2],
        memoryview(b"abcdef").cast("B", shape=[2, 3]),
        memoryview(bytearray(b"abcdef")).toreadonly(),
    ],
)
def test_fingerprint_buffer_matches_its_bytes(
    data: bytes | bytearray | memoryview,
) -> None:
    request = Request(url="https://x.test/", method="POST", data=data)
    assert request_fingerprint(request) == request_fingerprint(
        request.replace(data=bytes(data))
    )
