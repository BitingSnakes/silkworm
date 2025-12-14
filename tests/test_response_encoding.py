from __future__ import annotations

import codecs

from silkworm.request import Request
from silkworm.response import HTMLResponse, Response


def _make_request() -> Request:
    return Request(url="http://example.com")


def test_response_uses_header_charset():
    original = "\u041f\u0440\u0438\u0432\u0435\u0442"  # "Privet" in Cyrillic
    body = original.encode("windows-1251")
    resp = Response(
        url="http://example.com",
        status=200,
        headers={"content-type": "text/html; charset=windows-1251"},
        body=body,
        request=_make_request(),
    )

    assert resp.text == original
    assert resp.encoding == "windows-1251"


def test_response_detects_meta_charset():
    original = "Price \u00a3 12"
    html = (
        f'<html><head><meta charset="iso-8859-1"></head><body>{original}</body></html>'
    )
    body = html.encode("iso-8859-1")
    resp = HTMLResponse(
        url="http://example.com",
        status=200,
        headers={"content-type": "text/html"},
        body=body,
        request=_make_request(),
    )

    assert original in resp.text
    assert resp.encoding == "iso-8859-1"


def test_response_uses_bom_when_present():
    original = "snowman \u2603"
    body = codecs.BOM_UTF16_LE + original.encode("utf-16-le")
    resp = Response(
        url="http://example.com",
        status=200,
        headers={},
        body=body,
        request=_make_request(),
    )

    assert resp.text == original
    assert resp.encoding == "utf-16"


def test_response_falls_back_to_charset_detection():
    original = "\u041f\u0440\u0438\u0432\u0435\u0442 \u043c\u0438\u0440"
    body = original.encode("cp1251")
    resp = Response(
        url="http://example.com",
        status=200,
        headers={},
        body=body,
        request=_make_request(),
    )

    assert resp.text == original
    assert resp.encoding == "cp1251"
