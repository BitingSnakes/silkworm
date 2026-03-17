import asyncio
from unittest.mock import AsyncMock

import pytest

import silkworm.response as response_module
from silkworm.exceptions import SelectorError
from silkworm.request import Request
from silkworm.response import HTMLResponse


async def test_select_wraps_selector_errors(monkeypatch):
    async def boom(*args, **kwargs):
        raise RuntimeError("no parent ElemInfo")

    document = type("Document", (), {})()
    document.select = boom
    monkeypatch.setattr(response_module, "parse_async", AsyncMock(return_value=document))

    req = Request(url="http://example.com")
    resp = HTMLResponse(
        url=req.url,
        status=200,
        headers={},
        body=b"<html></html>",
        request=req,
    )

    with pytest.raises(SelectorError) as excinfo:
        await resp.select(".bad")

    assert "CSS selector" in str(excinfo.value)
    assert excinfo.value.__cause__ is not None
    assert "no parent ElemInfo" in str(excinfo.value.__cause__)


async def test_select_propagates_cancelled_error(monkeypatch):
    async def cancel(*args, **kwargs):
        raise asyncio.CancelledError

    document = type("Document", (), {})()
    document.select = cancel
    monkeypatch.setattr(response_module, "parse_async", AsyncMock(return_value=document))

    req = Request(url="http://example.com")
    resp = HTMLResponse(
        url=req.url,
        status=200,
        headers={},
        body=b"<html></html>",
        request=req,
    )

    with pytest.raises(asyncio.CancelledError):
        await resp.select(".still-runs")


async def test_prettify_wraps_scraper_errors(monkeypatch):
    async def boom(*args, **kwargs):
        raise RuntimeError("cannot prettify")

    document = type("Document", (), {})()
    document.prettify = boom
    monkeypatch.setattr(response_module, "parse_async", AsyncMock(return_value=document))

    req = Request(url="http://example.com")
    resp = HTMLResponse(
        url=req.url,
        status=200,
        headers={},
        body=b"<html></html>",
        request=req,
    )

    with pytest.raises(SelectorError) as excinfo:
        await resp.prettify()

    assert "HTML prettify" in str(excinfo.value)
    assert excinfo.value.__cause__ is not None
    assert "cannot prettify" in str(excinfo.value.__cause__)
