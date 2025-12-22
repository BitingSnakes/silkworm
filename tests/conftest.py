import importlib.util
import sys
import types
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, Mock

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


def _mock_logger() -> Mock:
    logger = Mock(name="logly_logger")
    logger.configure.return_value = None
    logger.bind.side_effect = lambda **_: logger
    for method in ("debug", "info", "warning", "error", "complete"):
        getattr(logger, method).return_value = None
    return logger


def _mock_response(
    *, status: int = 200, headers: Any = None, body: bytes | str = b""
) -> Mock:
    data = body if isinstance(body, bytes) else str(body).encode("utf-8")
    resp = Mock()
    resp.status = status
    resp.headers = headers or {}
    resp.read = AsyncMock(return_value=data)
    resp.text = AsyncMock(return_value=data.decode("utf-8", errors="replace"))
    return resp


def _build_client(*, emulation: Any = None, **_: Any) -> Mock:
    client = Mock(name="rnet_client")
    client.emulation = emulation
    calls: list[tuple[Any, str, dict[str, Any]]] = []
    client.calls = calls
    client.closed = False

    async def _request(method: Any, url: str, **kwargs: Any) -> Mock:
        client.calls.append((method, url, kwargs))
        return _mock_response()

    client.request = AsyncMock(side_effect=_request)
    client.get = AsyncMock(
        side_effect=lambda url, **kwargs: client.request("GET", url, **kwargs)
    )

    async def _close() -> None:
        client.closed = True

    client.aclose = AsyncMock(side_effect=_close)
    client.close = AsyncMock(side_effect=_close)
    return client


class _DummyEmulation:
    Firefox139 = "Firefox139"


class _DummyMethod:
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"


def _mock_document(html: str, *, max_size_bytes: int | None = None) -> Mock:
    doc = Mock()
    doc.html = html
    doc.max_size_bytes = max_size_bytes
    doc.closed = False
    doc.select.side_effect = lambda selector: [f"{selector}-match"]
    doc.find.side_effect = lambda selector: f"{selector}-first"
    doc.xpath.side_effect = lambda xpath: [f"{xpath}-match"]
    doc.xpath_first.side_effect = lambda xpath: f"{xpath}-first"
    doc.close.side_effect = lambda: setattr(doc, "closed", True)
    return doc


@dataclass
class _DummyRxmlNode:
    tag: str
    children: list["_DummyRxmlNode"] | None = None
    text: str = ""


def _dummy_write_string(
    node: "_DummyRxmlNode",
    *,
    indent: int = 0,  # noqa: ARG001
    default_xml_def: bool = True,  # noqa: ARG001
) -> str:
    content = "".join(
        _dummy_write_string(child, indent=indent, default_xml_def=default_xml_def)
        for child in (node.children or [])
    )
    return f"<{node.tag}>{node.text}{content}</{node.tag}>"


# Minimal stub modules so tests don't need real dependencies.
logly_module: Any = types.ModuleType("logly")
logly_module.logger = _mock_logger()

rnet_module: Any = types.ModuleType("rnet")
rnet_module.Client = _build_client
rnet_module.Emulation = _DummyEmulation
rnet_module.Method = _DummyMethod

rxml_module: Any = types.ModuleType("rxml")
rxml_module.Node = _DummyRxmlNode
rxml_module.write_string = _dummy_write_string

scraper_module: Any = types.ModuleType("scraper_rs")
scraper_module.__path__ = []  # Allow importing submodules from the mock package.
scraper_module.Document = Mock(side_effect=_mock_document)
scraper_asyncio_module: Any = types.ModuleType("scraper_rs.asyncio")
scraper_asyncio_module.Document = scraper_module.Document
scraper_asyncio_module.select = AsyncMock(return_value=[])
scraper_asyncio_module.select_first = AsyncMock(return_value=None)
scraper_asyncio_module.xpath = AsyncMock(return_value=[])
scraper_asyncio_module.xpath_first = AsyncMock(return_value=None)

sys.modules.update(
    {
        "logly": logly_module,
        "rnet": rnet_module,
        "rxml": rxml_module,
        "scraper_rs": scraper_module,
        "scraper_rs.asyncio": scraper_asyncio_module,
    }
)

# Define the base configuration
backends = [
    pytest.param(("asyncio", {"use_uvloop": False}), id="asyncio"),
]

# Only add uvloop if it is actually installed
if importlib.util.find_spec("uvloop") is not None:
    backends.append(
        pytest.param(("asyncio", {"use_uvloop": True}), id="asyncio+uvloop")
    )
else:
    print("uvloop not installed; skipping uvloop backend tests")


@pytest.fixture(params=backends)
def anyio_backend(request):
    return request.param
