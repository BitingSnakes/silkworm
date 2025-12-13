import sys
import types
from pathlib import Path
from typing import Any

import pytest

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))


class _DummyLogger:
    def configure(self, **_: Any) -> None:
        return None

    def bind(self, **_: Any) -> "_DummyLogger":
        return self

    def debug(self, *args: Any, **kwargs: Any) -> None:
        return None

    info = warning = error = debug

    def complete(self) -> None:
        return None


class _DummyRnetResponse:
    def __init__(
        self, *, status: int = 200, headers: Any = None, body: bytes | str = b""
    ) -> None:
        self.status = status
        self.headers = headers or {}
        self._body = body

    async def read(self) -> bytes:
        if isinstance(self._body, bytes):
            return self._body
        return str(self._body).encode("utf-8")

    async def text(self) -> str:
        if isinstance(self._body, bytes):
            return self._body.decode("utf-8", errors="replace")
        return str(self._body)


class _DummyClient:
    def __init__(self, emulation: Any = None, **_: Any) -> None:
        self.emulation = emulation
        self.calls: list[tuple[Any, str, dict[str, Any]]] = []
        self.closed = False

    async def request(self, method: Any, url: str, **kwargs: Any) -> _DummyRnetResponse:
        self.calls.append((method, url, kwargs))
        return _DummyRnetResponse()

    async def get(self, url: str, **kwargs: Any) -> _DummyRnetResponse:
        return await self.request("GET", url, **kwargs)

    async def aclose(self) -> None:
        self.closed = True

    async def close(self) -> None:
        self.closed = True


class _DummyEmulation:
    Firefox139 = "Firefox139"


class _DummyMethod:
    GET = "GET"
    POST = "POST"
    PUT = "PUT"
    DELETE = "DELETE"


class _DummyDocument:
    instance_count = 0

    def __init__(self, html: str, *, max_size_bytes: int | None = None) -> None:
        type(self).instance_count += 1
        self.html = html
        self.max_size_bytes = max_size_bytes
        self.closed = False

    def select(self, selector: str):
        return [f"{selector}-match"]

    def find(self, selector: str):
        return f"{selector}-first"

    def xpath(self, xpath: str):
        return [f"{xpath}-match"]

    def xpath_first(self, xpath: str):
        return f"{xpath}-first"

    def close(self) -> None:
        self.closed = True


class _DummyRxmlNode:
    def __init__(
        self,
        tag: str,
        *,
        children: list["_DummyRxmlNode"] | None = None,
        text: str = "",
    ) -> None:
        self.tag = tag
        self.children = children or []
        self.text = text


def _dummy_write_string(
    node: "_DummyRxmlNode",
    *,
    indent: int = 0,  # noqa: ARG001
    default_xml_def: bool = True,  # noqa: ARG001
) -> str:
    content = "".join(
        _dummy_write_string(child, indent=indent, default_xml_def=default_xml_def)
        for child in node.children
    )
    return f"<{node.tag}>{node.text}{content}</{node.tag}>"


# Minimal stub modules so tests don't need real dependencies.
logly_module: Any = types.ModuleType("logly")
logly_module.logger = _DummyLogger()

rnet_module: Any = types.ModuleType("rnet")
rnet_module.Client = _DummyClient
rnet_module.Emulation = _DummyEmulation
rnet_module.Method = _DummyMethod

rxml_module: Any = types.ModuleType("rxml")
rxml_module.Node = _DummyRxmlNode
rxml_module.write_string = _dummy_write_string


async def _dummy_select(doc: Any, selector: str) -> Any:
    return []


async def _dummy_select_first(doc: Any, selector: str) -> Any:
    return None


async def _dummy_xpath(doc: Any, xpath: str) -> Any:
    return []


async def _dummy_xpath_first(doc: Any, xpath: str) -> Any:
    return None


scraper_module: Any = types.ModuleType("scraper_rs")
scraper_module.Document = _DummyDocument
scraper_asyncio_module: Any = types.ModuleType("scraper_rs.asyncio")
scraper_asyncio_module.Document = _DummyDocument
scraper_asyncio_module.select = _dummy_select
scraper_asyncio_module.select_first = _dummy_select_first
scraper_asyncio_module.xpath = _dummy_xpath
scraper_asyncio_module.xpath_first = _dummy_xpath_first

sys.modules["logly"] = logly_module
sys.modules["rnet"] = rnet_module
sys.modules["rxml"] = rxml_module
sys.modules["scraper_rs"] = scraper_module
sys.modules["scraper_rs.asyncio"] = scraper_asyncio_module

# Define the base configuration
backends = [
    pytest.param(("asyncio", {"use_uvloop": False}), id="asyncio"),
]

# Only add uvloop if it is actually installed
try:
    import uvloop

    backends.append(
        pytest.param(("asyncio", {"use_uvloop": True}), id="asyncio+uvloop")
    )
except ImportError:
    print("uvloop not installed; skipping uvloop backend tests")


@pytest.fixture(params=backends)
def anyio_backend(request):
    return request.param
