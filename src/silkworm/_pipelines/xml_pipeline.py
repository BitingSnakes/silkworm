from __future__ import annotations

import io
import rxml
from pathlib import Path
from typing import TYPE_CHECKING

from ..logging import get_logger
from .base import _log_pipeline_item

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class XMLPipeline:
    def __init__(
        self,
        path: str | Path = "items.xml",
        *,
        root_element: str = "items",
        item_element: str = "item",
    ) -> None:
        self.path = Path(path)
        self.root_element = root_element
        self.item_element = item_element
        self._fp: io.TextIOWrapper | None = None
        self.logger = get_logger(component="XMLPipeline")

    async def open(self, spider: Spider) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fp = self.path.open("w", encoding="utf-8")
        self._fp.write(
            f'<?xml version="1.0" encoding="UTF-8"?>\n<{self.root_element}>\n',
        )
        self._fp.flush()
        self.logger.info("Opened XML pipeline", path=str(self.path))

    async def close(self, spider: Spider) -> None:
        if self._fp:
            self._fp.write(f"</{self.root_element}>\n")
            self._fp.close()
            self._fp = None
            self.logger.info("Closed XML pipeline", path=str(self.path))

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        if not self._fp:
            raise RuntimeError("XMLPipeline not opened")

        node = self._to_node(self.item_element, item)
        xml_str = rxml.write_string(node, indent=2, default_xml_def=False)
        indented_xml = "\n".join(f"  {line}" for line in xml_str.splitlines())

        self._fp.write(indented_xml + "\n")
        self._fp.flush()
        _log_pipeline_item(
            self, "Wrote item to XML", path=str(self.path), spider=spider.name
        )
        return item

    def _to_node(self, key: str, data: JSONValue) -> rxml.Node:
        """Convert a Python structure to an rxml Node tree."""
        tag = self._sanitize_tag(key)

        if isinstance(data, dict):
            children = [self._to_node(k, v) for k, v in data.items()]
            return rxml.Node(tag, children=children)

        if isinstance(data, list):
            children = [self._to_node("item", item) for item in data]
            return rxml.Node(tag, children=children)

        text = "" if data is None else str(data)
        return rxml.Node(tag, text=text)

    @staticmethod
    def _sanitize_tag(tag: object) -> str:
        """Make sure the tag name is XML-safe."""
        return str(tag).replace(" ", "_").replace("-", "_")
