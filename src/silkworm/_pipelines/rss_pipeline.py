from __future__ import annotations

from collections import deque
from datetime import date, datetime, timezone
from email.utils import format_datetime
import json
import xml.etree.ElementTree as ET
from collections.abc import Mapping
from pathlib import Path
from typing import TYPE_CHECKING

from ..logging import get_logger
from .base import _log_pipeline_item

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class RssPipeline:
    """
    Pipeline that writes items to an RSS 2.0 feed (buffered).

    Items must provide title, link, and description fields (configurable).
    """

    def __init__(
        self,
        path: str | Path = "feed.xml",
        *,
        channel_title: str,
        channel_link: str,
        channel_description: str,
        max_items: int | None = 50,
        item_title_field: str = "title",
        item_link_field: str = "link",
        item_description_field: str = "description",
        item_pub_date_field: str | None = None,
        item_guid_field: str | None = None,
        item_author_field: str | None = None,
    ) -> None:
        if not channel_title or not channel_link or not channel_description:
            raise ValueError(
                "channel_title, channel_link, and channel_description are required",
            )
        if max_items is not None:
            if not isinstance(max_items, int):
                raise TypeError("max_items must be an int or None")
            if max_items < 1:
                raise ValueError("max_items must be at least 1")

        self.path = Path(path)
        self.channel_title = channel_title
        self.channel_link = channel_link
        self.channel_description = channel_description
        self.max_items = max_items
        self.item_title_field = item_title_field
        self.item_link_field = item_link_field
        self.item_description_field = item_description_field
        self.item_pub_date_field = item_pub_date_field
        self.item_guid_field = item_guid_field
        self.item_author_field = item_author_field
        self._items: deque[dict[str, str]] = deque(maxlen=max_items)
        self.logger = get_logger(component="RssPipeline")

    async def open(self, spider: Spider) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._items = deque(maxlen=self.max_items)
        self.logger.info(
            "Opened RSS pipeline",
            path=str(self.path),
            max_items=self.max_items,
        )

    async def close(self, spider: Spider) -> None:
        rss = ET.Element("rss", {"version": "2.0"})
        channel = ET.SubElement(rss, "channel")
        ET.SubElement(channel, "title").text = self.channel_title
        ET.SubElement(channel, "link").text = self.channel_link
        ET.SubElement(channel, "description").text = self.channel_description

        for item in self._items:
            item_el = ET.SubElement(channel, "item")
            ET.SubElement(item_el, "title").text = item["title"]
            ET.SubElement(item_el, "link").text = item["link"]
            ET.SubElement(item_el, "description").text = item["description"]
            if pub_date := item.get("pub_date"):
                ET.SubElement(item_el, "pubDate").text = pub_date
            if guid := item.get("guid"):
                ET.SubElement(item_el, "guid").text = guid
            if author := item.get("author"):
                ET.SubElement(item_el, "author").text = author

        tree = ET.ElementTree(rss)
        ET.indent(tree, space="  ")
        with self.path.open("wb") as fp:
            tree.write(fp, encoding="utf-8", xml_declaration=True)

        self.logger.info(
            "Closed RSS pipeline",
            path=str(self.path),
            items_written=len(self._items),
        )

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        if not isinstance(item, Mapping):
            self.logger.warning(
                "Skipping non-mapping item for RSS feed",
                spider=spider.name,
            )
            return item

        title = self._stringify(item.get(self.item_title_field))
        link = self._stringify(item.get(self.item_link_field))
        description = self._stringify(item.get(self.item_description_field))
        if title is None or link is None or description is None:
            self.logger.warning(
                "Skipping item missing required RSS fields",
                spider=spider.name,
                title_field=self.item_title_field,
                link_field=self.item_link_field,
                description_field=self.item_description_field,
            )
            return item

        rss_item: dict[str, str] = {
            "title": title,
            "link": link,
            "description": description,
        }

        if self.item_pub_date_field:
            pub_date_value = item.get(self.item_pub_date_field)
            pub_date = self._format_pub_date(pub_date_value)
            if pub_date is not None:
                rss_item["pub_date"] = pub_date

        if self.item_guid_field:
            guid = self._stringify(item.get(self.item_guid_field))
            if guid is not None:
                rss_item["guid"] = guid

        if self.item_author_field:
            author = self._stringify(item.get(self.item_author_field))
            if author is not None:
                rss_item["author"] = author

        self._items.append(rss_item)
        _log_pipeline_item(
            self,
            "Buffered item for RSS",
            path=str(self.path),
            spider=spider.name,
        )
        return item

    @staticmethod
    def _stringify(value: JSONValue) -> str | None:
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return json.dumps(value, ensure_ascii=False)
        return str(value)

    @staticmethod
    def _format_pub_date(value: object) -> str | None:
        if value is None:
            return None
        if isinstance(value, datetime):
            dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
            return format_datetime(dt)
        if isinstance(value, date):
            dt = datetime(value.year, value.month, value.day, tzinfo=timezone.utc)
            return format_datetime(dt)
        return str(value)
