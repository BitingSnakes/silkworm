from __future__ import annotations

import json
from collections import deque
from collections.abc import Mapping
from datetime import UTC, date, datetime
from email.utils import format_datetime
from pathlib import Path
from typing import TYPE_CHECKING

import rxml

from ..logging import Logger, get_logger
from .base import log_pipeline_item

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class RssPipeline:
    """
    Pipeline that writes items to an RSS 2.0 feed (buffered).

    Items must provide title, link, and description fields (configurable).

    Args:
        path: Destination RSS XML file.
        channel_title: Feed title.
        channel_link: Canonical feed or site URL.
        channel_description: Feed description.
        max_items: Maximum most-recent valid items retained, or ``None`` for no
            limit.
        item_title_field: Mapping key containing each item title.
        item_link_field: Mapping key containing each item URL.
        item_description_field: Mapping key containing each item description.
        item_pub_date_field: Optional publication-date key.
        item_guid_field: Optional GUID key.
        item_author_field: Optional author key.
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

        self.path: Path = Path(path)
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
        self.logger: Logger = get_logger(component="RssPipeline")

    async def open(self, spider: Spider) -> None:
        """Create parent directories and reset the bounded item buffer."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._items = deque(maxlen=self.max_items)
        self.logger.info(
            "Opened RSS pipeline",
            path=str(self.path),
            max_items=self.max_items,
        )

    async def close(self, spider: Spider) -> None:
        """Build and write the RSS 2.0 document from buffered items."""
        item_nodes: list[rxml.Node] = []
        for item in self._items:
            children = [
                rxml.Node("title", text=item["title"]),
                rxml.Node("link", text=item["link"]),
                rxml.Node("description", text=item["description"]),
            ]
            if pub_date := item.get("pub_date"):
                children.append(rxml.Node("pubDate", text=pub_date))
            if guid := item.get("guid"):
                children.append(rxml.Node("guid", text=guid))
            if author := item.get("author"):
                children.append(rxml.Node("author", text=author))
            item_nodes.append(rxml.Node("item", children=children))

        channel = rxml.Node(
            "channel",
            children=[
                rxml.Node("title", text=self.channel_title),
                rxml.Node("link", text=self.channel_link),
                rxml.Node("description", text=self.channel_description),
                *item_nodes,
            ],
        )
        rss = rxml.Node("rss", attrs={"version": "2.0"}, children=[channel])
        rxml.write_file(rss, str(self.path), indent=2)

        self.logger.info(
            "Closed RSS pipeline",
            path=str(self.path),
            items_written=len(self._items),
        )

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        """Normalize and buffer a mapping containing required RSS fields.

        Non-mapping items and mappings missing required fields are logged and
        returned without being added to the feed.
        """
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
        log_pipeline_item(
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
            dt = value if value.tzinfo else value.replace(tzinfo=UTC)
            return format_datetime(dt)
        if isinstance(value, date):
            dt = datetime(value.year, value.month, value.day, tzinfo=UTC)
            return format_datetime(dt)
        return str(value)
