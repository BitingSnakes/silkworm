from __future__ import annotations
import csv
import io
import orjson
import sqlite3
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Any, Protocol

try:
    from taskiq import AsyncBroker

    TASKIQ_AVAILABLE = True
except ImportError:
    AsyncBroker = None  # type: ignore
    TASKIQ_AVAILABLE = False

if True:
    from .spiders import Spider  # type: ignore
from .logging import get_logger


class ItemPipeline(Protocol):
    async def open(self, spider: "Spider") -> None: ...
    async def close(self, spider: "Spider") -> None: ...
    async def process_item(self, item: Any, spider: "Spider") -> Any: ...


class JsonLinesPipeline:
    def __init__(
        self,
        path: str | Path = "items.jl",
    ) -> None:
        self.path = Path(path)
        self._fp: io.TextIOWrapper | None = None  # type: ignore[name-defined]
        self.logger = get_logger(component="JsonLinesPipeline")

    async def open(self, spider: "Spider") -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fp = self.path.open("a", encoding="utf-8")
        self.logger.info("Opened JSONL pipeline", path=str(self.path))

    async def close(self, spider: "Spider") -> None:
        if self._fp:
            self._fp.close()
            self._fp = None
            self.logger.info("Closed JSONL pipeline", path=str(self.path))

    async def process_item(self, item: Any, spider: "Spider") -> Any:
        if not self._fp:
            raise RuntimeError("JsonLinesPipeline not opened")
        line = orjson.dumps(item).decode("utf-8")
        self._fp.write(line + "\n")
        self._fp.flush()
        self.logger.debug(
            "Wrote item to JSONL", path=str(self.path), spider=spider.name
        )
        return item


class SQLitePipeline:
    def __init__(self, path: str | Path = "items.db", table: str = "items") -> None:
        self.path = Path(path)
        self.table = table
        self._conn: sqlite3.Connection | None = None
        self.logger = get_logger(component="SQLitePipeline")

    async def open(self, spider: "Spider") -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = sqlite3.connect(self.path)
        cur = self._conn.cursor()
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                spider TEXT NOT NULL,
                data   TEXT NOT NULL
            )
            """
        )
        self._conn.commit()
        self.logger.info(
            "Opened SQLite pipeline", path=str(self.path), table=self.table
        )

    async def close(self, spider: "Spider") -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
            self.logger.info("Closed SQLite pipeline", path=str(self.path))

    async def process_item(self, item: Any, spider: "Spider") -> Any:
        if not self._conn:
            raise RuntimeError("SQLitePipeline not opened")
        cur = self._conn.cursor()
        cur.execute(
            f"INSERT INTO {self.table} (spider, data) VALUES (?, ?)",
            (spider.name, orjson.dumps(item)),
        )
        self._conn.commit()
        self.logger.debug("Stored item in SQLite", table=self.table, spider=spider.name)
        return item


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
        self._fp: io.TextIOWrapper | None = None  # type: ignore[name-defined]
        self.logger = get_logger(component="XMLPipeline")

    async def open(self, spider: "Spider") -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fp = self.path.open("w", encoding="utf-8")
        self._fp.write(
            f'<?xml version="1.0" encoding="UTF-8"?>\n<{self.root_element}>\n'
        )
        self._fp.flush()
        self.logger.info("Opened XML pipeline", path=str(self.path))

    async def close(self, spider: "Spider") -> None:
        if self._fp:
            self._fp.write(f"</{self.root_element}>\n")
            self._fp.close()
            self._fp = None
            self.logger.info("Closed XML pipeline", path=str(self.path))

    async def process_item(self, item: Any, spider: "Spider") -> Any:
        if not self._fp:
            raise RuntimeError("XMLPipeline not opened")

        root = ET.Element(self.item_element)
        self._dict_to_xml(item, root)
        xml_str = ET.tostring(root, encoding="unicode")

        self._fp.write(f"  {xml_str}\n")
        self._fp.flush()
        self.logger.debug("Wrote item to XML", path=str(self.path), spider=spider.name)
        return item

    def _dict_to_xml(self, data: Any, parent: ET.Element) -> None:
        """Convert a dictionary to XML elements."""
        if isinstance(data, dict):
            for key, value in data.items():
                # Sanitize key to be a valid XML tag name
                key = str(key).replace(" ", "_").replace("-", "_")
                child = ET.SubElement(parent, key)
                self._dict_to_xml(value, child)
        elif isinstance(data, list):
            for item in data:
                list_item = ET.SubElement(parent, "item")
                self._dict_to_xml(item, list_item)
        else:
            parent.text = str(data) if data is not None else ""


class CSVPipeline:
    def __init__(
        self, path: str | Path = "items.csv", *, fieldnames: list[str] | None = None
    ) -> None:
        self.path = Path(path)
        self.fieldnames = fieldnames
        self._fp: io.TextIOWrapper | None = None
        self._writer: csv.DictWriter | None = None
        self._header_written = False
        self.logger = get_logger(component="CSVPipeline")

    async def open(self, spider: "Spider") -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._fp = self.path.open("w", encoding="utf-8", newline="")
        self._header_written = False
        self.logger.info("Opened CSV pipeline", path=str(self.path))

    async def close(self, spider: "Spider") -> None:
        if self._fp:
            self._fp.close()
            self._fp = None
            self._writer = None
            self.logger.info("Closed CSV pipeline", path=str(self.path))

    async def process_item(self, item: Any, spider: "Spider") -> Any:
        if not self._fp:
            raise RuntimeError("CSVPipeline not opened")

        # Flatten nested structures if item is a dict
        if isinstance(item, dict):
            flat_item = self._flatten_dict(item)
        else:
            flat_item = {"value": str(item)}

        # Initialize writer with fieldnames from first item if not provided
        if not self._writer:
            if self.fieldnames is None:
                self.fieldnames = list(flat_item.keys())
            self._writer = csv.DictWriter(
                self._fp, fieldnames=self.fieldnames, extrasaction="ignore"
            )

        # Write header if first item
        if not self._header_written:
            self._writer.writeheader()
            self._header_written = True

        self._writer.writerow(flat_item)
        self._fp.flush()
        self.logger.debug("Wrote item to CSV", path=str(self.path), spider=spider.name)
        return item

    def _flatten_dict(self, data: Any, parent_key: str = "", sep: str = "_") -> dict:
        """Flatten a nested dictionary structure."""
        items: list[tuple[str, Any]] = []
        if isinstance(data, dict):
            for key, value in data.items():
                new_key = f"{parent_key}{sep}{key}" if parent_key else key
                if isinstance(value, dict):
                    items.extend(self._flatten_dict(value, new_key, sep=sep).items())
                elif isinstance(value, list):
                    # Convert list to comma-separated string
                    items.append((new_key, ", ".join(str(v) for v in value)))
                else:
                    items.append((new_key, value))
        else:
            items.append((parent_key, data))
        return dict(items)


class TaskiqPipeline:
    """
    Pipeline that sends scraped items to a Taskiq broker/queue instead of writing to a file.

    This allows you to process items asynchronously with Taskiq workers, enabling
    distributed processing, retries, and other Taskiq features.

    Example:
        from taskiq import InMemoryBroker
        from silkworm.pipelines import TaskiqPipeline

        broker = InMemoryBroker()

        @broker.task
        async def process_item(item):
            # Your item processing logic here
            print(f"Processing: {item}")

        pipeline = TaskiqPipeline(broker, task=process_item)
        # Or: pipeline = TaskiqPipeline(broker, task_name=".:process_item")
    """

    def __init__(
        self,
        broker: "AsyncBroker",
        task: Any = None,
        task_name: str | None = None,
    ) -> None:
        """
        Initialize TaskiqPipeline.

        Args:
            broker: A Taskiq AsyncBroker instance (e.g., InMemoryBroker, RedisBroker)
            task: A decorated task function (created with @broker.task). If provided, task_name is ignored.
            task_name: Full name of the task registered on the broker (e.g., ".:process_item").
                      Either task or task_name must be provided.
        """
        if not TASKIQ_AVAILABLE:
            raise ImportError(
                "taskiq is required for TaskiqPipeline. Install it with: pip install taskiq"
            )
        if task is None and task_name is None:
            raise ValueError("Either 'task' or 'task_name' must be provided")

        self.broker = broker
        self._provided_task = task
        self._task: Any = None
        self.task_name = task_name
        self.logger = get_logger(component="TaskiqPipeline")

    async def open(self, spider: "Spider") -> None:
        """Open the pipeline and start the broker if needed."""
        await self.broker.startup()

        # If task was provided directly, use it
        if self._provided_task is not None:
            self._task = self._provided_task
            actual_task_name = getattr(self._task, "task_name", "unknown")
        else:
            # Find the registered task by name
            if self.task_name is None:
                raise ValueError("task_name cannot be None when task is not provided")
            self._task = self.broker.find_task(self.task_name)
            if self._task is None:
                raise ValueError(
                    f"Task '{self.task_name}' not found in broker. "
                    f"Make sure you've registered it with @broker.task and use the full task name (e.g., '.:task_name')"
                )
            actual_task_name = self.task_name

        self.logger.info(
            "Opened Taskiq pipeline",
            task_name=actual_task_name,
            broker=self.broker.__class__.__name__,
        )

    async def close(self, spider: "Spider") -> None:
        """Close the pipeline and shutdown the broker."""
        await self.broker.shutdown()
        task_name = getattr(self._task, "task_name", self.task_name or "unknown")
        self.logger.info("Closed Taskiq pipeline", task_name=task_name)

    async def process_item(self, item: Any, spider: "Spider") -> Any:
        """Send the item to the Taskiq broker for processing."""
        if self._task is None:
            raise RuntimeError("TaskiqPipeline not opened")

        # Send item to the task queue
        task_result = await self._task.kiq(item)

        task_name = getattr(self._task, "task_name", self.task_name or "unknown")
        self.logger.debug(
            "Sent item to Taskiq queue",
            task_name=task_name,
            task_id=task_result.task_id,
            spider=spider.name,
        )
        return item
