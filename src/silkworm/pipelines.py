from __future__ import annotations
import csv
import inspect
import io
import json
import re
import sqlite3
import sys
import rxml
from collections.abc import Mapping
from pathlib import Path
from typing import Any, Protocol, TYPE_CHECKING, runtime_checkable

try:
    from taskiq import AsyncBroker  # type: ignore[import-not-found]

    TASKIQ_AVAILABLE = True
except ImportError:
    AsyncBroker = None  # type: ignore
    TASKIQ_AVAILABLE = False

try:
    import ormsgpack  # type: ignore[import-not-found]

    ORMSGPACK_AVAILABLE = True
except ImportError:
    ORMSGPACK_AVAILABLE = False

try:
    import polars as pl  # type: ignore[import-not-found]

    POLARS_AVAILABLE = True
except ImportError:
    POLARS_AVAILABLE = False

try:
    import openpyxl  # type: ignore[import-not-found, import-untyped]

    OPENPYXL_AVAILABLE = True
except ImportError:
    OPENPYXL_AVAILABLE = False

try:
    import yaml  # type: ignore[import-untyped]

    YAML_AVAILABLE = True
except ImportError:
    YAML_AVAILABLE = False

try:
    import fastavro  # type: ignore[import-not-found]

    FASTAVRO_AVAILABLE = True
except ImportError:
    FASTAVRO_AVAILABLE = False

try:
    from elasticsearch import AsyncElasticsearch  # type: ignore[import-not-found]

    ELASTICSEARCH_AVAILABLE = True
except ImportError:
    AsyncElasticsearch = None  # type: ignore
    ELASTICSEARCH_AVAILABLE = False

try:
    import motor.motor_asyncio  # type: ignore[import-not-found]

    MOTOR_AVAILABLE = True
except ImportError:
    MOTOR_AVAILABLE = False

try:
    import opendal  # type: ignore[import-not-found]

    OPENDAL_AVAILABLE = True
except ImportError:
    OPENDAL_AVAILABLE = False

try:
    import vortex  # type: ignore[import-not-found]
    import vortex.io  # type: ignore[import-not-found]

    VORTEX_AVAILABLE = True
except ImportError:
    VORTEX_AVAILABLE = False

try:
    import aiomysql  # type: ignore[import-not-found, import-untyped]

    AIOMYSQL_AVAILABLE = True
except ImportError:
    AIOMYSQL_AVAILABLE = False

try:
    import asyncpg  # type: ignore[import-not-found, import-untyped]

    ASYNCPG_AVAILABLE = True
except ImportError:
    ASYNCPG_AVAILABLE = False

try:
    from google.oauth2.service_account import Credentials  # type: ignore[import-not-found]
    from googleapiclient.discovery import build  # type: ignore[import-not-found, import-untyped]

    GOOGLE_SHEETS_AVAILABLE = True
except ImportError:
    Credentials = None  # type: ignore
    build = None  # type: ignore
    GOOGLE_SHEETS_AVAILABLE = False

try:
    import snowflake.connector  # type: ignore[import-not-found]

    SNOWFLAKE_AVAILABLE = True
except ImportError:
    SNOWFLAKE_AVAILABLE = False

try:
    from rnet import Client, Method  # type: ignore[import]

    RNET_AVAILABLE = True
except ImportError:
    Client = None  # type: ignore
    Method = None  # type: ignore
    RNET_AVAILABLE = False

try:
    import aioftp  # type: ignore[import-not-found]

    AIOFTP_AVAILABLE = True
except ImportError:
    AIOFTP_AVAILABLE = False

try:
    import asyncssh  # type: ignore[import-not-found]

    ASYNCSSH_AVAILABLE = True
except ImportError:
    ASYNCSSH_AVAILABLE = False

try:
    # Skip on Windows - cassandra-driver requires libev C extension which is not available
    if sys.platform == "win32":
        raise ImportError("cassandra-driver not supported on Windows")
    from cassandra.cluster import Cluster  # type: ignore[import-not-found, import-untyped]
    from cassandra.auth import PlainTextAuthProvider  # type: ignore[import-not-found, import-untyped]

    CASSANDRA_AVAILABLE = True
except ImportError:
    Cluster = None  # type: ignore
    PlainTextAuthProvider = None  # type: ignore
    CASSANDRA_AVAILABLE = False

try:
    import aiocouch  # type: ignore[import-not-found]

    AIOCOUCH_AVAILABLE = True
except ImportError:
    AIOCOUCH_AVAILABLE = False

try:
    import aioboto3  # type: ignore[import-not-found]

    AIOBOTO3_AVAILABLE = True
except ImportError:
    AIOBOTO3_AVAILABLE = False

if TYPE_CHECKING:
    from .spiders import Spider
from .logging import get_logger
from ._types import JSONValue


def _validate_table_name(table: str) -> str:
    """Validate table name to prevent SQL injection."""
    if not re.match(r"^[a-zA-Z_][a-zA-Z0-9_]*$", table):
        raise ValueError(
            f"Invalid table name '{table}'. Table names must start with a letter or underscore "
            "and contain only alphanumeric characters and underscores."
        )
    return table


class ItemPipeline(Protocol):
    async def open(self, spider: "Spider") -> None: ...
    async def close(self, spider: "Spider") -> None: ...
    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue: ...


class CallbackPipeline:
    """
    Pipeline that invokes a callback function to process each item.

    This pipeline allows you to define custom item processing logic using a simple
    callback function, making it easy to handle items without creating a full pipeline class.

    The callback function can be either synchronous or asynchronous and receives the item
    and spider as arguments.

    Example:
        from silkworm.pipelines import CallbackPipeline

        def process_item(item, spider):
            # Your custom processing logic
            print(f"Processing item from {spider.name}: {item}")
            return item

        pipeline = CallbackPipeline(callback=process_item)

        # Or with an async callback:
        async def async_process_item(item, spider):
            # Your async processing logic
            await some_async_operation(item)
            return item

        pipeline = CallbackPipeline(callback=async_process_item)
    """

    def __init__(self, callback) -> None:
        """
        Initialize CallbackPipeline.

        Args:
            callback: A callable that takes (item, spider) and returns the processed item.
                     Can be either synchronous or asynchronous.
        """
        if not callable(callback):
            raise TypeError("callback must be callable")

        self.callback = callback
        self.logger = get_logger(component="CallbackPipeline")

    async def open(self, spider: "Spider") -> None:
        """Open the pipeline."""
        callback_name = getattr(self.callback, "__name__", str(self.callback))
        self.logger.info("Opened Callback pipeline", callback=callback_name)

    async def close(self, spider: "Spider") -> None:
        """Close the pipeline."""
        callback_name = getattr(self.callback, "__name__", str(self.callback))
        self.logger.info("Closed Callback pipeline", callback=callback_name)

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        """Process an item using the callback function."""
        # Call the callback - handle both sync and async functions
        if inspect.iscoroutinefunction(self.callback):
            result = await self.callback(item, spider)
        else:
            result = self.callback(item, spider)

        # If callback returns None, return the original item
        if result is None:
            return item

        self.logger.debug(
            "Processed item with callback",
            spider=spider.name,
        )
        return result


@runtime_checkable
class _TaskiqTask(Protocol):
    task_name: str

    async def kiq(self, item: JSONValue): ...


@runtime_checkable
class _TaskiqResult(Protocol):
    task_id: str | int


@runtime_checkable
class _TaskiqBroker(Protocol):
    async def startup(self) -> None: ...
    async def shutdown(self) -> None: ...
    def find_task(self, task_name: str) -> _TaskiqTask | None: ...


class JsonLinesPipeline:
    def __init__(
        self,
        path: str | Path = "items.jl",
        *,
        use_opendal: bool | None = None,
    ) -> None:
        self.path = Path(path)
        self._fp: io.TextIOWrapper | None = None
        self._operator: opendal.AsyncOperator | None = None
        self._object_path: str | None = None
        self._use_opendal = OPENDAL_AVAILABLE if use_opendal is None else use_opendal
        if self._use_opendal and not OPENDAL_AVAILABLE:
            raise ImportError(
                "opendal is required for async JsonLinesPipeline writes. "
                "Install it with: pip install silkworm-rs[s3]"
            )
        self.logger = get_logger(component="JsonLinesPipeline")

    async def open(self, spider: "Spider") -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if self._use_opendal:
            self._operator = opendal.AsyncOperator("fs", root=str(self.path.parent))
            self._object_path = self.path.name
            self.logger.info(
                "Opened JSONL pipeline",
                path=str(self.path),
                backend="opendal",
            )
        else:
            self._fp = self.path.open("a", encoding="utf-8")
            self.logger.info("Opened JSONL pipeline", path=str(self.path))

    async def close(self, spider: "Spider") -> None:
        if self._operator:
            self._operator = None
            self._object_path = None
            self.logger.info(
                "Closed JSONL pipeline", path=str(self.path), backend="opendal"
            )
        if self._fp:
            self._fp.close()
            self._fp = None
            self.logger.info("Closed JSONL pipeline", path=str(self.path))

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        line = json.dumps(item, ensure_ascii=False)
        if self._operator:
            try:
                await self._append_with_opendal(line + "\n")
            except Exception as exc:
                self.logger.warning(
                    "OpenDAL write failed, falling back to local file handle",
                    path=str(self.path),
                    error=str(exc),
                )
                self._operator = None
                self._object_path = None
                self._fp = self.path.open("a", encoding="utf-8")
            else:
                self.logger.debug(
                    "Wrote item to JSONL",
                    path=str(self.path),
                    spider=spider.name,
                    backend="opendal",
                )
                return item

        if not self._fp:
            raise RuntimeError("JsonLinesPipeline not opened")
        self._fp.write(line + "\n")
        self._fp.flush()
        self.logger.debug(
            "Wrote item to JSONL", path=str(self.path), spider=spider.name
        )
        return item

    async def _append_with_opendal(self, data: str) -> None:
        if not self._operator or not self._object_path:
            raise RuntimeError("JsonLinesPipeline not opened")

        append_fn = getattr(self._operator, "append", None)
        if callable(append_fn):
            await append_fn(self._object_path, data.encode("utf-8"))
            return

        raise RuntimeError("OpenDAL operator does not support append writes")


class MsgPackPipeline:
    """
    Pipeline that writes items to a file in MessagePack format.

    MessagePack is a binary serialization format that is more compact and faster
    than JSON. This pipeline uses ormsgpack for fast serialization.

    Example:
        from silkworm.pipelines import MsgPackPipeline

        pipeline = MsgPackPipeline("data/items.msgpack")
        # Or append to existing file:
        pipeline = MsgPackPipeline("data/items.msgpack", mode="append")

    Reading MsgPack files:
        import msgpack

        # Read all items at once
        with open("data/items.msgpack", "rb") as f:
            unpacker = msgpack.Unpacker(f)
            items = list(unpacker)

        # Or stream items one by one (memory efficient for large files)
        with open("data/items.msgpack", "rb") as f:
            unpacker = msgpack.Unpacker(f)
            for item in unpacker:
                process(item)
    """

    def __init__(
        self,
        path: str | Path = "items.msgpack",
        *,
        mode: str = "write",
    ) -> None:
        """
        Initialize MsgPackPipeline.

        Args:
            path: Path to the output file (default: "items.msgpack")
            mode: Write mode - "write" (overwrite) or "append" (default: "write")
        """
        if not ORMSGPACK_AVAILABLE:
            raise ImportError(
                "ormsgpack is required for MsgPackPipeline. Install it with: pip install silkworm-rs[msgpack]"
            )
        if mode not in ("write", "append"):
            raise ValueError(f"mode must be 'write' or 'append', got '{mode}'")

        self.path = Path(path)
        self.mode = mode
        self._fp: io.BufferedWriter | None = None
        self.logger = get_logger(component="MsgPackPipeline")

    async def open(self, spider: "Spider") -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        file_mode = "ab" if self.mode == "append" else "wb"
        self._fp = self.path.open(file_mode)  # type: ignore[assignment]
        self.logger.info("Opened MsgPack pipeline", path=str(self.path), mode=self.mode)

    async def close(self, spider: "Spider") -> None:
        if self._fp:
            self._fp.close()
            self._fp = None
            self.logger.info("Closed MsgPack pipeline", path=str(self.path))

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        if not self._fp:
            raise RuntimeError("MsgPackPipeline not opened")
        packed = ormsgpack.packb(item)
        self._fp.write(packed)
        self._fp.flush()
        self.logger.debug(
            "Wrote item to MsgPack", path=str(self.path), spider=spider.name
        )
        return item


class SQLitePipeline:
    def __init__(self, path: str | Path = "items.db", table: str = "items") -> None:
        self.path = Path(path)
        self.table = _validate_table_name(table)
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

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        if not self._conn:
            raise RuntimeError("SQLitePipeline not opened")
        cur = self._conn.cursor()
        cur.execute(
            f"INSERT INTO {self.table} (spider, data) VALUES (?, ?)",
            (spider.name, json.dumps(item, ensure_ascii=False)),
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
        self._fp: io.TextIOWrapper | None = None
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

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        if not self._fp:
            raise RuntimeError("XMLPipeline not opened")

        node = self._to_node(self.item_element, item)
        xml_str = rxml.write_string(node, indent=2, default_xml_def=False)
        indented_xml = "\n".join(f"  {line}" for line in xml_str.splitlines())

        self._fp.write(indented_xml + "\n")
        self._fp.flush()
        self.logger.debug("Wrote item to XML", path=str(self.path), spider=spider.name)
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

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        if not self._fp:
            raise RuntimeError("CSVPipeline not opened")

        # Flatten nested structures if item is a dict
        if isinstance(item, Mapping):
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

    def _flatten_dict(
        self, data: Mapping[str, JSONValue], parent_key: str = "", sep: str = "_"
    ) -> dict[str, JSONValue | str]:
        """Flatten a nested dictionary structure."""
        items: list[tuple[str, JSONValue | str]] = []
        for key, value in data.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            if isinstance(value, Mapping):
                items.extend(self._flatten_dict(value, new_key, sep=sep).items())
            elif isinstance(value, list):
                # Convert list to comma-separated string
                items.append((new_key, ", ".join(str(v) for v in value)))
            else:
                items.append((new_key, value))
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
        broker: "_TaskiqBroker",
        task: _TaskiqTask | None = None,
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

        self.broker: _TaskiqBroker = broker
        self._provided_task: _TaskiqTask | None = task
        self._task: _TaskiqTask | None = None
        self.task_name = task_name
        self.logger = get_logger(component="TaskiqPipeline")

    async def open(self, spider: "Spider") -> None:
        """Open the pipeline and start the broker if needed."""
        await self.broker.startup()

        # If task was provided directly, use it
        if self._provided_task is not None:
            self._task = self._provided_task
            actual_task_name = self._task.task_name
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
        task_name = (
            self._task.task_name
            if self._task is not None
            else self.task_name
            if self.task_name is not None
            else "unknown"
        )
        self.logger.info("Closed Taskiq pipeline", task_name=task_name)

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        """Send the item to the Taskiq broker for processing."""
        if self._task is None:
            raise RuntimeError("TaskiqPipeline not opened")

        # Send item to the task queue
        task_result = await self._task.kiq(item)
        task_name = self._task.task_name
        task_id: str | int | None = None
        if isinstance(task_result, _TaskiqResult):
            task_id = task_result.task_id
        self.logger.debug(
            "Sent item to Taskiq queue",
            task_name=task_name,
            task_id=task_id or "unknown",
            spider=spider.name,
        )
        return item


class PolarsPipeline:
    """
    Pipeline that writes items to a Parquet file using Polars.

    Parquet is a columnar storage format optimized for analytics workloads.
    This pipeline uses Polars for fast and efficient Parquet serialization.

    Example:
        from silkworm.pipelines import PolarsPipeline

        pipeline = PolarsPipeline("data/items.parquet")
        # Or append to existing file:
        pipeline = PolarsPipeline("data/items.parquet", mode="append")

    Reading Parquet files:
        import polars as pl

        # Read entire dataset
        df = pl.read_parquet("data/items.parquet")

        # Or read with filters/projections (memory efficient)
        df = pl.scan_parquet("data/items.parquet").filter(
            pl.col("author") == "John"
        ).collect()
    """

    def __init__(
        self,
        path: str | Path = "items.parquet",
        *,
        mode: str = "write",
    ) -> None:
        """
        Initialize PolarsPipeline.

        Args:
            path: Path to the output file (default: "items.parquet")
            mode: Write mode - "write" (overwrite) or "append" (default: "write")
        """
        if not POLARS_AVAILABLE:
            raise ImportError(
                "polars is required for PolarsPipeline. Install it with: pip install silkworm-rs[polars]"
            )
        if mode not in ("write", "append"):
            raise ValueError(f"mode must be 'write' or 'append', got '{mode}'")

        self.path = Path(path)
        self.mode = mode
        self._items: list[JSONValue] = []
        self.logger = get_logger(component="PolarsPipeline")

    async def open(self, spider: "Spider") -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._items = []
        self.logger.info("Opened Polars pipeline", path=str(self.path), mode=self.mode)

    async def close(self, spider: "Spider") -> None:
        if self._items:
            df = pl.DataFrame(self._items)
            if self.mode == "append" and self.path.exists():
                # Read existing data and concatenate
                existing_df = pl.read_parquet(self.path)
                df = pl.concat([existing_df, df])
            df.write_parquet(self.path)
        self.logger.info("Closed Polars pipeline", path=str(self.path))

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        self._items.append(item)
        self.logger.debug(
            "Buffered item for Parquet", path=str(self.path), spider=spider.name
        )
        return item


class ExcelPipeline:
    """
    Pipeline that writes items to an Excel file (.xlsx).

    Example:
        from silkworm.pipelines import ExcelPipeline

        pipeline = ExcelPipeline("data/items.xlsx", sheet_name="quotes")
    """

    def __init__(
        self,
        path: str | Path = "items.xlsx",
        *,
        sheet_name: str = "Sheet1",
    ) -> None:
        """
        Initialize ExcelPipeline.

        Args:
            path: Path to the output file (default: "items.xlsx")
            sheet_name: Name of the Excel sheet (default: "Sheet1")
        """
        if not OPENPYXL_AVAILABLE:
            raise ImportError(
                "openpyxl is required for ExcelPipeline. Install it with: pip install silkworm-rs[excel]"
            )

        self.path = Path(path)
        self.sheet_name = sheet_name
        self._items: list[JSONValue] = []
        self.logger = get_logger(component="ExcelPipeline")

    async def open(self, spider: "Spider") -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._items = []
        self.logger.info("Opened Excel pipeline", path=str(self.path))

    async def close(self, spider: "Spider") -> None:
        if self._items:
            wb = openpyxl.Workbook()
            ws = wb.active
            ws.title = self.sheet_name

            # Get fieldnames from first item
            if isinstance(self._items[0], Mapping):
                flat_items: list[dict[str, JSONValue | str]] = []
                for item in self._items:
                    if isinstance(item, Mapping):
                        flat_items.append(self._flatten_dict(item))
                    else:
                        flat_items.append({"value": str(item)})
                fieldnames = list(flat_items[0].keys())

                # Write header
                ws.append(fieldnames)

                # Write data
                for item in flat_items:
                    ws.append([item.get(field) for field in fieldnames])
            else:
                # Simple values
                ws.append(["value"])
                for item in self._items:
                    ws.append([str(item)])

            wb.save(self.path)
        self.logger.info("Closed Excel pipeline", path=str(self.path))

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        self._items.append(item)
        self.logger.debug(
            "Buffered item for Excel", path=str(self.path), spider=spider.name
        )
        return item

    def _flatten_dict(
        self, data: Mapping[str, JSONValue], parent_key: str = "", sep: str = "_"
    ) -> dict[str, JSONValue | str]:
        """Flatten a nested dictionary structure."""
        items: list[tuple[str, JSONValue | str]] = []
        for key, value in data.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            if isinstance(value, Mapping):
                items.extend(self._flatten_dict(value, new_key, sep=sep).items())
            elif isinstance(value, list):
                # Convert list to comma-separated string
                items.append((new_key, ", ".join(str(v) for v in value)))
            else:
                items.append((new_key, value))
        return dict(items)


class YAMLPipeline:
    """
    Pipeline that writes items to a YAML file.

    Example:
        from silkworm.pipelines import YAMLPipeline

        pipeline = YAMLPipeline("data/items.yaml")
    """

    def __init__(
        self,
        path: str | Path = "items.yaml",
    ) -> None:
        """
        Initialize YAMLPipeline.

        Args:
            path: Path to the output file (default: "items.yaml")
        """
        if not YAML_AVAILABLE:
            raise ImportError(
                "pyyaml is required for YAMLPipeline. Install it with: pip install silkworm-rs[yaml]"
            )

        self.path = Path(path)
        self._items: list[JSONValue] = []
        self.logger = get_logger(component="YAMLPipeline")

    async def open(self, spider: "Spider") -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._items = []
        self.logger.info("Opened YAML pipeline", path=str(self.path))

    async def close(self, spider: "Spider") -> None:
        if self._items:
            with self.path.open("w", encoding="utf-8") as f:
                yaml.dump(self._items, f, default_flow_style=False, allow_unicode=True)
        self.logger.info("Closed YAML pipeline", path=str(self.path))

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        self._items.append(item)
        self.logger.debug(
            "Buffered item for YAML", path=str(self.path), spider=spider.name
        )
        return item


class AvroPipeline:
    """
    Pipeline that writes items to an Avro file.

    Avro is a row-oriented data serialization system with compact binary format.

    Example:
        from silkworm.pipelines import AvroPipeline

        schema = {
            "type": "record",
            "name": "Quote",
            "fields": [
                {"name": "text", "type": "string"},
                {"name": "author", "type": "string"},
                {"name": "tags", "type": {"type": "array", "items": "string"}},
            ],
        }
        pipeline = AvroPipeline("data/items.avro", schema=schema)
    """

    def __init__(
        self,
        path: str | Path = "items.avro",
        *,
        schema: dict | None = None,
    ) -> None:
        """
        Initialize AvroPipeline.

        Args:
            path: Path to the output file (default: "items.avro")
            schema: Avro schema dict. If None, will infer from first item.
        """
        if not FASTAVRO_AVAILABLE:
            raise ImportError(
                "fastavro is required for AvroPipeline. Install it with: pip install silkworm-rs[avro]"
            )

        self.path = Path(path)
        self.schema = schema
        self._items: list[JSONValue] = []
        self.logger = get_logger(component="AvroPipeline")

    async def open(self, spider: "Spider") -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._items = []
        self.logger.info("Opened Avro pipeline", path=str(self.path))

    async def close(self, spider: "Spider") -> None:
        if self._items:
            schema = self.schema
            if schema is None:
                # Infer schema from first item
                schema = self._infer_schema(self._items[0])

            with self.path.open("wb") as f:
                fastavro.writer(f, schema, self._items)
        self.logger.info("Closed Avro pipeline", path=str(self.path))

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        self._items.append(item)
        self.logger.debug(
            "Buffered item for Avro", path=str(self.path), spider=spider.name
        )
        return item

    def _infer_schema(self, item: JSONValue) -> dict:
        """Infer a simple Avro schema from the first item."""
        fields = []
        if isinstance(item, dict):
            for key, value in item.items():
                field_type = self._infer_type(value)
                fields.append({"name": key, "type": ["null", field_type]})

        return {
            "type": "record",
            "name": "ScrapedItem",
            "fields": fields,
        }

    def _infer_type(self, value: JSONValue) -> str | dict:
        """Infer Avro type from Python value."""
        if isinstance(value, bool):
            return "boolean"
        elif isinstance(value, int):
            return "long"
        elif isinstance(value, float):
            return "double"
        elif isinstance(value, str):
            return "string"
        elif isinstance(value, list):
            if value:
                item_type = self._infer_type(value[0])
                return {"type": "array", "items": item_type}
            return {"type": "array", "items": "string"}
        elif isinstance(value, dict):
            # For nested dicts, convert to JSON string
            return "string"
        else:
            return "string"


class ElasticsearchPipeline:
    """
    Pipeline that sends items to an Elasticsearch index.

    Example:
        from silkworm.pipelines import ElasticsearchPipeline

        pipeline = ElasticsearchPipeline(
            hosts=["http://localhost:9200"],
            index="quotes",
        )
    """

    def __init__(
        self,
        hosts: list[str] | str = "http://localhost:9200",
        *,
        index: str = "items",
        **es_kwargs,
    ) -> None:
        """
        Initialize ElasticsearchPipeline.

        Args:
            hosts: Elasticsearch host(s)
            index: Index name
            **es_kwargs: Additional kwargs for AsyncElasticsearch client
        """
        if not ELASTICSEARCH_AVAILABLE:
            raise ImportError(
                "elasticsearch is required for ElasticsearchPipeline. Install it with: pip install silkworm-rs[elasticsearch]"
            )

        self.hosts = [hosts] if isinstance(hosts, str) else hosts
        self.index = index
        self.es_kwargs = es_kwargs
        self._client: "AsyncElasticsearch | None" = None
        self.logger = get_logger(component="ElasticsearchPipeline")

    async def open(self, spider: "Spider") -> None:
        self._client = AsyncElasticsearch(self.hosts, **self.es_kwargs)
        self.logger.info(
            "Opened Elasticsearch pipeline", hosts=self.hosts, index=self.index
        )

    async def close(self, spider: "Spider") -> None:
        if self._client:
            await self._client.close()
            self._client = None
            self.logger.info("Closed Elasticsearch pipeline", index=self.index)

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        if not self._client:
            raise RuntimeError("ElasticsearchPipeline not opened")

        await self._client.index(index=self.index, document=item)
        self.logger.debug(
            "Indexed item in Elasticsearch", index=self.index, spider=spider.name
        )
        return item


class MongoDBPipeline:
    """
    Pipeline that sends items to a MongoDB collection.

    Example:
        from silkworm.pipelines import MongoDBPipeline

        pipeline = MongoDBPipeline(
            connection_string="mongodb://localhost:27017",
            database="scraping",
            collection="quotes",
        )
    """

    def __init__(
        self,
        connection_string: str = "mongodb://localhost:27017",
        *,
        database: str = "scraping",
        collection: str = "items",
    ) -> None:
        """
        Initialize MongoDBPipeline.

        Args:
            connection_string: MongoDB connection string
            database: Database name
            collection: Collection name
        """
        if not MOTOR_AVAILABLE:
            raise ImportError(
                "motor is required for MongoDBPipeline. Install it with: pip install silkworm-rs[mongodb]"
            )

        self.connection_string = connection_string
        self.database = database
        self.collection = collection
        self._client = None  # type: ignore[var-annotated]
        self._db = None
        self._coll = None
        self.logger = get_logger(component="MongoDBPipeline")

    async def open(self, spider: "Spider") -> None:
        self._client = motor.motor_asyncio.AsyncIOMotorClient(self.connection_string)  # type: ignore[assignment]
        self._db = self._client[self.database]  # type: ignore[index]
        self._coll = self._db[self.collection]  # type: ignore[index]
        self.logger.info(
            "Opened MongoDB pipeline",
            database=self.database,
            collection=self.collection,
        )

    async def close(self, spider: "Spider") -> None:
        if self._client:
            self._client.close()
            self._client = None
            self._db = None
            self._coll = None
            self.logger.info("Closed MongoDB pipeline", collection=self.collection)

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        if not self._coll:
            raise RuntimeError("MongoDBPipeline not opened")

        await self._coll.insert_one(item)
        self.logger.debug(
            "Inserted item in MongoDB", collection=self.collection, spider=spider.name
        )
        return item


class S3JsonLinesPipeline:
    """
    Pipeline that writes items to S3 in JSON Lines format using async OpenDAL.

    Example:
        from silkworm.pipelines import S3JsonLinesPipeline

        pipeline = S3JsonLinesPipeline(
            bucket="my-bucket",
            key="data/items.jl",
            region="us-east-1",
        )
    """

    def __init__(
        self,
        bucket: str,
        key: str = "items.jl",
        *,
        region: str = "us-east-1",
        endpoint: str | None = None,
        access_key_id: str | None = None,
        secret_access_key: str | None = None,
    ) -> None:
        """
        Initialize S3JsonLinesPipeline.

        Args:
            bucket: S3 bucket name
            key: S3 object key (path)
            region: AWS region
            endpoint: Custom S3 endpoint (for S3-compatible services)
            access_key_id: AWS access key ID (uses env vars if not provided)
            secret_access_key: AWS secret access key (uses env vars if not provided)
        """
        if not OPENDAL_AVAILABLE:
            raise ImportError(
                "opendal is required for S3JsonLinesPipeline. Install it with: pip install silkworm-rs[s3]"
            )

        self.bucket = bucket
        self.key = key
        self.region = region
        self.endpoint = endpoint
        self.access_key_id = access_key_id
        self.secret_access_key = secret_access_key
        self._items: list[str] = []
        self._operator: opendal.AsyncOperator | None = None
        self.logger = get_logger(component="S3JsonLinesPipeline")

    async def open(self, spider: "Spider") -> None:
        # Configure OpenDAL operator for S3
        config = {
            "bucket": self.bucket,
            "region": self.region,
        }
        if self.endpoint:
            config["endpoint"] = self.endpoint
        if self.access_key_id:
            config["access_key_id"] = self.access_key_id
        if self.secret_access_key:
            config["secret_access_key"] = self.secret_access_key

        self._operator = opendal.AsyncOperator("s3", **config)
        self._items = []
        self.logger.info(
            "Opened S3 JSON Lines pipeline",
            bucket=self.bucket,
            key=self.key,
            region=self.region,
        )

    async def close(self, spider: "Spider") -> None:
        if self._items and self._operator:
            # Write all buffered items to S3
            content = "\n".join(self._items)
            await self._operator.write(self.key, content.encode("utf-8"))
        self._operator = None
        self.logger.info("Closed S3 JSON Lines pipeline", key=self.key)

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        line = json.dumps(item, ensure_ascii=False)
        self._items.append(line)
        self.logger.debug("Buffered item for S3", key=self.key, spider=spider.name)
        return item


class VortexPipeline:
    """
    Pipeline that writes items to a Vortex file using the vortex-data library.

    Vortex is a next-generation columnar file format optimized for high-performance
    data processing with 100x faster random access reads compared to Parquet, 10-20x
    faster scans, and similar compression ratios. It provides zero-copy compatibility
    with Apache Arrow.

    Example:
        from silkworm.pipelines import VortexPipeline

        pipeline = VortexPipeline("data/items.vortex")

    Reading Vortex files:
        import vortex

        # Open and read a Vortex file
        vortex_file = vortex.file.open("data/items.vortex")
        arrow_table = vortex_file.to_arrow().read_all()

        # Or convert to other formats
        df = vortex_file.to_polars()  # Polars DataFrame
        df = vortex_file.to_dataset().to_table()  # PyArrow Table
    """

    def __init__(
        self,
        path: str | Path = "items.vortex",
    ) -> None:
        """
        Initialize VortexPipeline.

        Args:
            path: Path to the output file (default: "items.vortex")
        """
        if not VORTEX_AVAILABLE:
            raise ImportError(
                "vortex is required for VortexPipeline. Install it with: pip install silkworm-rs[vortex]"
            )

        self.path = Path(path)
        self._items: list[JSONValue] = []
        self.logger = get_logger(component="VortexPipeline")

    async def open(self, spider: "Spider") -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._items = []
        self.logger.info("Opened Vortex pipeline", path=str(self.path))

    async def close(self, spider: "Spider") -> None:
        if self._items:
            # Convert items list to PyArrow Table
            # Vortex can directly accept PyArrow tables for efficient writing
            import pyarrow as pa  # type: ignore[import-not-found]

            table = pa.Table.from_pylist(self._items)

            # Write the table to a Vortex file
            vortex.io.write(table, str(self.path))

            self.logger.info(
                "Closed Vortex pipeline",
                path=str(self.path),
                items_written=len(self._items),
            )
        else:
            self.logger.info("Closed Vortex pipeline (no items)", path=str(self.path))

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        self._items.append(item)
        self.logger.debug(
            "Buffered item for Vortex", path=str(self.path), spider=spider.name
        )
        return item


class MySQLPipeline:
    """
    Pipeline that sends items to a MySQL database.

    Example:
        from silkworm.pipelines import MySQLPipeline

        pipeline = MySQLPipeline(
            host="localhost",
            port=3306,
            user="root",
            password="password",
            database="scraping",
            table="items",
        )
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 3306,
        user: str = "root",
        password: str = "",
        database: str = "scraping",
        *,
        table: str = "items",
    ) -> None:
        """
        Initialize MySQLPipeline.

        Args:
            host: MySQL host
            port: MySQL port
            user: MySQL user
            password: MySQL password
            database: Database name
            table: Table name
        """
        if not AIOMYSQL_AVAILABLE:
            raise ImportError(
                "aiomysql is required for MySQLPipeline. Install it with: pip install silkworm-rs[mysql]"
            )

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.table = _validate_table_name(table)
        self._pool = None  # type: ignore[var-annotated]
        self.logger = get_logger(component="MySQLPipeline")

    async def open(self, spider: "Spider") -> None:
        self._pool = await aiomysql.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            db=self.database,
        )

        # Create table if it doesn't exist
        async with self._pool.acquire() as conn:  # type: ignore[union-attr, attr-defined]
            async with conn.cursor() as cur:
                await cur.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {self.table} (
                        id INT AUTO_INCREMENT PRIMARY KEY,
                        spider VARCHAR(255) NOT NULL,
                        data JSON NOT NULL,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                    """
                )
                await conn.commit()

        self.logger.info(
            "Opened MySQL pipeline",
            host=self.host,
            database=self.database,
            table=self.table,
        )

    async def close(self, spider: "Spider") -> None:
        if self._pool:
            self._pool.close()
            await self._pool.wait_closed()
            self._pool = None
            self.logger.info("Closed MySQL pipeline", table=self.table)

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        if not self._pool:
            raise RuntimeError("MySQLPipeline not opened")

        async with self._pool.acquire() as conn:  # type: ignore[union-attr, attr-defined]
            async with conn.cursor() as cur:
                await cur.execute(
                    f"INSERT INTO {self.table} (spider, data) VALUES (%s, %s)",
                    (spider.name, json.dumps(item, ensure_ascii=False)),
                )
                await conn.commit()

        self.logger.debug(
            "Inserted item in MySQL", table=self.table, spider=spider.name
        )
        return item


class PostgreSQLPipeline:
    """
    Pipeline that sends items to a PostgreSQL database.

    Example:
        from silkworm.pipelines import PostgreSQLPipeline

        pipeline = PostgreSQLPipeline(
            host="localhost",
            port=5432,
            user="postgres",
            password="password",
            database="scraping",
            table="items",
        )
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 5432,
        user: str = "postgres",
        password: str = "",
        database: str = "scraping",
        *,
        table: str = "items",
    ) -> None:
        """
        Initialize PostgreSQLPipeline.

        Args:
            host: PostgreSQL host
            port: PostgreSQL port
            user: PostgreSQL user
            password: PostgreSQL password
            database: Database name
            table: Table name
        """
        if not ASYNCPG_AVAILABLE:
            raise ImportError(
                "asyncpg is required for PostgreSQLPipeline. Install it with: pip install silkworm-rs[postgresql]"
            )

        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.table = _validate_table_name(table)
        self._pool = None  # type: ignore[var-annotated]
        self.logger = get_logger(component="PostgreSQLPipeline")

    async def open(self, spider: "Spider") -> None:
        self._pool = await asyncpg.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
        )

        # Create table if it doesn't exist
        async with self._pool.acquire() as conn:  # type: ignore[union-attr, attr-defined]
            await conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.table} (
                    id SERIAL PRIMARY KEY,
                    spider VARCHAR(255) NOT NULL,
                    data JSONB NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
            )

        self.logger.info(
            "Opened PostgreSQL pipeline",
            host=self.host,
            database=self.database,
            table=self.table,
        )

    async def close(self, spider: "Spider") -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None
            self.logger.info("Closed PostgreSQL pipeline", table=self.table)

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        if not self._pool:
            raise RuntimeError("PostgreSQLPipeline not opened")

        async with self._pool.acquire() as conn:  # type: ignore[union-attr, attr-defined]
            await conn.execute(
                f"INSERT INTO {self.table} (spider, data) VALUES ($1, $2)",
                spider.name,
                json.dumps(item, ensure_ascii=False),
            )

        self.logger.debug(
            "Inserted item in PostgreSQL", table=self.table, spider=spider.name
        )
        return item


class WebhookPipeline:
    """
    Pipeline that sends items to a webhook endpoint using rnet HTTP client.

    This pipeline uses the same HTTP client (rnet) as the spider itself for
    sending data to webhooks, ensuring consistent behavior and browser impersonation.

    Example:
        from silkworm.pipelines import WebhookPipeline

        pipeline = WebhookPipeline(
            url="https://webhook.site/unique-id",
            method="POST",
            headers={"Authorization": "Bearer token123"},
        )
    """

    def __init__(
        self,
        url: str,
        *,
        method: str = "POST",
        headers: dict[str, str] | None = None,
        timeout: float | None = 30.0,
        batch_size: int = 1,
    ) -> None:
        """
        Initialize WebhookPipeline.

        Args:
            url: Webhook endpoint URL
            method: HTTP method (default: "POST")
            headers: Optional HTTP headers to send with each request
            timeout: Request timeout in seconds (default: 30.0)
            batch_size: Number of items to batch before sending (default: 1 for immediate sending)
        """
        if not RNET_AVAILABLE:
            raise ImportError(
                "rnet is required for WebhookPipeline but appears to be unavailable. "
                "This should not happen as rnet is a core dependency."
            )

        self.url = url
        self.method = method
        self.headers = headers or {}
        self.timeout = timeout
        self.batch_size = batch_size
        self._client: "Client | None" = None  # type: ignore[name-defined]
        self._batch: list[JSONValue] = []
        self.logger = get_logger(component="WebhookPipeline")

    async def open(self, spider: "Spider") -> None:
        self._client = Client()  # type: ignore[misc]
        self._batch = []
        self.logger.info(
            "Opened Webhook pipeline",
            url=self.url,
            method=self.method,
            batch_size=self.batch_size,
        )

    async def close(self, spider: "Spider") -> None:
        # Send any remaining batched items
        if self._batch:
            await self._send_batch()

        if self._client:
            closer = getattr(self._client, "aclose", None) or getattr(
                self._client, "close", None
            )
            if closer and callable(closer):
                try:
                    result = closer()
                    if hasattr(result, "__await__"):
                        await result  # type: ignore
                except Exception:
                    pass
            self._client = None

        self.logger.info("Closed Webhook pipeline", url=self.url)

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        if not self._client:
            raise RuntimeError("WebhookPipeline not opened")

        self._batch.append(item)

        if len(self._batch) >= self.batch_size:
            await self._send_batch()

        return item

    async def _send_batch(self) -> None:
        """Send the current batch of items to the webhook."""
        if not self._batch or not self._client:
            return

        # Prepare payload
        payload = self._batch[0] if len(self._batch) == 1 else self._batch

        try:
            # Use rnet client to send the request
            method_upper = self.method.upper()
            if not hasattr(Method, method_upper):  # type: ignore[attr-defined]
                raise ValueError(
                    f"Invalid HTTP method '{self.method}'. Must be one of: GET, POST, PUT, DELETE, PATCH, HEAD, OPTIONS"
                )
            method_enum = getattr(Method, method_upper)  # type: ignore[attr-defined]
            kwargs = {
                "headers": self.headers,
                "json": payload,
            }
            if self.timeout is not None:
                kwargs["timeout"] = self.timeout

            response = await self._client.request(method_enum, self.url, **kwargs)  # type: ignore[union-attr, arg-type]

            # Try to get status code
            status = getattr(response, "status", None)
            if status is None:
                status = getattr(response, "status_code", None)
            if status is not None and hasattr(status, "value"):
                status = status.value

            # Close response if possible
            closer = getattr(response, "aclose", None) or getattr(
                response, "close", None
            )
            if closer and callable(closer):
                try:
                    result = closer()
                    if hasattr(result, "__await__"):
                        await result  # type: ignore
                except Exception:
                    pass

            self.logger.debug(
                "Sent items to webhook",
                url=self.url,
                count=len(self._batch),
                status=status,
            )
        except Exception as exc:
            self.logger.error(
                "Failed to send items to webhook",
                url=self.url,
                count=len(self._batch),
                error=str(exc),
            )
            raise

        # Clear the batch after sending
        self._batch = []


class GoogleSheetsPipeline:
    """
    Pipeline that appends items to a Google Sheet.

    Requires Google Sheets API credentials (service account JSON file).

    Example:
        from silkworm.pipelines import GoogleSheetsPipeline

        pipeline = GoogleSheetsPipeline(
            spreadsheet_id="1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms",
            credentials_file="path/to/credentials.json",
            sheet_name="Sheet1",
        )
    """

    def __init__(
        self,
        spreadsheet_id: str,
        credentials_file: str,
        *,
        sheet_name: str = "Sheet1",
        batch_size: int = 100,
    ) -> None:
        """
        Initialize GoogleSheetsPipeline.

        Args:
            spreadsheet_id: Google Sheets spreadsheet ID (from the URL)
            credentials_file: Path to service account credentials JSON file
            sheet_name: Name of the sheet to append to (default: "Sheet1")
            batch_size: Number of items to batch before writing (default: 100)
        """
        if not GOOGLE_SHEETS_AVAILABLE:
            raise ImportError(
                "google-api-python-client and google-auth are required for GoogleSheetsPipeline. "
                "Install them with: pip install silkworm-rs[gsheets]"
            )

        self.spreadsheet_id = spreadsheet_id
        self.credentials_file = credentials_file
        self.sheet_name = sheet_name
        self.batch_size = batch_size
        self._service = None  # type: ignore[var-annotated]
        self._batch: list[JSONValue] = []
        self._fieldnames: list[str] | None = None
        self._header_written = False
        self.logger = get_logger(component="GoogleSheetsPipeline")

    async def open(self, spider: "Spider") -> None:
        # Initialize Google Sheets API client
        creds = Credentials.from_service_account_file(  # type: ignore[union-attr]
            self.credentials_file,
            scopes=["https://www.googleapis.com/auth/spreadsheets"],
        )
        self._service = build("sheets", "v4", credentials=creds)  # type: ignore[misc]
        self._batch = []
        self._fieldnames = None
        self._header_written = False
        self.logger.info(
            "Opened Google Sheets pipeline",
            spreadsheet_id=self.spreadsheet_id,
            sheet_name=self.sheet_name,
        )

    async def close(self, spider: "Spider") -> None:
        # Write any remaining batched items
        if self._batch:
            await self._write_batch()

        self._service = None
        self.logger.info(
            "Closed Google Sheets pipeline",
            spreadsheet_id=self.spreadsheet_id,
            sheet_name=self.sheet_name,
        )

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        if not self._service:
            raise RuntimeError("GoogleSheetsPipeline not opened")

        self._batch.append(item)

        if len(self._batch) >= self.batch_size:
            await self._write_batch()

        return item

    async def _write_batch(self) -> None:
        """Write the current batch of items to Google Sheets."""
        if not self._batch or not self._service:
            return

        try:
            # Flatten items if they are dicts
            rows: list[list[str | int | float | bool | None]] = []

            for item in self._batch:
                if isinstance(item, Mapping):
                    flat_item = self._flatten_dict(item)
                    # Initialize fieldnames from first item
                    if self._fieldnames is None:
                        self._fieldnames = list(flat_item.keys())
                    row = [flat_item.get(field) for field in self._fieldnames]  # type: ignore[misc]
                    rows.append(row)  # type: ignore
                else:
                    # Simple value
                    if self._fieldnames is None:
                        self._fieldnames = ["value"]
                    rows.append([str(item)])

            # Write header if first batch
            if not self._header_written and self._fieldnames:
                header_range = f"{self.sheet_name}!A1"
                header_body = {"values": [self._fieldnames]}
                self._service.spreadsheets().values().append(
                    spreadsheetId=self.spreadsheet_id,
                    range=header_range,
                    valueInputOption="RAW",
                    body=header_body,
                ).execute()
                self._header_written = True

            # Append data rows
            if rows:
                data_range = f"{self.sheet_name}!A2"
                body = {"values": rows}
                self._service.spreadsheets().values().append(
                    spreadsheetId=self.spreadsheet_id,
                    range=data_range,
                    valueInputOption="RAW",
                    body=body,
                ).execute()

            self.logger.debug(
                "Wrote items to Google Sheets",
                spreadsheet_id=self.spreadsheet_id,
                sheet_name=self.sheet_name,
                count=len(self._batch),
            )
        except Exception as exc:
            self.logger.error(
                "Failed to write items to Google Sheets",
                spreadsheet_id=self.spreadsheet_id,
                sheet_name=self.sheet_name,
                count=len(self._batch),
                error=str(exc),
            )
            raise

        # Clear the batch after writing
        self._batch = []

    def _flatten_dict(
        self, data: Mapping[str, JSONValue], parent_key: str = "", sep: str = "_"
    ) -> dict[str, JSONValue | str]:
        """Flatten a nested dictionary structure."""
        items: list[tuple[str, JSONValue | str]] = []
        for key, value in data.items():
            new_key = f"{parent_key}{sep}{key}" if parent_key else key
            if isinstance(value, Mapping):
                items.extend(self._flatten_dict(value, new_key, sep=sep).items())
            elif isinstance(value, list):
                # Convert list to comma-separated string
                items.append((new_key, ", ".join(str(v) for v in value)))
            else:
                items.append((new_key, value))
        return dict(items)


class SnowflakePipeline:
    """
    Pipeline that sends items to a Snowflake data warehouse.

    Example:
        from silkworm.pipelines import SnowflakePipeline

        pipeline = SnowflakePipeline(
            account="myaccount",
            user="myuser",
            password="mypassword",
            database="mydatabase",
            schema="myschema",
            warehouse="mywarehouse",
            table="items",
        )
    """

    def __init__(
        self,
        account: str,
        user: str,
        password: str,
        database: str,
        schema: str,
        warehouse: str,
        *,
        table: str = "items",
        role: str | None = None,
    ) -> None:
        """
        Initialize SnowflakePipeline.

        Args:
            account: Snowflake account identifier
            user: Snowflake username
            password: Snowflake password
            database: Database name
            schema: Schema name
            warehouse: Warehouse name
            table: Table name (default: "items")
            role: Optional role name
        """
        if not SNOWFLAKE_AVAILABLE:
            raise ImportError(
                "snowflake-connector-python is required for SnowflakePipeline. "
                "Install it with: pip install silkworm-rs[snowflake]"
            )

        self.account = account
        self.user = user
        self.password = password
        self.database = database
        self.schema = schema
        self.warehouse = warehouse
        self.table = _validate_table_name(table)
        self.role = role
        self._conn = None  # type: ignore[var-annotated]
        self._cursor = None  # type: ignore[var-annotated]
        self.logger = get_logger(component="SnowflakePipeline")

    async def open(self, spider: "Spider") -> None:
        # Connect to Snowflake
        conn_params = {
            "account": self.account,
            "user": self.user,
            "password": self.password,
            "database": self.database,
            "schema": self.schema,
            "warehouse": self.warehouse,
        }
        if self.role:
            conn_params["role"] = self.role

        conn = snowflake.connector.connect(**conn_params)  # type: ignore[attr-defined]
        cursor = conn.cursor()
        self._conn = conn
        self._cursor = cursor

        # Create table if it doesn't exist
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                id NUMBER AUTOINCREMENT PRIMARY KEY,
                spider VARCHAR(255) NOT NULL,
                data VARIANT NOT NULL,
                created_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
            """
        )

        self.logger.info(
            "Opened Snowflake pipeline",
            account=self.account,
            database=self.database,
            schema=self.schema,
            table=self.table,
        )

    async def close(self, spider: "Spider") -> None:
        if self._cursor:
            self._cursor.close()
            self._cursor = None

        if self._conn:
            self._conn.close()
            self._conn = None

        self.logger.info("Closed Snowflake pipeline", table=self.table)

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        if not self._cursor or not self._conn:
            raise RuntimeError("SnowflakePipeline not opened")

        # Insert item into Snowflake
        self._cursor.execute(
            f"INSERT INTO {self.table} (spider, data) VALUES (%s, %s)",
            (spider.name, json.dumps(item, ensure_ascii=False)),
        )
        self._conn.commit()

        self.logger.debug(
            "Inserted item in Snowflake", table=self.table, spider=spider.name
        )
        return item


class FTPPipeline:
    """
    Pipeline that writes items to an FTP server in JSON Lines format.

    Example:
        from silkworm.pipelines import FTPPipeline

        pipeline = FTPPipeline(
            host="ftp.example.com",
            user="username",
            password="password",
            remote_path="data/items.jl",
        )
    """

    def __init__(
        self,
        host: str,
        user: str,
        password: str,
        remote_path: str = "items.jl",
        *,
        port: int = 21,
    ) -> None:
        """
        Initialize FTPPipeline.

        Args:
            host: FTP server hostname
            user: FTP username
            password: FTP password
            remote_path: Remote file path (default: "items.jl")
            port: FTP port (default: 21)
        """
        if not AIOFTP_AVAILABLE:
            raise ImportError(
                "aioftp is required for FTPPipeline. Install it with: pip install silkworm-rs[ftp]"
            )

        self.host = host
        self.user = user
        self.password = password
        self.remote_path = remote_path
        self.port = port
        self._items: list[str] = []
        self._client: "aioftp.Client | None" = None  # type: ignore[name-defined]
        self.logger = get_logger(component="FTPPipeline")

    async def open(self, spider: "Spider") -> None:
        self._items = []
        self.logger.info(
            "Opened FTP pipeline",
            host=self.host,
            port=self.port,
            remote_path=self.remote_path,
        )

    async def close(self, spider: "Spider") -> None:
        if self._items:
            # Connect to FTP server and upload all buffered items
            self._client = aioftp.Client()  # type: ignore[attr-defined]
            try:
                await self._client.connect(self.host, self.port)  # type: ignore[union-attr]
                await self._client.login(self.user, self.password)  # type: ignore[union-attr]

                # Write items to a temporary buffer
                content = "\n".join(self._items) + "\n"
                buffer = io.BytesIO(content.encode("utf-8"))

                # Upload the file
                await self._client.upload_stream(buffer, self.remote_path)  # type: ignore[union-attr]

                self.logger.info(
                    "Uploaded items to FTP",
                    host=self.host,
                    remote_path=self.remote_path,
                    count=len(self._items),
                )
            finally:
                if self._client:
                    await self._client.quit()  # type: ignore[union-attr]
                    self._client = None

        self.logger.info("Closed FTP pipeline", remote_path=self.remote_path)

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        line = json.dumps(item, ensure_ascii=False)
        self._items.append(line)
        self.logger.debug(
            "Buffered item for FTP", remote_path=self.remote_path, spider=spider.name
        )
        return item


class SFTPPipeline:
    """
    Pipeline that writes items to an SFTP server in JSON Lines format.

    Example:
        from silkworm.pipelines import SFTPPipeline

        pipeline = SFTPPipeline(
            host="sftp.example.com",
            user="username",
            password="password",
            remote_path="data/items.jl",
        )
    """

    def __init__(
        self,
        host: str,
        user: str,
        password: str | None = None,
        remote_path: str = "items.jl",
        *,
        port: int = 22,
        private_key: str | None = None,
    ) -> None:
        """
        Initialize SFTPPipeline.

        Args:
            host: SFTP server hostname
            user: SFTP username
            password: SFTP password (optional if using private_key)
            remote_path: Remote file path (default: "items.jl")
            port: SFTP port (default: 22)
            private_key: Path to private key file for key-based authentication (optional)
        """
        if not ASYNCSSH_AVAILABLE:
            raise ImportError(
                "asyncssh is required for SFTPPipeline. Install it with: pip install silkworm-rs[sftp]"
            )

        if password is None and private_key is None:
            raise ValueError("Either password or private_key must be provided")

        self.host = host
        self.user = user
        self.password = password
        self.remote_path = remote_path
        self.port = port
        self.private_key = private_key
        self._items: list[str] = []
        self._conn = None  # type: ignore[var-annotated]
        self._sftp = None  # type: ignore[var-annotated]
        self.logger = get_logger(component="SFTPPipeline")

    async def open(self, spider: "Spider") -> None:
        self._items = []
        self.logger.info(
            "Opened SFTP pipeline",
            host=self.host,
            port=self.port,
            remote_path=self.remote_path,
        )

    async def close(self, spider: "Spider") -> None:
        if self._items:
            # Connect to SFTP server and upload all buffered items
            try:
                conn: Any | None = None
                sftp: Any | None = None
                connect_kwargs = {
                    "host": self.host,
                    "port": self.port,
                    "username": self.user,
                    "known_hosts": None,  # Disable host key verification for simplicity
                }
                if self.password:
                    connect_kwargs["password"] = self.password
                if self.private_key:
                    connect_kwargs["client_keys"] = [self.private_key]

                conn = await asyncssh.connect(**connect_kwargs)  # type: ignore[attr-defined]
                self._conn = conn
                sftp = await conn.start_sftp_client()  # type: ignore[attr-defined]
                self._sftp = sftp

                # Write items to a temporary buffer
                content = "\n".join(self._items) + "\n"
                buffer = io.BytesIO(content.encode("utf-8"))

                # Upload the file
                async with sftp.open(self.remote_path, "wb") as remote_file:  # type: ignore[union-attr]
                    await remote_file.write(buffer.getvalue())

                self.logger.info(
                    "Uploaded items to SFTP",
                    host=self.host,
                    remote_path=self.remote_path,
                    count=len(self._items),
                )
            finally:
                if sftp:
                    sftp.exit()
                    self._sftp = None
                if conn:
                    conn.close()
                    await conn.wait_closed()  # type: ignore[union-attr]
                    self._conn = None

        self.logger.info("Closed SFTP pipeline", remote_path=self.remote_path)

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        line = json.dumps(item, ensure_ascii=False)
        self._items.append(line)
        self.logger.debug(
            "Buffered item for SFTP", remote_path=self.remote_path, spider=spider.name
        )
        return item


class CassandraPipeline:
    """
    Pipeline that sends items to an Apache Cassandra database.

    Example:
        from silkworm.pipelines import CassandraPipeline

        pipeline = CassandraPipeline(
            hosts=["127.0.0.1"],
            keyspace="scraping",
            table="items",
            username="cassandra",
            password="cassandra",
        )
    """

    def __init__(
        self,
        hosts: list[str] | None = None,
        keyspace: str = "scraping",
        *,
        table: str = "items",
        username: str | None = None,
        password: str | None = None,
        port: int = 9042,
    ) -> None:
        """
        Initialize CassandraPipeline.

        Args:
            hosts: List of Cassandra cluster hosts (default: ["127.0.0.1"])
            keyspace: Keyspace name
            table: Table name (default: "items")
            username: Optional username for authentication
            password: Optional password for authentication
            port: Cassandra port (default: 9042)
        """
        if not CASSANDRA_AVAILABLE:
            raise ImportError(
                "cassandra-driver is required for CassandraPipeline. "
                "Install it with: pip install silkworm-rs[cassandra]"
            )

        self.hosts = hosts or ["127.0.0.1"]
        self.keyspace = keyspace
        self.table = _validate_table_name(table)
        self.username = username
        self.password = password
        self.port = port
        self._cluster = None  # type: ignore[var-annotated]
        self._session = None  # type: ignore[var-annotated]
        self.logger = get_logger(component="CassandraPipeline")

    async def open(self, spider: "Spider") -> None:
        # Setup authentication if credentials provided
        auth_provider = None
        if self.username and self.password:
            auth_provider = PlainTextAuthProvider(  # type: ignore[misc]
                username=self.username, password=self.password
            )

        # Connect to Cassandra cluster
        cluster = Cluster(  # type: ignore[misc]
            self.hosts, port=self.port, auth_provider=auth_provider
        )
        session = cluster.connect()
        self._cluster = cluster
        self._session = session

        # Create keyspace if it doesn't exist
        session.execute(
            f"""
            CREATE KEYSPACE IF NOT EXISTS {self.keyspace}
            WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}
            """
        )

        # Use the keyspace
        session.set_keyspace(self.keyspace)

        # Create table if it doesn't exist
        session.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {self.table} (
                id uuid PRIMARY KEY,
                spider text,
                data text,
                created_at timestamp
            )
            """
        )

        self.logger.info(
            "Opened Cassandra pipeline",
            hosts=self.hosts,
            keyspace=self.keyspace,
            table=self.table,
        )

    async def close(self, spider: "Spider") -> None:
        if self._cluster:
            self._cluster.shutdown()
            self._cluster = None
            self._session = None
            self.logger.info("Closed Cassandra pipeline", table=self.table)

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        if not self._session:
            raise RuntimeError("CassandraPipeline not opened")

        import uuid
        from datetime import datetime

        # Insert item into Cassandra
        self._session.execute(
            f"""
            INSERT INTO {self.table} (id, spider, data, created_at)
            VALUES (%s, %s, %s, %s)
            """,
            (
                uuid.uuid4(),
                spider.name,
                json.dumps(item, ensure_ascii=False),
                datetime.now(),
            ),
        )

        self.logger.debug(
            "Inserted item in Cassandra", table=self.table, spider=spider.name
        )
        return item


class CouchDBPipeline:
    """
    Pipeline that sends items to a CouchDB database.

    Example:
        from silkworm.pipelines import CouchDBPipeline

        pipeline = CouchDBPipeline(
            url="http://localhost:5984",
            database="scraping",
            username="admin",
            password="password",
        )
    """

    def __init__(
        self,
        url: str = "http://localhost:5984",
        database: str = "scraping",
        *,
        username: str | None = None,
        password: str | None = None,
    ) -> None:
        """
        Initialize CouchDBPipeline.

        Args:
            url: CouchDB server URL (default: "http://localhost:5984")
            database: Database name (default: "scraping")
            username: Optional username for authentication
            password: Optional password for authentication
        """
        if not AIOCOUCH_AVAILABLE:
            raise ImportError(
                "aiocouch is required for CouchDBPipeline. "
                "Install it with: pip install silkworm-rs[couchdb]"
            )

        self.url = url
        self.database = database
        self.username = username
        self.password = password
        self._client = None  # type: ignore[var-annotated]
        self._db = None  # type: ignore[var-annotated]
        self.logger = get_logger(component="CouchDBPipeline")

    async def open(self, spider: "Spider") -> None:
        # Connect to CouchDB
        if self.username and self.password:
            self._client = await aiocouch.CouchDB(  # type: ignore[attr-defined]
                self.url, user=self.username, password=self.password
            ).__aenter__()
        else:
            self._client = await aiocouch.CouchDB(self.url).__aenter__()  # type: ignore[attr-defined]

        client = self._client
        if client is None:
            raise RuntimeError("Failed to initialize CouchDB client")

        # Create database if it doesn't exist
        try:
            self._db = await client[self.database]  # type: ignore[index]
        except KeyError:
            self._db = await client.create(self.database)  # type: ignore[union-attr]

        self.logger.info(
            "Opened CouchDB pipeline", url=self.url, database=self.database
        )

    async def close(self, spider: "Spider") -> None:
        if self._client:
            await self._client.__aexit__(None, None, None)  # type: ignore[union-attr]
            self._client = None
            self._db = None
            self.logger.info("Closed CouchDB pipeline", database=self.database)

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        if not self._db:
            raise RuntimeError("CouchDBPipeline not opened")

        # Add spider name to item metadata
        doc_data = {"spider": spider.name, "data": item}

        # Create document in CouchDB
        await self._db.create(doc_data)  # type: ignore[union-attr]

        self.logger.debug(
            "Inserted item in CouchDB", database=self.database, spider=spider.name
        )
        return item


class DynamoDBPipeline:
    """
    Pipeline that sends items to AWS DynamoDB.

    Example:
        from silkworm.pipelines import DynamoDBPipeline

        pipeline = DynamoDBPipeline(
            table_name="items",
            region_name="us-east-1",
            aws_access_key_id="YOUR_KEY",
            aws_secret_access_key="YOUR_SECRET",
        )
    """

    def __init__(
        self,
        table_name: str = "items",
        *,
        region_name: str = "us-east-1",
        aws_access_key_id: str | None = None,
        aws_secret_access_key: str | None = None,
        endpoint_url: str | None = None,
    ) -> None:
        """
        Initialize DynamoDBPipeline.

        Args:
            table_name: DynamoDB table name (default: "items")
            region_name: AWS region (default: "us-east-1")
            aws_access_key_id: AWS access key ID (uses env vars/IAM if not provided)
            aws_secret_access_key: AWS secret access key (uses env vars/IAM if not provided)
            endpoint_url: Custom endpoint URL for DynamoDB Local or other services
        """
        if not AIOBOTO3_AVAILABLE:
            raise ImportError(
                "aioboto3 is required for DynamoDBPipeline. "
                "Install it with: pip install silkworm-rs[dynamodb]"
            )

        self.table_name = table_name
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.endpoint_url = endpoint_url
        self._session = None  # type: ignore[var-annotated]
        self._client = None  # type: ignore[var-annotated]
        self._resource = None  # type: ignore[var-annotated]
        self._table = None  # type: ignore[var-annotated]
        self.logger = get_logger(component="DynamoDBPipeline")

    async def open(self, spider: "Spider") -> None:
        # Create aioboto3 session
        session_kwargs = {"region_name": self.region_name}
        if self.aws_access_key_id and self.aws_secret_access_key:
            session_kwargs["aws_access_key_id"] = self.aws_access_key_id
            session_kwargs["aws_secret_access_key"] = self.aws_secret_access_key

        session = aioboto3.Session(**session_kwargs)  # type: ignore[attr-defined]
        self._session = session

        # Create resource and client
        resource_kwargs = {}
        if self.endpoint_url:
            resource_kwargs["endpoint_url"] = self.endpoint_url

        resource = await session.resource(  # type: ignore[attr-defined]
            "dynamodb", **resource_kwargs
        ).__aenter__()
        client = await session.client(  # type: ignore[attr-defined]
            "dynamodb", **resource_kwargs
        ).__aenter__()
        self._resource = resource
        self._client = client

        # Create table if it doesn't exist
        try:
            await client.describe_table(TableName=self.table_name)  # type: ignore[union-attr]
            table = await resource.Table(self.table_name)  # type: ignore[union-attr]
        except client.exceptions.ResourceNotFoundException:  # type: ignore[union-attr]
            # Create table with a simple schema (id as primary key)
            table = await resource.create_table(  # type: ignore[union-attr]
                TableName=self.table_name,
                KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
                AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
                BillingMode="PAY_PER_REQUEST",
            )
            # Wait for table to be created
            await table.wait_until_exists()  # type: ignore[union-attr]
        self._table = table

        self.logger.info(
            "Opened DynamoDB pipeline",
            table_name=self.table_name,
            region=self.region_name,
        )

    async def close(self, spider: "Spider") -> None:
        if self._client:
            await self._client.__aexit__(None, None, None)  # type: ignore[union-attr]
            self._client = None
        if self._resource:
            await self._resource.__aexit__(None, None, None)  # type: ignore[union-attr]
            self._resource = None
            self._table = None
        self.logger.info("Closed DynamoDB pipeline", table_name=self.table_name)

    async def process_item(self, item: JSONValue, spider: "Spider") -> JSONValue:
        if not self._table:
            raise RuntimeError("DynamoDBPipeline not opened")

        import uuid

        # Create item with unique ID and spider metadata
        dynamo_item = {
            "id": str(uuid.uuid4()),
            "spider": spider.name,
            "data": json.dumps(item, ensure_ascii=False),
        }

        # Put item in DynamoDB
        await self._table.put_item(Item=dynamo_item)  # type: ignore[union-attr]

        self.logger.debug(
            "Inserted item in DynamoDB", table_name=self.table_name, spider=spider.name
        )
        return item
