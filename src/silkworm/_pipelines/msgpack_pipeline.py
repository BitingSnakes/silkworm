from __future__ import annotations

import io
from pathlib import Path
from typing import TYPE_CHECKING

try:
    import ormsgpack  # type: ignore[import-not-found]

    ORMSGPACK_AVAILABLE = True
except ImportError:
    ORMSGPACK_AVAILABLE = False

from ..logging import Logger, get_logger
from .base import log_pipeline_item

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class MsgPackPipeline:
    """
    Pipeline that writes items to a file in MessagePack format.

    MessagePack is a binary serialization format that is more compact and faster
    than JSON. This pipeline uses ormsgpack for fast serialization.

    Args:
        path: Destination MessagePack file.
        mode: ``"write"`` to replace the file or ``"append"`` to retain and
            extend an existing stream of MessagePack values.

    Example::

        from silkworm.pipelines import MsgPackPipeline

        pipeline = MsgPackPipeline("data/items.msgpack")
        # Or append to existing file:
        pipeline = MsgPackPipeline("data/items.msgpack", mode="append")

    Reading MsgPack files::

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
                "ormsgpack is required for MsgPackPipeline. Install it with: pip install silkworm-rs[msgpack]",
            )
        if mode not in ("write", "append"):
            raise ValueError(f"mode must be 'write' or 'append', got '{mode}'")

        self.path: Path = Path(path)
        self.mode = mode
        self._fp: io.BufferedWriter | None = None
        self.logger: Logger = get_logger(component="MsgPackPipeline")

    async def open(self, spider: Spider) -> None:
        """Open the binary destination in overwrite or append mode."""
        self.path.parent.mkdir(parents=True, exist_ok=True)
        file_mode = "ab" if self.mode == "append" else "wb"
        self._fp = self.path.open(file_mode)
        self.logger.info("Opened MsgPack pipeline", path=str(self.path), mode=self.mode)

    async def close(self, spider: Spider) -> None:
        """Flush and close the MessagePack destination."""
        fp = self._fp
        self._fp = None
        if fp:
            fp.close()
            self.logger.info("Closed MsgPack pipeline", path=str(self.path))

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        """Serialize and append one MessagePack value."""
        if not self._fp:
            raise RuntimeError("MsgPackPipeline not opened")
        packed = ormsgpack.packb(item)  # pyright: ignore[reportPossiblyUnboundVariable]
        self._fp.write(packed)
        self._fp.flush()
        log_pipeline_item(
            self,
            "Wrote item to MsgPack",
            path=str(self.path),
            spider=spider.name,
        )
        return item
