from __future__ import annotations

import json
from typing import TYPE_CHECKING

try:
    import aioftp  # type: ignore[import-not-found]

    AIOFTP_AVAILABLE = True
except ImportError:
    AIOFTP_AVAILABLE = False

from ..logging import get_logger
from .base import _log_pipeline_item

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


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
                "aioftp is required for FTPPipeline. Install it with: pip install silkworm-rs[ftp]",
            )

        self.host = host
        self.user = user
        self.password = password
        self.remote_path = remote_path
        self.port = port
        self._items: list[str] = []
        self._client: aioftp.Client | None = None  # type: ignore[name-defined]
        self.logger = get_logger(component="FTPPipeline")

    async def open(self, spider: Spider) -> None:
        self._items = []
        self.logger.info(
            "Opened FTP pipeline",
            host=self.host,
            port=self.port,
            remote_path=self.remote_path,
        )

    async def close(self, spider: Spider) -> None:
        if self._items:
            # Connect to FTP server and upload all buffered items
            self._client = aioftp.Client()  # type: ignore[attr-defined]
            try:
                await self._client.connect(self.host, self.port)  # type: ignore[union-attr]
                await self._client.login(self.user, self.password)  # type: ignore[union-attr]

                content = "\n".join(self._items) + "\n"

                # Upload the file
                async with self._client.upload_stream(self.remote_path) as stream:  # type: ignore[union-attr]
                    await stream.write(content.encode("utf-8"))

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

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        line = json.dumps(item, ensure_ascii=False)
        self._items.append(line)
        _log_pipeline_item(
            self,
            "Buffered item for FTP",
            remote_path=self.remote_path,
            spider=spider.name,
        )
        return item
