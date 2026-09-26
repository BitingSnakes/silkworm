from __future__ import annotations

import json
from typing import TYPE_CHECKING

try:
    import aioftp  # type: ignore[import-not-found]

    AIOFTP_AVAILABLE = True
except ImportError:
    AIOFTP_AVAILABLE = False

from ..logging import Logger, get_logger
from .base import log_pipeline_item

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class FTPPipeline:
    """
    Pipeline that writes items to an FTP server in JSON Lines format.

    Args:
        host: FTP server hostname.
        user: Login username.
        password: Login password.
        remote_path: Destination object path.
        port: FTP control port.

    Items are buffered locally and replace the remote file during close.

    Example::

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
        self._client: aioftp.Client | None = None
        self.logger: Logger = get_logger(component="FTPPipeline")

    async def open(self, spider: Spider) -> None:
        """Reset the in-memory JSON Lines buffer without connecting yet."""
        self._items = []
        self.logger.info(
            "Opened FTP pipeline",
            host=self.host,
            port=self.port,
            remote_path=self.remote_path,
        )

    async def close(self, spider: Spider) -> None:
        """Upload all buffered lines and close the FTP connection."""
        if self._items:
            client = aioftp.Client()  # type: ignore[attr-defined]
            self._client = client
            primary: BaseException | None = None
            try:
                await client.connect(self.host, self.port)
                await client.login(self.user, self.password)

                content = "\n".join(self._items) + "\n"
                async with client.upload_stream(self.remote_path) as stream:
                    await stream.write(content.encode("utf-8"))

                self.logger.info(
                    "Uploaded items to FTP",
                    host=self.host,
                    remote_path=self.remote_path,
                    count=len(self._items),
                )
            except BaseException as exc:  # noqa: BLE001 - cleanup follows cancellation
                primary = exc
            finally:
                self._client = None
                try:
                    await client.quit()
                except BaseException as cleanup_exc:  # noqa: BLE001
                    if primary is None:
                        primary = cleanup_exc
                    else:
                        primary.add_note(f"FTP cleanup failed: {cleanup_exc}")
            if primary is not None:
                raise primary
            self._items = []

        self.logger.info("Closed FTP pipeline", remote_path=self.remote_path)

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        """Serialize and buffer one JSON line for upload during :meth:`close`."""
        line = json.dumps(item, ensure_ascii=False)
        self._items.append(line)
        log_pipeline_item(
            self,
            "Buffered item for FTP",
            remote_path=self.remote_path,
            spider=spider.name,
        )
        return item
