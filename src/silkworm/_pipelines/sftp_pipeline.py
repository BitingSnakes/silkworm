from __future__ import annotations

import io
import json
from typing import TYPE_CHECKING, Any

try:
    import asyncssh  # type: ignore[import-not-found]

    ASYNCSSH_AVAILABLE = True
except ImportError:
    ASYNCSSH_AVAILABLE = False

from ..logging import get_logger
from .base import _log_pipeline_item

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


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
                "asyncssh is required for SFTPPipeline. Install it with: pip install silkworm-rs[sftp]",
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
        self._conn: Any = None
        self._sftp: Any = None
        self.logger = get_logger(component="SFTPPipeline")

    async def open(self, spider: Spider) -> None:
        self._items = []
        self.logger.info(
            "Opened SFTP pipeline",
            host=self.host,
            port=self.port,
            remote_path=self.remote_path,
        )

    async def close(self, spider: Spider) -> None:
        if self._items:
            # Connect to SFTP server and upload all buffered items
            conn: Any | None = None
            sftp: Any | None = None
            try:
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

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        line = json.dumps(item, ensure_ascii=False)
        self._items.append(line)
        _log_pipeline_item(
            self,
            "Buffered item for SFTP",
            remote_path=self.remote_path,
            spider=spider.name,
        )
        return item
