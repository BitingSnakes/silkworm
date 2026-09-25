from __future__ import annotations

import io
import json
import os
from typing import TYPE_CHECKING, Any

try:
    import asyncssh  # type: ignore[import-not-found]

    ASYNCSSH_AVAILABLE = True
except ImportError:
    ASYNCSSH_AVAILABLE = False

from ..logging import Logger, get_logger
from .base import log_pipeline_item

if TYPE_CHECKING:
    from .._types import JSONValue
    from ..spiders import Spider


class SFTPPipeline:
    """
    Pipeline that writes items to an SFTP server in JSON Lines format.

    Items are buffered until :meth:`close`, when a single upload is performed.

    Args:
        host: SFTP server hostname.
        user: Account name used for authentication.
        password: Password authentication secret. Either this or ``private_key``
            must be provided.
        remote_path: Destination path on the SFTP server.
        port: SFTP server port.
        private_key: Path to a private key used for authentication.
        known_hosts: Alternate OpenSSH known-hosts file. By default AsyncSSH uses
            the current user's standard known-hosts file.
        verify_host_key: Whether to reject servers whose host key is untrusted.

    Example:
        from silkworm.pipelines import SFTPPipeline

        pipeline = SFTPPipeline(
            host="sftp.example.com",
            user="username",
            password="password",
            remote_path="data/items.jl",
        )

    The server's host key is verified against ``~/.ssh/known_hosts`` by default.
    Pass ``known_hosts`` to use another file, or ``verify_host_key=False`` to
    skip verification (not recommended: it allows man-in-the-middle attacks).
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
        known_hosts: str | os.PathLike[str] | None = None,
        verify_host_key: bool = True,
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
            known_hosts: Path to an OpenSSH known_hosts file used to verify the
                server's host key. ``None`` (default) uses asyncssh's default,
                the user's ``~/.ssh/known_hosts``.
            verify_host_key: Verify the server's host key (default: True). Set to
                False only for trusted networks; it disables protection against
                man-in-the-middle attacks. Cannot be combined with ``known_hosts``.
        """
        if not ASYNCSSH_AVAILABLE:
            raise ImportError(
                "asyncssh is required for SFTPPipeline. Install it with: pip install silkworm-rs[sftp]",
            )

        if password is None and private_key is None:
            raise ValueError("Either password or private_key must be provided")
        if not verify_host_key and known_hosts is not None:
            raise ValueError("known_hosts cannot be used with verify_host_key=False")

        self.host = host
        self.user = user
        self.password = password
        self.remote_path = remote_path
        self.port = port
        self.private_key = private_key
        self.known_hosts: str | os.PathLike[str] | None = known_hosts
        self.verify_host_key: bool = verify_host_key
        self._items: list[str] = []
        self._conn: Any = None
        self._sftp: Any = None
        self.logger: Logger = get_logger(component="SFTPPipeline")

    async def open(self, spider: Spider) -> None:
        """Reset the in-memory JSON Lines buffer without connecting yet."""
        self._items = []
        self.logger.info(
            "Opened SFTP pipeline",
            host=self.host,
            port=self.port,
            remote_path=self.remote_path,
        )

    async def close(self, spider: Spider) -> None:
        """Connect, upload all buffered lines, and close SFTP resources."""
        if self._items:
            # Connect to SFTP server and upload all buffered items
            conn: Any | None = None
            sftp: Any | None = None
            try:
                connect_kwargs: dict[str, object] = {
                    "host": self.host,
                    "port": self.port,
                    "username": self.user,
                }
                if not self.verify_host_key:
                    # asyncssh treats known_hosts=None as "skip host key checks".
                    self.logger.warning(
                        "SFTP host key verification is disabled",
                        host=self.host,
                        port=self.port,
                    )
                    connect_kwargs["known_hosts"] = None
                elif self.known_hosts is not None:
                    connect_kwargs["known_hosts"] = os.fspath(self.known_hosts)
                # Otherwise asyncssh verifies against ~/.ssh/known_hosts.
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
                    await conn.wait_closed()
                    self._conn = None

        self.logger.info("Closed SFTP pipeline", remote_path=self.remote_path)

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        """Serialize and buffer one JSON line for upload during :meth:`close`."""
        line = json.dumps(item, ensure_ascii=False)
        self._items.append(line)
        log_pipeline_item(
            self,
            "Buffered item for SFTP",
            remote_path=self.remote_path,
            spider=spider.name,
        )
        return item
