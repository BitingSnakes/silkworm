from __future__ import annotations

import json
import tempfile
from pathlib import Path
from typing import Any, Self

import pytest

from silkworm.spiders import Spider

# FTPPipeline tests - skip if aioftp not installed
try:
    import aioftp  # type: ignore[import-not-found]

    from silkworm.pipelines import FTPPipeline

    AIOFTP_AVAILABLE = True
except ImportError:
    AIOFTP_AVAILABLE = False
    FTPPipeline = None


@pytest.mark.skipif(not AIOFTP_AVAILABLE, reason="aioftp not installed")
def test_ftp_pipeline_initialization():
    # Test that we can initialize the pipeline
    pipeline = FTPPipeline(  # type: ignore
        host="ftp.example.com",
        user="username",
        password="password",
        remote_path="data/items.jl",
        port=21,
    )
    assert pipeline.host == "ftp.example.com"
    assert pipeline.user == "username"
    assert pipeline.password == "password"
    assert pipeline.remote_path == "data/items.jl"
    assert pipeline.port == 21


@pytest.mark.skipif(not AIOFTP_AVAILABLE, reason="aioftp not installed")
async def test_ftp_pipeline_uploads_items():
    with tempfile.TemporaryDirectory() as tmpdir:
        user = aioftp.User("user", "password", base_path=tmpdir)
        server = aioftp.Server([user])
        await server.start("127.0.0.1", 0)
        try:
            port = server.server.sockets[0].getsockname()[1]
            pipeline = FTPPipeline(  # type: ignore
                host="127.0.0.1",
                user="user",
                password="password",
                remote_path="items.jl",
                port=port,
            )
            spider = Spider()

            await pipeline.open(spider)
            await pipeline.process_item({"text": "Hello"}, spider)
            await pipeline.process_item({"text": "World"}, spider)
            await pipeline.close(spider)
        finally:
            await server.close()

        lines = (Path(tmpdir) / "items.jl").read_text(encoding="utf-8").splitlines()
        assert [json.loads(line) for line in lines] == [
            {"text": "Hello"},
            {"text": "World"},
        ]


# SFTPPipeline tests - skip if asyncssh not installed
try:
    import asyncssh  # type: ignore[import-not-found]  # noqa: F401

    from silkworm.pipelines import SFTPPipeline

    ASYNCSSH_AVAILABLE = True
except ImportError:
    ASYNCSSH_AVAILABLE = False
    SFTPPipeline = None


@pytest.mark.skipif(not ASYNCSSH_AVAILABLE, reason="asyncssh not installed")
def test_sftp_pipeline_initialization():
    # Test that we can initialize the pipeline
    pipeline = SFTPPipeline(  # type: ignore
        host="sftp.example.com",
        user="username",
        password="password",
        remote_path="data/items.jl",
        port=22,
    )
    assert pipeline.host == "sftp.example.com"
    assert pipeline.user == "username"
    assert pipeline.password == "password"
    assert pipeline.remote_path == "data/items.jl"
    assert pipeline.port == 22


@pytest.mark.skipif(not ASYNCSSH_AVAILABLE, reason="asyncssh not installed")
def test_sftp_pipeline_initialization_with_private_key():
    # Test that we can initialize the pipeline with private key
    pipeline = SFTPPipeline(  # type: ignore
        host="sftp.example.com",
        user="username",
        remote_path="data/items.jl",
        private_key="/path/to/key",
    )
    assert pipeline.host == "sftp.example.com"
    assert pipeline.user == "username"
    assert pipeline.password is None
    assert pipeline.private_key == "/path/to/key"


@pytest.mark.skipif(not ASYNCSSH_AVAILABLE, reason="asyncssh not installed")
def test_sftp_pipeline_requires_password_or_key():
    # Test that initializing without password or key raises error
    with pytest.raises(
        ValueError, match="Either password or private_key must be provided"
    ):
        SFTPPipeline(  # type: ignore
            host="sftp.example.com",
            user="username",
            remote_path="data/items.jl",
        )


class _FakeSFTPFile:
    def __init__(self, uploads: dict[str, bytes], path: str) -> None:
        self._uploads = uploads
        self._path = path

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *exc_info: object) -> None:
        return None

    async def write(self, data: bytes) -> None:
        self._uploads[self._path] = data


class _FakeSFTPClient:
    def __init__(self, uploads: dict[str, bytes]) -> None:
        self._uploads = uploads

    def open(self, path: str, mode: str) -> _FakeSFTPFile:
        return _FakeSFTPFile(self._uploads, path)

    def exit(self) -> None:
        return None


class _FakeSSHConnection:
    def __init__(self, uploads: dict[str, bytes]) -> None:
        self._uploads = uploads

    async def start_sftp_client(self) -> _FakeSFTPClient:
        return _FakeSFTPClient(self._uploads)

    def close(self) -> None:
        return None

    async def wait_closed(self) -> None:
        return None


async def _upload_with_fake_asyncssh(
    monkeypatch: pytest.MonkeyPatch, pipeline: Any
) -> tuple[dict[str, Any], dict[str, bytes]]:
    """Run the pipeline against a fake asyncssh.connect; return its kwargs and uploads."""
    from silkworm._pipelines import sftp_pipeline

    captured: dict[str, Any] = {}
    uploads: dict[str, bytes] = {}

    async def fake_connect(**kwargs: Any) -> _FakeSSHConnection:
        captured.update(kwargs)
        return _FakeSSHConnection(uploads)

    monkeypatch.setattr(sftp_pipeline.asyncssh, "connect", fake_connect)
    spider = Spider()
    await pipeline.open(spider)
    await pipeline.process_item({"text": "Hello"}, spider)
    await pipeline.close(spider)
    return captured, uploads


@pytest.mark.skipif(not ASYNCSSH_AVAILABLE, reason="asyncssh not installed")
async def test_sftp_pipeline_verifies_host_key_by_default(
    monkeypatch: pytest.MonkeyPatch,
):
    pipeline = SFTPPipeline(  # type: ignore
        host="sftp.example.com", user="username", password="password"
    )

    captured, uploads = await _upload_with_fake_asyncssh(monkeypatch, pipeline)

    # Not passing known_hosts lets asyncssh verify against ~/.ssh/known_hosts;
    # known_hosts=None would disable verification.
    assert "known_hosts" not in captured
    assert captured["host"] == "sftp.example.com"
    assert uploads == {"items.jl": b'{"text": "Hello"}\n'}


@pytest.mark.skipif(not ASYNCSSH_AVAILABLE, reason="asyncssh not installed")
async def test_sftp_pipeline_forwards_known_hosts_path(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
):
    known_hosts = tmp_path / "known_hosts"
    pipeline = SFTPPipeline(  # type: ignore
        host="sftp.example.com",
        user="username",
        password="password",
        known_hosts=known_hosts,
    )

    captured, _ = await _upload_with_fake_asyncssh(monkeypatch, pipeline)

    assert captured["known_hosts"] == str(known_hosts)


@pytest.mark.skipif(not ASYNCSSH_AVAILABLE, reason="asyncssh not installed")
async def test_sftp_pipeline_can_disable_host_key_verification(
    monkeypatch: pytest.MonkeyPatch,
):
    pipeline = SFTPPipeline(  # type: ignore
        host="sftp.example.com",
        user="username",
        password="password",
        verify_host_key=False,
    )

    captured, _ = await _upload_with_fake_asyncssh(monkeypatch, pipeline)

    assert "known_hosts" in captured
    assert captured["known_hosts"] is None


@pytest.mark.skipif(not ASYNCSSH_AVAILABLE, reason="asyncssh not installed")
def test_sftp_pipeline_rejects_known_hosts_without_verification():
    with pytest.raises(ValueError, match="known_hosts cannot be used"):
        SFTPPipeline(  # type: ignore
            host="sftp.example.com",
            user="username",
            password="password",
            known_hosts="/etc/ssh/known_hosts",
            verify_host_key=False,
        )
