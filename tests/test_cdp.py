import asyncio
import json

import pytest

from silkworm.exceptions import HttpError

websockets = pytest.importorskip("websockets")

from silkworm.cdp import CDPClient


async def _cdp_server(ws) -> None:
    async for raw in ws:
        message = json.loads(raw)
        method = message["method"]
        if method == "Target.createTarget":
            result = {"targetId": "target-1"}
        elif method == "Target.attachToTarget":
            result = {"sessionId": "session-1"}
        elif method == "Test.closeConnection":
            await ws.close(1001, "going away")
            return
        else:
            result = {}
        await ws.send(json.dumps({"id": message["id"], "result": result}))


async def test_cdp_client_fails_pending_commands_when_server_closes():
    async with websockets.serve(_cdp_server, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        client = CDPClient(ws_endpoint=f"ws://127.0.0.1:{port}")
        await client.connect()
        try:
            with pytest.raises(HttpError, match="closed unexpectedly.*code=1001"):
                await asyncio.wait_for(
                    client._send_command("Test.closeConnection"), timeout=5
                )
        finally:
            await client.close()


def _chrome_like_server(browser_path: str, *, advertise: bool = True):
    """Serve CDP only at ``browser_path`` and ``/json/version`` like Chrome does."""
    from http import HTTPStatus

    async def process_request(connection, request):
        if request.path == "/json/version":
            if not advertise:
                return connection.respond(HTTPStatus.NOT_FOUND, "not found\n")
            # Browsers advertise their own bind address, not the one clients use.
            body = json.dumps(
                {"webSocketDebuggerUrl": f"ws://127.0.0.1:1{browser_path}"}
            )
            return connection.respond(HTTPStatus.OK, body)
        if request.path != browser_path:
            return connection.respond(HTTPStatus.NOT_FOUND, "unknown target\n")
        return None

    return websockets.serve(
        _cdp_server, "127.0.0.1", 0, process_request=process_request
    )


@pytest.mark.parametrize("scheme", ["ws", "http"])
async def test_cdp_client_discovers_chrome_browser_endpoint(scheme: str) -> None:
    browser_path = "/devtools/browser/abc-123"
    async with _chrome_like_server(browser_path) as server:
        port = server.sockets[0].getsockname()[1]
        client = CDPClient(ws_endpoint=f"{scheme}://localhost:{port}")
        await client.connect()
        try:
            assert client._ws is not None
            assert client._ws.request.path == browser_path
            assert client._target_id == "target-1"
        finally:
            await client.close()


async def test_cdp_client_reports_failure_when_endpoint_cannot_be_discovered():
    async with _chrome_like_server("/devtools/browser/x", advertise=False) as server:
        port = server.sockets[0].getsockname()[1]
        client = CDPClient(ws_endpoint=f"ws://127.0.0.1:{port}")
        with pytest.raises(HttpError, match="Failed to connect to CDP endpoint"):
            await client.connect()
        assert client._ws is None


async def test_cdp_client_uses_explicit_browser_endpoint_without_discovery(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    browser_path = "/devtools/browser/abc-123"

    async def fail_discovery() -> str | None:
        raise AssertionError("discovery must not run for a working endpoint")

    async with _chrome_like_server(browser_path) as server:
        port = server.sockets[0].getsockname()[1]
        client = CDPClient(ws_endpoint=f"ws://127.0.0.1:{port}{browser_path}")
        monkeypatch.setattr(client, "_discover_ws_endpoint", fail_discovery)
        await client.connect()
        await client.close()
