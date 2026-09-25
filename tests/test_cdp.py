import asyncio
import json

import pytest

from silkworm.exceptions import HttpError

websockets = pytest.importorskip("websockets")

from silkworm.cdp import CDPClient  # noqa: E402


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
