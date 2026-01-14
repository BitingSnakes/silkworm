from __future__ import annotations

from unittest.mock import patch

import pytest

from silkworm.exceptions import HttpError


class TestCDPClient:
    """Tests for the CDP client."""

    @pytest.mark.anyio
    async def test_cdp_client_import_without_websockets(self):
        """Test that importing CDPClient without websockets raises ImportError."""
        with patch("silkworm.cdp.HAS_WEBSOCKETS", False):
            from silkworm.cdp import CDPClient

            with pytest.raises(ImportError, match="websockets package required"):
                CDPClient()

    @pytest.mark.anyio
    async def test_cdp_client_properties(self):
        """Test CDP client properties - if websockets not available, skip."""
        try:
            from silkworm.cdp import CDPClient
        except ImportError:
            pytest.skip("websockets not installed")

        # We can't actually create a client without websockets available
        # but we can test that it would be configured correctly
        # This test mainly ensures the API is correct
        with pytest.raises(ImportError):
            CDPClient(
                ws_endpoint="ws://127.0.0.1:9222",
                concurrency=32,
                html_max_size_bytes=10_000_000,
            )

    @pytest.mark.anyio
    async def test_cdp_client_timeout_conversion(self):
        """Test that timedelta timeout conversion method exists."""
        from datetime import timedelta

        # Test the timeout conversion logic
        try:
            from silkworm.cdp import CDPClient
        except ImportError:
            pytest.skip("websockets not installed")

        # Since we can't instantiate without websockets, we just verify
        # the import works and the class exists
        assert hasattr(CDPClient, "_timeout_seconds")


class TestFetchHTMLCDP:
    """Tests for the fetch_html_cdp convenience function."""

    @pytest.mark.anyio
    async def test_fetch_html_cdp_import(self):
        """Test that fetch_html_cdp can be imported."""
        try:
            from silkworm import fetch_html_cdp

            # Just verify it's importable
            assert callable(fetch_html_cdp)
        except ImportError as exc:
            # If websockets is not installed, it's okay
            if "websockets" not in str(exc):
                raise