"""Prometheus text exposition of crawl statistics, served without dependencies."""

from __future__ import annotations

import asyncio
import re
from typing import TYPE_CHECKING

from .logging import Logger, get_logger

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from ._stats import CrawlStats

# Labeled counters -> (metric name, label name).
_LABELED_METRICS = {
    "responses_by_status": ("silkworm_responses_by_status_total", "status"),
    "requests_by_domain": ("silkworm_requests_by_domain_total", "domain"),
    "errors_by_type": ("silkworm_errors_by_type_total", "type"),
    "items_dropped_by_reason": ("silkworm_items_dropped_by_reason_total", "reason"),
    "ignored_by_reason": ("silkworm_ignored_requests_by_reason_total", "reason"),
}
_INVALID_NAME_CHARS = re.compile(r"[^a-zA-Z0-9_]")


def _metric_name(name: str) -> str:
    return _INVALID_NAME_CHARS.sub("_", name)


def _label_value(value: str) -> str:
    return value.replace("\\", "\\\\").replace("\n", "\\n").replace('"', '\\"')


def render_metrics(
    *,
    spider: str,
    stats: CrawlStats,
    gauges: Mapping[str, float],
    custom: Mapping[str, object],
) -> str:
    """Render crawl statistics in the Prometheus text exposition format."""
    spider_label = f'spider="{_label_value(spider)}"'
    lines: list[str] = []

    for name, value in sorted(stats.counters.items()):
        metric = f"silkworm_{_metric_name(name)}_total"
        lines += [f"# TYPE {metric} counter", f"{metric}{{{spider_label}}} {value}"]

    for name, counter in sorted(stats.labeled.items()):
        metric, label = _LABELED_METRICS.get(
            name, (f"silkworm_{_metric_name(name)}_total", "label")
        )
        lines.append(f"# TYPE {metric} counter")
        for label_value, value in sorted(counter.items()):
            labels = f'{spider_label},{label}="{_label_value(label_value)}"'
            lines.append(f"{metric}{{{labels}}} {value}")

    for name, value in sorted(gauges.items()):
        metric = f"silkworm_{_metric_name(name)}"
        lines += [f"# TYPE {metric} gauge", f"{metric}{{{spider_label}}} {value}"]

    for name, value in sorted(custom.items()):
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            continue
        metric = f"silkworm_custom_{_metric_name(name)}"
        lines += [f"# TYPE {metric} gauge", f"{metric}{{{spider_label}}} {value}"]

    return "\n".join(lines) + "\n"


class MetricsServer:
    """Serve ``GET /metrics`` on a local TCP port while the crawl runs.

    Args:
        render: Callable producing the exposition text on each scrape.
        host: Interface to bind; defaults to loopback.
        port: TCP port; ``0`` picks a free port (see :attr:`port`).
    """

    def __init__(self, render: Callable[[], str], *, host: str, port: int) -> None:
        self._render = render
        self._host = host
        self._requested_port = port
        self._server: asyncio.Server | None = None
        self.logger: Logger = get_logger(component="metrics")

    @property
    def port(self) -> int | None:
        """Return the bound port once started."""
        if self._server is None or not self._server.sockets:
            return None
        return int(self._server.sockets[0].getsockname()[1])

    async def start(self) -> None:
        """Start listening for scrapes."""
        self._server = await asyncio.start_server(
            self._handle, self._host, self._requested_port
        )
        self.logger.info(
            "Serving Prometheus metrics",
            url=f"http://{self._host}:{self.port}/metrics",
        )

    async def close(self) -> None:
        """Stop listening and wait for open connections to finish."""
        if self._server is None:
            return
        self._server.close()
        await self._server.wait_closed()
        self._server = None

    async def _handle(
        self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter
    ) -> None:
        try:
            async with asyncio.timeout(5):
                request_line = await reader.readline()
                while (await reader.readline()).strip():
                    pass  # discard headers
            parts = request_line.decode("latin-1").split()
            path = parts[1] if len(parts) >= 2 else ""
            if (
                len(parts) >= 2
                and parts[0] == "GET"
                and path.split("?")[0]
                in {
                    "/metrics",
                    "/metrics/",
                }
            ):
                body = self._render().encode()
                status = "200 OK"
                content_type = "text/plain; version=0.0.4; charset=utf-8"
            else:
                body = b"Not Found\n"
                status = "404 Not Found"
                content_type = "text/plain; charset=utf-8"
            writer.write(
                f"HTTP/1.1 {status}\r\nContent-Type: {content_type}\r\n"
                f"Content-Length: {len(body)}\r\nConnection: close\r\n\r\n".encode()
                + body
            )
            await writer.drain()
        except (TimeoutError, OSError, UnicodeDecodeError):
            pass
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except OSError:
                pass
