from __future__ import annotations

import argparse
import json
from pathlib import Path
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
from urllib.parse import urlsplit

from silkworm import CookiesMiddleware, Response, Spider, run_spider
from silkworm.middlewares import RequestMiddleware, ResponseMiddleware
from silkworm.pipelines import ItemPipeline, JsonLinesPipeline


class CaptureCookiesSpider(Spider):
    name = "capture_cookies"

    async def parse(self, response: Response):
        yield {
            "phase": "capture",
            "url": response.url,
            "status": response.status,
            "set_cookie": response.headers.get("set-cookie"),
        }


class ReuseCookiesSpider(Spider):
    name = "reuse_cookies"

    async def parse(self, response: Response):
        try:
            payload = json.loads(response.text)
        except json.JSONDecodeError:
            payload = {"body": response.text[:500]}

        yield {
            "phase": "reuse",
            "url": response.url,
            "status": response.status,
            "payload": payload,
            "sent_cookie": response.request.headers.get("Cookie"),
        }


class CookieDemoHandler(BaseHTTPRequestHandler):
    server_version = "SilkwormCookieDemo/1.0"

    def do_GET(self) -> None:
        match urlsplit(self.path).path:
            case "/login":
                body = {
                    "message": "cookie issued",
                    "cookie": "demo_session=silkworm",
                }
                self._send_json(
                    body,
                    headers={"Set-Cookie": "demo_session=silkworm; Path=/"},
                )
            case "/cookies":
                self._send_json(
                    {
                        "message": "cookie echoed",
                        "received_cookie": self.headers.get("Cookie"),
                    },
                )
            case _:
                self.send_error(404)

    def log_message(self, format: str, *args: object) -> None:
        return

    def _send_json(
        self,
        payload: dict[str, str | None],
        *,
        headers: dict[str, str] | None = None,
    ) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for name, value in (headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)


class CookieDemoServer:
    def __init__(self) -> None:
        self._server = ThreadingHTTPServer(("127.0.0.1", 0), CookieDemoHandler)
        self._thread = Thread(target=self._server.serve_forever, daemon=True)

    @property
    def base_url(self) -> str:
        host, port = self._server.server_address
        return f"http://{host}:{port}"

    def __enter__(self) -> CookieDemoServer:
        self._thread.start()
        return self

    def __exit__(self, *exc_info: object) -> None:
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=5)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Capture cookies in one spider run and reuse them in another."
    )
    parser.add_argument(
        "--capture-url",
        default=None,
        help="URL that returns a Set-Cookie header. Defaults to a local demo server.",
    )
    parser.add_argument(
        "--reuse-url",
        default=None,
        help="URL to request after loading cookies. Defaults to a local demo server.",
    )
    parser.add_argument(
        "--cookie-file",
        default="data/cookies.txt",
        help="Netscape/Mozilla cookie file path.",
    )
    parser.add_argument(
        "--output",
        default="data/cookie_reuse.jl",
        help="Output JSON Lines path for both spider runs.",
    )
    return parser.parse_args()


def run_cookie_reuse_demo(
    *,
    capture_url: str,
    reuse_url: str,
    cookie_file: Path,
    output_path: Path,
) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.unlink(missing_ok=True)
    cookie_file.unlink(missing_ok=True)

    capture_cookies = CookiesMiddleware()
    request_middlewares: list[RequestMiddleware] = [capture_cookies]
    response_middlewares: list[ResponseMiddleware] = [capture_cookies]
    item_pipelines: list[ItemPipeline] = [
        JsonLinesPipeline(output_path, use_opendal=False),
    ]

    run_spider(
        CaptureCookiesSpider,
        start_urls=[capture_url],
        request_middlewares=request_middlewares,
        response_middlewares=response_middlewares,
        item_pipelines=item_pipelines,
        concurrency=1,
        request_timeout=10,
    )
    capture_cookies.save(cookie_file)

    reuse_cookies = CookiesMiddleware()
    reuse_cookies.load(cookie_file)

    run_spider(
        ReuseCookiesSpider,
        start_urls=[reuse_url],
        request_middlewares=[reuse_cookies],
        response_middlewares=[reuse_cookies],
        item_pipelines=[JsonLinesPipeline(output_path, use_opendal=False)],
        concurrency=1,
        request_timeout=10,
    )


def main() -> None:
    args = parse_args()
    cookie_file = Path(args.cookie_file)
    output_path = Path(args.output)

    if args.capture_url and args.reuse_url:
        run_cookie_reuse_demo(
            capture_url=args.capture_url,
            reuse_url=args.reuse_url,
            cookie_file=cookie_file,
            output_path=output_path,
        )
        return

    if args.capture_url or args.reuse_url:
        msg = "Pass both --capture-url and --reuse-url, or neither for the local demo."
        raise SystemExit(msg)

    with CookieDemoServer() as server:
        run_cookie_reuse_demo(
            capture_url=f"{server.base_url}/login",
            reuse_url=f"{server.base_url}/cookies",
            cookie_file=cookie_file,
            output_path=output_path,
        )


if __name__ == "__main__":
    main()
