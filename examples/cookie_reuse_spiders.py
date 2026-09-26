"""
Save cookies from one crawl and reuse them in a later crawl.

A common case: you log in once, save the session cookie to a file, and later
crawls load that file so they are already logged in.

`CookiesMiddleware` does the work:
- It remembers cookies from `Set-Cookie` response headers.
- It sends them back in the `Cookie` header of later requests.
- `.save(path)` writes them to a file, `.load(path)` reads them back.

The demo runs two spiders one after the other:

    1. CaptureCookiesSpider  visits /login   -> server sets a cookie -> save to file
    2. ReuseCookiesSpider    visits /cookies -> load file, send cookie -> server echoes it

By default it starts a tiny local web server, so it works offline.

How to run:
    python examples/cookie_reuse_spiders.py

    # Or against your own URLs:
    python examples/cookie_reuse_spiders.py --capture-url https://... --reuse-url https://...

Output:
    data/cookie_reuse.jl  (one item per spider)
    data/cookies.txt      (the saved cookies)
"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from threading import Thread

from silkworm import CookiesMiddleware, Response, Spider, run_spider
from silkworm.pipelines import JsonLinesPipeline

# ---------------------------------------------------------------------------
# The two spiders
# ---------------------------------------------------------------------------


class CaptureCookiesSpider(Spider):
    """Visits a page that sets a cookie."""

    name = "capture_cookies"

    async def parse(self, response: Response):
        yield {
            "phase": "capture",
            "url": response.url,
            "status": response.status,
            "set_cookie": response.headers.get("set-cookie"),
        }


class ReuseCookiesSpider(Spider):
    """Visits a page and records which cookie was sent."""

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


def run_cookie_reuse_demo(
    capture_url: str,
    reuse_url: str,
    cookie_file: Path,
    output_path: Path,
) -> None:
    # Start fresh: remove files from a previous run.
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.unlink(missing_ok=True)
    cookie_file.unlink(missing_ok=True)

    # --- Crawl 1: capture the cookie ---
    cookies = CookiesMiddleware()
    run_spider(
        CaptureCookiesSpider(start_urls=[capture_url]),
        # The middleware must see requests (to send cookies) AND
        # responses (to store new cookies), so add it to both lists.
        request_middlewares=[cookies],
        response_middlewares=[cookies],
        item_pipelines=[JsonLinesPipeline(output_path, use_opendal=False)],
        concurrency=1,
        request_timeout=10,
    )
    cookies.save(cookie_file)
    print(f"Saved cookies to {cookie_file}")

    # --- Crawl 2: a brand-new middleware, loaded from the file ---
    saved_cookies = CookiesMiddleware()
    saved_cookies.load(cookie_file)
    run_spider(
        ReuseCookiesSpider(start_urls=[reuse_url]),
        request_middlewares=[saved_cookies],
        response_middlewares=[saved_cookies],
        item_pipelines=[JsonLinesPipeline(output_path, use_opendal=False)],
        concurrency=1,
        request_timeout=10,
    )
    print(f"Results written to {output_path}")


# ---------------------------------------------------------------------------
# A tiny local web server, so the demo works without the internet.
# You don't need to understand this part to use cookies in your own spiders.
# ---------------------------------------------------------------------------


class CookieDemoHandler(BaseHTTPRequestHandler):
    """Answers two URLs: /login sets a cookie, /cookies shows what it received."""

    def do_GET(self) -> None:
        if self.path == "/login":
            self.send_json(
                {"message": "cookie issued"},
                extra_headers={"Set-Cookie": "demo_session=silkworm; Path=/"},
            )
        elif self.path == "/cookies":
            self.send_json(
                {
                    "message": "cookie echoed",
                    "received_cookie": self.headers.get("Cookie"),
                }
            )
        else:
            self.send_error(404)

    def send_json(
        self,
        payload: dict[str, str | None],
        extra_headers: dict[str, str] | None = None,
    ) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        for name, value in (extra_headers or {}).items():
            self.send_header(name, value)
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: object) -> None:
        pass  # Keep the console quiet.


def run_with_local_server(cookie_file: Path, output_path: Path) -> None:
    # Port 0 means "pick any free port".
    server = ThreadingHTTPServer(("127.0.0.1", 0), CookieDemoHandler)
    port = server.server_address[1]
    base_url = f"http://127.0.0.1:{port}"

    # Run the server in a background thread so our spiders can talk to it.
    thread = Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        run_cookie_reuse_demo(
            capture_url=f"{base_url}/login",
            reuse_url=f"{base_url}/cookies",
            cookie_file=cookie_file,
            output_path=output_path,
        )
    finally:
        # Always stop the server, even if the crawl failed.
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)


def main() -> None:
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
    args = parser.parse_args()

    cookie_file = Path(args.cookie_file)
    output_path = Path(args.output)

    if args.capture_url and args.reuse_url:
        run_cookie_reuse_demo(
            args.capture_url, args.reuse_url, cookie_file, output_path
        )
    elif args.capture_url or args.reuse_url:
        raise SystemExit(
            "Pass both --capture-url and --reuse-url, or neither for the local demo."
        )
    else:
        run_with_local_server(cookie_file, output_path)


if __name__ == "__main__":
    main()
