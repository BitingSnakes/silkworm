"""Command-line interface: ``silkworm crawl``, ``parse``, ``fetch``, ``version``.

Examples::

    silkworm crawl examples/quotes_spider.py -o data/quotes.jl -s max_items=50
    silkworm crawl myproject.spiders:QuotesSpider -o - -a category=books
    silkworm parse https://quotes.toscrape.com/ --spider examples/quotes_spider.py
    silkworm fetch https://example.com/ --headers

A spider reference is a Python file or an importable module, optionally
followed by ``:ClassName`` when the module defines several spiders.

Exit codes: ``0`` success, ``1`` failure policy violated or a callback failed
(``parse``), ``2`` usage or spider-loading error, ``130`` interrupted.
"""

from __future__ import annotations

import argparse
import asyncio
import importlib
import importlib.util
import inspect
import json
import os
import re
import sys
from importlib.metadata import PackageNotFoundError, version
from pathlib import Path
from typing import TYPE_CHECKING, cast

if TYPE_CHECKING:
    from collections.abc import Callable, Sequence
    from types import ModuleType

    from ._types import JSONValue
    from .pipelines import ItemPipeline
    from .request import Callback
    from .spiders import Spider

EXIT_OK = 0
EXIT_FAILED = 1
EXIT_USAGE = 2
EXIT_INTERRUPTED = 130

_WINDOWS_DRIVE = re.compile(r"^[A-Za-z]:[\\/]")


class CliError(Exception):
    """A user-facing command-line error (exit code 2)."""


def _package_version() -> str:
    try:
        return version("silkworm-rs")
    except PackageNotFoundError:
        return "unknown"


def _split_reference(reference: str) -> tuple[str, str | None]:
    """Split ``path_or_module[:ClassName]`` without breaking Windows drives."""
    head, colon, tail = reference.rpartition(":")
    if not colon or not head:
        return reference, None
    # "C:\\spiders\\quotes.py" has only the drive colon: no class given.
    if _WINDOWS_DRIVE.match(reference) and ":" not in head:
        return reference, None
    return head, tail or None


def _load_module(target: str) -> ModuleType:
    looks_like_path = (
        target.endswith(".py")
        or os.sep in target
        or "/" in target
        or Path(target).exists()
    )
    if not looks_like_path:
        try:
            return importlib.import_module(target)
        except ImportError as exc:
            raise CliError(f"Cannot import spider module {target!r}: {exc}") from exc

    path = Path(target).resolve()
    if not path.is_file():
        raise CliError(f"Spider file not found: {target}")
    # Make sibling modules importable, as `python path/to/spider.py` would.
    sys.path.insert(0, str(path.parent))
    module_name = f"_silkworm_spider_{abs(hash(str(path)))}"
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise CliError(f"Cannot load spider file: {target}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    except Exception as exc:
        raise CliError(f"Error while loading {target}: {exc!r}") from exc
    return module


def load_spider_class(reference: str) -> type[Spider]:
    """Return the spider class named by ``path_or_module[:ClassName]``.

    Without ``ClassName`` the module must define exactly one spider class
    (imported spiders are ignored); otherwise the choices are listed.

    Raises:
        CliError: If the module cannot be loaded or no single spider matches.
    """
    from .spiders import Spider

    target, class_name = _split_reference(reference)
    module = _load_module(target)
    if class_name is not None:
        candidate = getattr(module, class_name, None)
        if not (inspect.isclass(candidate) and issubclass(candidate, Spider)):
            raise CliError(f"{class_name!r} in {target} is not a Spider subclass")
        return candidate

    spiders = [
        obj
        for obj in vars(module).values()
        if inspect.isclass(obj)
        and issubclass(obj, Spider)
        and obj is not Spider
        and obj.__module__ == module.__name__
    ]
    if len(spiders) == 1:
        return spiders[0]
    if not spiders:
        raise CliError(f"No Spider subclass defined in {target}")
    names = ", ".join(sorted(cls.__name__ for cls in spiders))
    raise CliError(
        f"{target} defines several spiders ({names}); use {target}:ClassName"
    )


def _parse_pairs(pairs: Sequence[str], flag: str) -> dict[str, str]:
    parsed: dict[str, str] = {}
    for pair in pairs:
        name, sep, value = pair.partition("=")
        if not sep or not name.strip():
            raise CliError(f"{flag} expects NAME=VALUE, got {pair!r}")
        parsed[name.strip()] = value
    return parsed


def _build_spider(cls: type[Spider], arguments: dict[str, str]) -> Spider:
    # Spider subclasses define their own keyword arguments; -a passes strings.
    factory = cast("Callable[..., Spider]", cls)
    try:
        return factory(**arguments)
    except TypeError as exc:
        raise CliError(
            f"Cannot create {cls.__name__} with -a arguments: {exc}"
        ) from exc


class _StdoutPipeline:
    """Write each item to stdout as one JSON line."""

    async def open(self, spider: Spider) -> None:
        return None

    async def close(self, spider: Spider) -> None:
        sys.stdout.flush()

    async def process_item(self, item: JSONValue, spider: Spider) -> JSONValue:
        sys.stdout.write(json.dumps(item, ensure_ascii=False, default=str) + "\n")
        return item


def output_pipeline(target: str) -> ItemPipeline:
    """Return the pipeline writing items to ``target`` (``-`` means stdout).

    The format follows the file extension: ``.jl``/``.jsonl``/``.ndjson``,
    ``.csv``, ``.xml``, ``.db``/``.sqlite``/``.sqlite3``, ``.msgpack``,
    ``.parquet``, ``.yaml``/``.yml``, or ``.xlsx``.
    """
    from . import pipelines

    if target == "-":
        return _StdoutPipeline()
    path = Path(target)
    path.parent.mkdir(parents=True, exist_ok=True)
    match path.suffix.lower():
        case ".jl" | ".jsonl" | ".ndjson":
            return pipelines.JsonLinesPipeline(path)
        case ".csv":
            return pipelines.CSVPipeline(path)
        case ".xml":
            return pipelines.XMLPipeline(path)
        case ".db" | ".sqlite" | ".sqlite3":
            return pipelines.SQLitePipeline(path)
        case ".msgpack":
            return pipelines.MsgPackPipeline(path)
        case ".parquet":
            return pipelines.PolarsPipeline(path)
        case ".yaml" | ".yml":
            return pipelines.YAMLPipeline(path)
        case ".xlsx":
            return pipelines.ExcelPipeline(path)
        case suffix:
            raise CliError(
                f"Unsupported output format {suffix or '(none)'!r} for {target}; use "
                ".jl, .jsonl, .csv, .xml, .db, .msgpack, .parquet, .yaml, .xlsx, or -"
            )


def _loop_factory(name: str) -> object | None:
    from . import runner

    match name:
        case "asyncio":
            return None
        case "uvloop":
            return runner._install_uvloop()
        case "rsloop":
            return runner._install_rsloop()
        case "winloop":
            return runner._install_winloop()
        case _:
            raise CliError(f"Unknown event loop {name!r}")


def _cmd_crawl(args: argparse.Namespace) -> int:
    from .exceptions import CrawlFailedError
    from .runner import run_spider
    from .settings import coerce_setting

    settings: dict[str, object] = {}
    for name, raw in _parse_pairs(args.set, "-s").items():
        try:
            settings[name] = coerce_setting(name, raw, source="-s")
        except (KeyError, ValueError) as exc:
            raise CliError(str(exc).strip('"')) from exc
    if args.job_dir:
        settings["job_dir"] = coerce_setting(
            "job_dir", args.job_dir, source="--job-dir"
        )
    if args.http_cache:
        settings["http_cache"] = coerce_setting(
            "http_cache", args.http_cache, source="--http-cache"
        )
    try:
        loop_factory = _loop_factory(args.loop)
    except ImportError as exc:
        raise CliError(str(exc)) from exc

    spider = _build_spider(load_spider_class(args.spider), _parse_pairs(args.arg, "-a"))
    if args.output:
        settings["item_pipelines"] = [output_pipeline(target) for target in args.output]

    try:
        result = run_spider(
            spider,
            loop_factory=cast("object", loop_factory),  # type: ignore[arg-type]
            **settings,  # type: ignore[arg-type]
        )
    except CrawlFailedError as exc:
        _print_summary(exc.result)
        for failure in exc.result.failures:
            print(f"silkworm: crawl failed: {failure}", file=sys.stderr)
        return EXIT_FAILED
    except KeyboardInterrupt:
        print("silkworm: crawl interrupted", file=sys.stderr)
        return EXIT_INTERRUPTED
    _print_summary(result)
    return EXIT_INTERRUPTED if result.close_reason == "shutdown" else EXIT_OK


def _print_summary(result: object) -> None:
    from ._stats import CrawlResult

    if not isinstance(result, CrawlResult):
        return
    print(
        f"silkworm: {result.spider} finished ({result.close_reason}) in "
        f"{result.elapsed_seconds:.1f}s: {result.items_scraped} items, "
        f"{result.requests_sent} requests, {result.errors} errors",
        file=sys.stderr,
    )


def _cmd_parse(args: argparse.Namespace) -> int:
    from .http import HttpClient
    from .request import Request
    from .response import HTMLResponse
    from .testing import run_callback

    spider = _build_spider(load_spider_class(args.spider), _parse_pairs(args.arg, "-a"))
    callback_name = args.callback or "parse"
    found = getattr(spider, callback_name, None)
    if not callable(found):
        raise CliError(f"{type(spider).__name__} has no callback {callback_name!r}")
    callback = cast("Callback", found)
    try:
        meta = cast("dict[str, JSONValue]", json.loads(args.meta)) if args.meta else {}
    except json.JSONDecodeError as exc:
        raise CliError(f"--meta must be a JSON object: {exc}") from exc

    async def run() -> int:
        await spider.open()
        try:
            async with HttpClient(timeout=args.timeout) as client:
                request = Request(url=args.url, callback=callback, meta=meta)
                response = await client.fetch(request)
            if callback_name == "parse" and not isinstance(response, HTMLResponse):
                # The engine always hands parse() an HTMLResponse.
                response = HTMLResponse(
                    url=response.url,
                    status=response.status,
                    headers=response.headers,
                    body=response.body,
                    request=response.request,
                )
            print(
                f"silkworm: fetched {response.url} ({response.status}), "
                f"running {callback_name}()",
                file=sys.stderr,
            )
            try:
                result = await run_callback(callback, response)
            except Exception as exc:  # noqa: BLE001 - reported to the user
                print(f"silkworm: {callback_name}() failed: {exc!r}", file=sys.stderr)
                return EXIT_FAILED
        finally:
            await spider.close()
        for item in result.items:
            print(
                json.dumps(
                    {"type": "item", "item": item}, ensure_ascii=False, default=str
                )
            )
        for request in result.requests:
            print(
                json.dumps(
                    {
                        "type": "request",
                        "url": request.url,
                        "method": request.method,
                        "callback": getattr(request.callback, "__name__", None),
                        "priority": request.priority,
                    }
                )
            )
        print(
            f"silkworm: {len(result.items)} items, {len(result.requests)} requests",
            file=sys.stderr,
        )
        return EXIT_OK

    return asyncio.run(run())


def _cmd_fetch(args: argparse.Namespace) -> int:
    from .http import HttpClient
    from .request import Request

    async def run() -> int:
        async with HttpClient(timeout=args.timeout) as client:
            response = await client.fetch(Request(url=args.url))
        print(f"silkworm: {response.status} {response.url}", file=sys.stderr)
        if args.headers:
            for name, value in sorted(response.headers.items()):
                print(f"{name}: {value}", file=sys.stderr)
        if args.output:
            Path(args.output).write_bytes(response.body)
            print(
                f"silkworm: saved {len(response.body)} bytes to {args.output}",
                file=sys.stderr,
            )
        else:
            sys.stdout.buffer.write(response.body)
            sys.stdout.flush()
        return EXIT_OK

    return asyncio.run(run())


def build_parser() -> argparse.ArgumentParser:
    """Return the ``silkworm`` argument parser."""
    parser = argparse.ArgumentParser(
        prog="silkworm", description="Run and debug silkworm spiders."
    )
    parser.add_argument("--version", action="version", version=_package_version())
    parser.add_argument(
        "--log-level",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="log level (default: SILKWORM_LOG_LEVEL or INFO)",
    )
    commands = parser.add_subparsers(dest="command", required=True)

    crawl = commands.add_parser("crawl", help="run a spider")
    crawl.add_argument("spider", help="spider file or module, optionally :ClassName")
    crawl.add_argument(
        "-o",
        "--output",
        action="append",
        default=[],
        help="write items to FILE (format from extension; '-' for stdout); repeatable",
    )
    crawl.add_argument(
        "-s",
        "--set",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="engine setting, e.g. -s concurrency=8 -s max_items=100; repeatable",
    )
    crawl.add_argument(
        "-a",
        "--arg",
        action="append",
        default=[],
        metavar="NAME=VALUE",
        help="spider constructor argument; repeatable",
    )
    crawl.add_argument("--job-dir", help="persist crawl state here to pause and resume")
    crawl.add_argument(
        "--http-cache", metavar="DIR", help="cache responses on disk in DIR"
    )
    crawl.add_argument(
        "--loop",
        choices=["asyncio", "uvloop", "rsloop", "winloop"],
        default="asyncio",
        help="event loop implementation (default: asyncio)",
    )
    crawl.set_defaults(handler=_cmd_crawl)

    parse = commands.add_parser("parse", help="run one URL through a spider callback")
    parse.add_argument("url")
    parse.add_argument(
        "--spider", required=True, help="spider file or module[:ClassName]"
    )
    parse.add_argument("-c", "--callback", help="callback method name (default: parse)")
    parse.add_argument("-a", "--arg", action="append", default=[], metavar="NAME=VALUE")
    parse.add_argument("--meta", help="request meta as a JSON object")
    parse.add_argument(
        "--timeout", type=float, default=30.0, help="seconds (default 30)"
    )
    parse.set_defaults(handler=_cmd_parse)

    fetch = commands.add_parser("fetch", help="download a URL and print its body")
    fetch.add_argument("url")
    fetch.add_argument("--headers", action="store_true", help="print response headers")
    fetch.add_argument(
        "-o", "--output", help="save the body to a file instead of stdout"
    )
    fetch.add_argument(
        "--timeout", type=float, default=30.0, help="seconds (default 30)"
    )
    fetch.set_defaults(handler=_cmd_fetch)

    version_cmd = commands.add_parser("version", help="print the silkworm version")
    version_cmd.set_defaults(handler=lambda _args: print(_package_version()) or EXIT_OK)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    """Run the command line and return its exit code."""
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.log_level:
        os.environ["SILKWORM_LOG_LEVEL"] = args.log_level
    try:
        return int(args.handler(args))
    except CliError as exc:
        print(f"silkworm: error: {exc}", file=sys.stderr)
        return EXIT_USAGE
    except KeyboardInterrupt:
        print("silkworm: interrupted", file=sys.stderr)
        return EXIT_INTERRUPTED


if __name__ == "__main__":  # pragma: no cover
    sys.exit(main())
