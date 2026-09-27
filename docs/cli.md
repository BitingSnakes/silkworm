# Command Line

Installing `silkworm-rs` provides a `silkworm` command for running and debugging
spiders without writing a runner script.

```bash
silkworm --version
silkworm --log-level DEBUG crawl ...   # --log-level applies to every command
```

## Spider references

Commands that take a spider accept a Python file or an importable module,
optionally followed by `:ClassName`:

```bash
silkworm crawl examples/quotes_spider.py
silkworm crawl myproject.spiders.news
silkworm crawl myproject/spiders.py:NewsSpider
```

Without a class name the module must define exactly one spider class (spiders it
imports are ignored). Files are loaded with their directory on `sys.path`, so
sibling modules can be imported.

## `silkworm crawl`

```bash
silkworm crawl SPIDER [-o FILE]... [-s NAME=VALUE]... [-a NAME=VALUE]...
               [--job-dir DIR] [--http-cache DIR] [--loop LOOP]
```

- `-o FILE` writes items to a file; the format follows the extension: `.jl`,
  `.jsonl`, `.ndjson`, `.csv`, `.xml`, `.db`/`.sqlite`/`.sqlite3`, `.msgpack`,
  `.parquet`, `.yaml`/`.yml`, or `.xlsx`. Use `-o -` for JSON lines on stdout.
  Repeat `-o` to write several formats; parent directories are created.
- `-s NAME=VALUE` sets an engine option (any scalar option, see
  [Settings](production.md#settings)), e.g. `-s concurrency=8 -s max_items=100`.
  These override `SILKWORM_*` environment variables and `custom_settings`.
- `-a NAME=VALUE` passes a keyword argument to the spider's constructor (as a
  string).
- `--job-dir DIR` persists state so an interrupted crawl resumes
  ([details](production.md#pausing-and-resuming)).
- `--http-cache DIR` caches responses on disk
  ([details](production.md#http-cache-for-development)).
- `--loop` picks `asyncio` (default), `uvloop`, `rsloop`, or `winloop`.

The first Ctrl+C or SIGTERM stops gracefully; a second one cancels immediately.
A summary line is printed to stderr at the end.

```bash
silkworm crawl examples/quotes_spider.py -o data/quotes.jl -o data/quotes.csv \
    -s max_error_rate=0.1 -s min_items=50 --job-dir state/quotes
```

## `silkworm parse`

Fetches one URL, runs it through a spider callback, and prints what the callback
produced as JSON lines (`{"type": "item", ...}` and `{"type": "request", ...}`).
Handy while writing selectors:

```bash
silkworm parse https://quotes.toscrape.com/ --spider examples/quotes_spider.py
silkworm parse https://example.com/product/1 --spider shop.py -c parse_product \
    --meta '{"category": "books"}'
```

The spider's `open()`/`close()` hooks run around the call, and `parse` receives an
`HTMLResponse` just as in a crawl.

## `silkworm fetch`

Downloads a URL with the default client and writes the body to stdout (or a file
with `-o`); `--headers` prints the status and headers to stderr.

```bash
silkworm fetch https://example.com/ --headers -o page.html
```

## Exit codes

| Code | Meaning |
| --- | --- |
| `0` | Success. |
| `1` | The crawl violated its failure policy (`max_error_rate`, `min_items`, `max_item_drop_rate`), or `parse`'s callback raised. |
| `2` | Usage error: bad arguments or settings, or a spider that cannot be loaded. |
| `130` | Interrupted by a signal. |
