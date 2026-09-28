# Limitations

Silkworm favors a small async crawling core. These constraints are intentional
or imposed by optional dependencies; the linked guides describe the available
workarounds.

## JavaScript is not rendered by default

The default wreq client downloads HTTP responses without executing JavaScript.
Use `ServoFetchClient` or a CDP-compatible browser such as Lightpanda, Chrome, or
Chromium when content exists only after client-side rendering. Standard HTTP is
usually faster and simpler when the initial response already contains the data.

See [ServoFetchClient and CDP Rendering](engine-and-http.md#servofetchclient).

## Request fingerprints omit headers and metadata

Default deduplication uses the HTTP method, canonical URL including `params`, and
request body. Headers and `Request.meta` are not part of the fingerprint.
Requests that differ only in headers or metadata are filtered unless you set
`dont_filter=True` or provide a custom `dedup_key`.

See [Deduplication](core-concepts.md#deduplication).

## Redirect destinations bypass scheduling filters

The HTTP client follows redirects after the engine applies `allowed_domains` and
other scheduling filters. A permitted URL can therefore redirect to another
host. Disable redirect following or validate the final `response.url` when that
boundary matters.

See [Redirect Behavior](engine-and-http.md#redirect-behavior).

## Response and document sizes are bounded

Downloaded bodies are limited by `max_response_size_bytes`—50 MB by default—and
HTML parsing is limited by `html_max_size_bytes` or `doc_max_size_bytes`—5 MB by
default. Increase the relevant limit deliberately or preprocess unusually large
documents.

See [Engine and HTTP Client](engine-and-http.md).

## Some pipelines buffer in memory

Polars, Excel, YAML, Avro, Vortex, S3 JSON Lines, FTP, SFTP, and RSS pipelines
buffer output until close. Prefer streaming destinations such as JSON Lines,
CSV, XML, or SQLite for long or unbounded crawls.

See [Streaming vs Buffered Pipelines](pipelines.md#streaming-vs-buffered-pipelines).

## Optional integrations have platform constraints

- Apache Iggy and Cassandra extras are currently limited to Python 3.13.
- On Python 3.15, the MsgPack, Vortex, and OnionLink extras install without their
  backing libraries until compatible wheels are published.
- Cassandra is unavailable on Windows.
- The Trio runner currently requires Python 3.13.
- Servo is distributed as separate `servofetch` wheels rather than a Silkworm
  package extra.

See [Requirements and Optional Extras](getting-started.md#requirements) for the
current compatibility matrix.
