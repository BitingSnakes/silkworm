"""SQLite-backed crawl state that lets an interrupted crawl resume.

The job directory holds one SQLite database with three tables:

* ``state``: job metadata (spider name and status).
* ``seen``: 16-byte digests of every deduplication key scheduled so far.
* ``pending``: every request that was scheduled but not yet fully processed.

Requests are written to ``pending`` when they enter the queue and deleted only
after their callback (and every request it scheduled) finished, so after a
crash or stop each unfinished request is still stored and is crawled again on
resume (at-least-once delivery). Writes are committed in small batches; at most
the last batch is lost on a hard crash, which again only causes re-crawling.
"""

from __future__ import annotations

import base64
import json
import sqlite3
import time
from collections.abc import Iterable, Mapping
from contextlib import closing
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, cast

from ._types import JSONValue
from .exceptions import SpiderError
from .request import Request

if TYPE_CHECKING:
    import os

    from ._types import BodyData, QueryParams
    from .request import Callback, Errback
    from .spiders import Spider

JOB_FILE_NAME = "job.sqlite3"
_SCHEMA = """
CREATE TABLE IF NOT EXISTS state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
CREATE TABLE IF NOT EXISTS seen (digest BLOB PRIMARY KEY) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS pending (
    seq INTEGER PRIMARY KEY,
    priority INTEGER NOT NULL,
    payload TEXT NOT NULL
);
"""


class JobState:
    """Persistent seen-set and pending-request journal for one spider.

    Args:
        directory: Job directory; created when missing.
        spider: Spider whose callbacks resolve restored requests.
        commit_every: Commit after this many writes.
        commit_interval: Also commit when this many seconds passed since the
            previous commit.

    Raises:
        ValueError: If the directory holds state for a different spider.
    """

    def __init__(
        self,
        directory: str | os.PathLike[str],
        spider: Spider,
        *,
        commit_every: int = 500,
        commit_interval: float = 1.0,
    ) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self._spider = spider
        self._commit_every = commit_every
        self._commit_interval = commit_interval
        self._pending_writes = 0
        self._last_commit = time.monotonic()
        self._db = sqlite3.connect(self.directory / JOB_FILE_NAME)
        self._execute("PRAGMA journal_mode=WAL")
        self._execute("PRAGMA synchronous=NORMAL")
        with closing(self._db.executescript(_SCHEMA)):
            pass
        self.resumed = self._start()

    def _execute(self, sql: str, parameters: tuple[object, ...] = ()) -> int:
        with closing(self._db.execute(sql, parameters)) as cursor:
            return cursor.rowcount

    def _fetchone(
        self, sql: str, parameters: tuple[object, ...] = ()
    ) -> tuple[object, ...] | None:
        with closing(self._db.execute(sql, parameters)) as cursor:
            return cast("tuple[object, ...] | None", cursor.fetchone())

    def _fetchall(
        self, sql: str, parameters: tuple[object, ...] = ()
    ) -> list[tuple[object, ...]]:
        with closing(self._db.execute(sql, parameters)) as cursor:
            return cast("list[tuple[object, ...]]", cursor.fetchall())

    def _state(self, key: str) -> str | None:
        row = self._fetchone("SELECT value FROM state WHERE key = ?", (key,))
        return None if row is None else str(row[0])

    def _set_state(self, key: str, value: str) -> None:
        self._execute(
            "INSERT INTO state (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )

    def _start(self) -> bool:
        stored_spider = self._state("spider")
        if stored_spider is not None and stored_spider != self._spider.name:
            self._db.close()
            msg = (
                f"Job directory {self.directory} belongs to spider "
                f"{stored_spider!r}, not {self._spider.name!r}"
            )
            raise ValueError(msg)
        resumed = self._state("status") in {"running", "paused"}
        if not resumed:
            self._execute("DELETE FROM seen")
            self._execute("DELETE FROM pending")
        self._set_state("spider", self._spider.name)
        self._set_state("status", "running")
        self._db.commit()
        return resumed

    @property
    def status(self) -> str | None:
        """Return the stored job status (``running``, ``paused``, ``finished``)."""
        return self._state("status")

    def seen_add(self, digest: bytes) -> bool:
        """Record ``digest``; return ``False`` when it was already recorded."""
        inserted = (
            self._execute("INSERT OR IGNORE INTO seen (digest) VALUES (?)", (digest,))
            == 1
        )
        if inserted:
            self._wrote()
        return inserted

    def seen_count(self) -> int:
        """Return the number of recorded deduplication digests."""
        row = self._fetchone("SELECT COUNT(*) FROM seen")
        assert row is not None
        return int(cast("int", row[0]))

    def add_pending(self, seq: int, request: Request, payload: str) -> None:
        """Journal a queued request under its queue sequence number."""
        self._execute(
            "INSERT OR REPLACE INTO pending (seq, priority, payload) VALUES (?, ?, ?)",
            (seq, request.priority, payload),
        )
        self._wrote()

    def remove_pending(self, seq: int) -> None:
        """Forget a request after it was fully processed."""
        self._execute("DELETE FROM pending WHERE seq = ?", (seq,))
        self._wrote()

    def pending_count(self) -> int:
        """Return the number of journaled, unfinished requests."""
        row = self._fetchone("SELECT COUNT(*) FROM pending")
        assert row is not None
        return int(cast("int", row[0]))

    def load_pending(self) -> list[tuple[int, Request]]:
        """Return journaled requests in their original queue order."""
        rows = self._fetchall("SELECT seq, payload FROM pending ORDER BY seq")
        return [
            (int(cast("int", seq)), self.deserialize(str(payload)))
            for seq, payload in rows
        ]

    def _wrote(self) -> None:
        self._pending_writes += 1
        now = time.monotonic()
        if (
            self._pending_writes >= self._commit_every
            or now - self._last_commit >= self._commit_interval
        ):
            self.commit()

    def commit(self) -> None:
        """Flush buffered writes to disk."""
        self._db.commit()
        self._pending_writes = 0
        self._last_commit = time.monotonic()

    def close(self, *, finished: bool) -> None:
        """Commit and close; ``finished`` resets the job for the next run."""
        try:
            if finished:
                self._execute("DELETE FROM pending")
            self._set_state("status", "finished" if finished else "paused")
            self.commit()
        finally:
            self._db.close()

    # -- request serialization -------------------------------------------------

    def serialize(self, request: Request) -> str:
        """Encode ``request`` as JSON, or raise if it cannot be restored later.

        Raises:
            SpiderError: If a callback or errback is not a method of the spider,
                or the request carries values JSON cannot represent.
        """
        payload: dict[str, JSONValue] = {
            "url": request.url,
            "method": request.method,
            "headers": cast("JSONValue", dict(request.headers)),
            "params": _encode_params(request.params),
            "data": _encode_body(request.data),
            "json": request.json,
            "meta": cast("JSONValue", request.meta),
            "timeout": _encode_timeout(request.timeout),
            "callback": self._method_name(request.callback, "callback", request),
            "errback": self._method_name(request.errback, "errback", request),
            "dont_filter": request.dont_filter,
            "priority": request.priority,
        }
        try:
            return json.dumps(payload, separators=(",", ":"))
        except (TypeError, ValueError) as exc:
            msg = (
                f"Request {request.url} cannot be saved to the job directory: {exc}. "
                "With job_dir, request meta, params, and bodies must be "
                "JSON-serializable"
            )
            raise SpiderError(msg) from exc

    def deserialize(self, payload: str) -> Request:
        """Rebuild a request saved by :meth:`serialize`."""
        data = cast("dict[str, object]", json.loads(payload))
        return Request(
            url=str(data["url"]),
            method=str(data["method"]),
            headers=cast("dict[str, str]", data["headers"]),
            params=cast("QueryParams", data["params"]),
            data=_decode_body(data["data"]),
            json=cast("JSONValue", data["json"]),
            meta=cast("dict[str, JSONValue]", data["meta"]),
            timeout=cast("float | None", data["timeout"]),
            callback=cast("Callback | None", self._resolve_method(data["callback"])),
            errback=cast("Errback | None", self._resolve_method(data["errback"])),
            dont_filter=bool(data["dont_filter"]),
            priority=int(cast("int", data["priority"])),
        )

    def _method_name(self, func: object, kind: str, request: Request) -> str | None:
        if func is None:
            return None
        name = getattr(func, "__name__", None)
        bound_self = getattr(func, "__self__", None)
        if (
            bound_self is not self._spider
            or not isinstance(name, str)
            or getattr(self._spider, name, None) != func
        ):
            msg = (
                f"Request {request.url} has a {kind} that is not a method of "
                f"spider {self._spider.name!r}; with job_dir, callbacks and "
                "errbacks must be spider methods so they can be restored"
            )
            raise SpiderError(msg)
        return name

    def _resolve_method(self, name: object) -> object:
        if name is None:
            return None
        method = getattr(self._spider, str(name), None)
        if method is None or not callable(method):
            msg = (
                f"Saved request refers to missing spider method {name!r}; "
                f"delete {self.directory} to start the job fresh"
            )
            raise SpiderError(msg)
        return method


def _encode_timeout(timeout: float | timedelta | None) -> float | None:
    if isinstance(timeout, timedelta):
        return timeout.total_seconds()
    return timeout


def _encode_params(params: QueryParams) -> JSONValue:
    encoded: dict[str, JSONValue] = {}
    for key, value in params.items():
        if isinstance(value, (str, int, float, bool)) or value is None:
            encoded[key] = value
        else:
            encoded[key] = [cast("JSONValue", item) for item in value]
    return encoded


def _encode_body(data: BodyData) -> JSONValue:
    if data is None:
        return None
    if isinstance(data, (bytes, bytearray, memoryview)):
        return {"bytes": base64.b64encode(bytes(data)).decode("ascii")}
    if isinstance(data, str):
        return {"text": data}
    if isinstance(data, Mapping):
        return {"form": cast("JSONValue", dict(data))}
    items = list(cast("Iterable[object]", data))
    if all(isinstance(item, tuple) for item in items):
        return {"pairs": [list(cast("tuple[str, str]", item)) for item in items]}
    return {"list": cast("JSONValue", items)}


def _decode_body(raw: object) -> BodyData:
    if raw is None:
        return None
    body = cast("dict[str, object]", raw)
    if "bytes" in body:
        return base64.b64decode(str(body["bytes"]))
    if "text" in body:
        return str(body["text"])
    if "form" in body:
        return cast("dict[str, JSONValue]", body["form"])
    if "pairs" in body:
        return [
            (str(key), str(value))
            for key, value in cast("list[list[object]]", body["pairs"])
        ]
    return cast("list[JSONValue]", body["list"])
