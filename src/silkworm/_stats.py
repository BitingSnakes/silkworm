"""Crawl counters and the immutable result returned by a finished crawl."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from collections.abc import Mapping

    from ._types import JSONValue

# Counters every crawl reports, even when they stay at zero.
BASE_COUNTERS = (
    "requests_sent",
    "responses_received",
    "items_scraped",
    "items_dropped",
    "errors",
    "retries",
    "dupe_filtered",
    "offsite_filtered",
    "depth_filtered",
    "ignored_requests",
    "dropped_requests",
)

# Counters broken down by a label, e.g. ``responses_by_status["200"]``.
BASE_LABELED_COUNTERS = (
    "responses_by_status",
    "requests_by_domain",
    "errors_by_type",
    "items_dropped_by_reason",
    "ignored_by_reason",
)


# Distinct labels kept per labeled counter before folding into OTHER_LABEL.
MAX_LABELS = 1000
OTHER_LABEL = "_other"


class CrawlStats:
    """Mutable counters collected by the engine while a crawl runs.

    ``counters`` holds plain totals; ``labeled`` holds per-label breakdowns such
    as responses per HTTP status or requests per domain.
    """

    __slots__ = ("counters", "labeled")

    def __init__(self) -> None:
        self.counters: dict[str, int] = dict.fromkeys(BASE_COUNTERS, 0)
        self.labeled: dict[str, Counter[str]] = {
            name: Counter() for name in BASE_LABELED_COUNTERS
        }

    def inc(self, name: str, value: int = 1) -> None:
        """Add ``value`` to counter ``name``."""
        self.counters[name] = self.counters.get(name, 0) + value

    def inc_labeled(self, name: str, label: str, value: int = 1) -> None:
        """Add ``value`` to counter ``name`` for ``label``.

        Each counter keeps at most :data:`MAX_LABELS` distinct labels; further
        labels (e.g. the long tail of domains in a broad crawl) are summed
        under :data:`OTHER_LABEL` to bound memory and metrics cardinality.
        """
        counter = self.labeled.setdefault(name, Counter())
        if label not in counter and len(counter) >= MAX_LABELS:
            label = OTHER_LABEL
        counter[label] += value

    def get(self, name: str) -> int:
        """Return the current value of counter ``name`` (``0`` when unset)."""
        return self.counters.get(name, 0)


@dataclass(slots=True, frozen=True)
class CrawlResult:
    """Outcome of a finished crawl, returned by ``Engine.run()`` and runners.

    Attributes:
        spider: Spider name.
        close_reason: Why the crawl ended: ``"finished"`` when the queue
            drained, ``"shutdown"`` after a stop request or signal, a limit
            name such as ``"max_items"``, or the reason passed to
            :class:`~silkworm.exceptions.CloseSpider`.
        elapsed_seconds: Wall-clock crawl duration.
        stats: Final counters (see :data:`BASE_COUNTERS`).
        labeled_stats: Per-label breakdowns (see :data:`BASE_LABELED_COUNTERS`).
        custom_stats: A copy of the spider's ``stats_payload``.
        failures: Failure-policy violations; empty when the crawl succeeded.
    """

    spider: str
    close_reason: str
    elapsed_seconds: float
    stats: Mapping[str, int]
    labeled_stats: Mapping[str, Mapping[str, int]]
    custom_stats: Mapping[str, JSONValue] = field(
        default_factory=lambda: MappingProxyType({})
    )
    failures: tuple[str, ...] = ()

    @property
    def ok(self) -> bool:
        """Return whether the crawl met its failure policy."""
        return not self.failures

    @property
    def requests_sent(self) -> int:
        """Return the number of requests handed to the HTTP client."""
        return self.stats.get("requests_sent", 0)

    @property
    def responses_received(self) -> int:
        """Return the number of responses received."""
        return self.stats.get("responses_received", 0)

    @property
    def items_scraped(self) -> int:
        """Return the number of items that passed every pipeline."""
        return self.stats.get("items_scraped", 0)

    @property
    def items_dropped(self) -> int:
        """Return the number of items discarded by pipelines or limits."""
        return self.stats.get("items_dropped", 0)

    @property
    def errors(self) -> int:
        """Return the number of unrecovered request or callback failures."""
        return self.stats.get("errors", 0)

    @property
    def error_rate(self) -> float:
        """Return ``errors / requests_sent`` (``0.0`` when nothing was sent)."""
        sent = self.requests_sent
        return self.errors / sent if sent else 0.0

    @property
    def item_drop_rate(self) -> float:
        """Return ``items_dropped / (items_scraped + items_dropped)``."""
        total = self.items_scraped + self.items_dropped
        return self.items_dropped / total if total else 0.0


def freeze_result(
    *,
    spider: str,
    close_reason: str,
    elapsed_seconds: float,
    stats: CrawlStats,
    custom_stats: Mapping[str, JSONValue],
    failures: tuple[str, ...],
) -> CrawlResult:
    """Build an immutable :class:`CrawlResult` snapshot of ``stats``."""
    return CrawlResult(
        spider=spider,
        close_reason=close_reason,
        elapsed_seconds=round(elapsed_seconds, 3),
        stats=MappingProxyType(dict(stats.counters)),
        labeled_stats=MappingProxyType(
            {
                name: MappingProxyType(dict(counter))
                for name, counter in stats.labeled.items()
            }
        ),
        custom_stats=MappingProxyType(dict(custom_stats)),
        failures=failures,
    )
