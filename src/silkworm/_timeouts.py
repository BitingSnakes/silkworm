from __future__ import annotations

from datetime import timedelta


def to_seconds(timeout: float | timedelta | None) -> float | None:
    """Normalize a timeout given in seconds or as a ``timedelta`` to seconds."""
    if timeout is None:
        return None
    if isinstance(timeout, timedelta):
        return timeout.total_seconds()
    return float(timeout)
