from __future__ import annotations


def require_positive_int(value: int, name: str) -> None:
    if value <= 0:
        msg = f"{name} must be positive"
        raise ValueError(msg)
