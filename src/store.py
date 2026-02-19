from __future__ import annotations

import time
from typing import Any, NamedTuple, Optional


class StoreEntry(NamedTuple):
    value: Any
    expires_at: Optional[float]


def make_store() -> dict[str, StoreEntry]:
    return {}


def store_get(store: dict, key: str) -> Optional[StoreEntry]:
    entry = store.get(key)

    if entry is None:
        return None

    if entry.expires_at is not None and time.time() > entry.expires_at:
        return None

    return entry


def store_set(
    store: dict,
    key: str,
    value: Any,
    ttl_seconds: Optional[float] = None,
) -> dict:
    expires_at = (time.time() + ttl_seconds) if ttl_seconds is not None else None
    return {**store, key: StoreEntry(value, expires_at)}


def store_delete(store: dict, *keys: str) -> tuple[dict, int]:
    deleted = sum(1 for k in keys if k in store)
    new_store = {k: v for k, v in store.items() if k not in keys}

    return new_store, deleted


def store_exists(store: dict, *keys: str) -> int:
    return sum(1 for k in keys if store_get(store, k) is not None)


def store_keys(store: dict, pattern: str = "*") -> list[str]:
    import fnmatch

    now = time.time()

    return [k for k, e in store.items() if fnmatch.fnmatch(k, pattern) and (e.expires_at is None or e.expires_at > now)]


def store_ttl(store: dict, key: str) -> int:
    entry = store.get(key)

    if entry is None:
        return -2

    if entry.expires_at is None:
        return -1

    remaining = entry.expires_at - time.time()
    return max(0, int(remaining))
