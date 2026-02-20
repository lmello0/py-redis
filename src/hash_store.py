from __future__ import annotations

import fnmatch
import math
import random

from store import StoreEntry, store_get

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _wrong_type() -> str:
    return "WRONGTYPE Operation against a key holding the wrong kind of value"


def _get_hash(store: dict, key: str) -> dict | None | str:
    """
    Returns the inner hash dict, None if the key is absent, or an error
    string if the key holds a non-hash value.
    """
    entry = store_get(store, key)
    if entry is None:
        return None
    if not isinstance(entry.value, dict):
        return _wrong_type()
    return entry.value


def _put_hash(store: dict, key: str, inner: dict) -> dict:
    """Write (or overwrite) a hash entry, preserving existing TTL."""
    existing = store.get(key)
    expires_at = existing.expires_at if existing else None
    return {**store, key: StoreEntry(inner, expires_at)}


# ---------------------------------------------------------------------------
# Write operations  →  return (new_store, result)
# ---------------------------------------------------------------------------


def hash_set(
    store: dict,
    key: str,
    field_value_pairs: list[tuple[str, str]],
) -> tuple[dict, int | str]:
    """
    HSET key field value [field value ...]
    Returns number of *new* fields added (updates don't count).
    """
    inner = _get_hash(store, key)
    if isinstance(inner, str):
        return store, inner
    inner = dict(inner) if inner else {}

    added = sum(1 for f, _ in field_value_pairs if f not in inner)
    for field, value in field_value_pairs:
        inner[field] = value

    return _put_hash(store, key, inner), added


def hash_setnx(
    store: dict,
    key: str,
    field: str,
    value: str,
) -> tuple[dict, int | str]:
    """
    HSETNX key field value
    Sets field only if it does not exist. Returns 1 if set, 0 otherwise.
    """
    inner = _get_hash(store, key)
    if isinstance(inner, str):
        return store, inner
    inner = dict(inner) if inner else {}

    if field in inner:
        return store, 0
    inner[field] = value
    return _put_hash(store, key, inner), 1


def hash_del(
    store: dict,
    key: str,
    *fields: str,
) -> tuple[dict, int | str]:
    """
    HDEL key field [field ...]
    Returns number of fields actually deleted.
    """
    inner = _get_hash(store, key)
    if isinstance(inner, str):
        return store, inner
    if inner is None:
        return store, 0

    inner = dict(inner)
    deleted = sum(1 for f in fields if f in inner)
    for f in fields:
        inner.pop(f, None)

    # If hash is now empty, remove the key entirely
    if not inner:
        new_store = {k: v for k, v in store.items() if k != key}
        return new_store, deleted

    return _put_hash(store, key, inner), deleted


def hash_incrby(
    store: dict,
    key: str,
    field: str,
    increment: int,
) -> tuple[dict, int | str]:
    """
    HINCRBY key field increment
    Increments the integer value of a field.
    """
    inner = _get_hash(store, key)
    if isinstance(inner, str):
        return store, inner
    inner = dict(inner) if inner else {}

    current = inner.get(field, "0")
    try:
        new_val = int(current) + increment
    except ValueError:
        return store, "ERR hash value is not an integer"

    inner[field] = str(new_val)
    return _put_hash(store, key, inner), new_val


def hash_incrbyfloat(
    store: dict,
    key: str,
    field: str,
    increment: float,
) -> tuple[dict, float | str]:
    """
    HINCRBYFLOAT key field increment
    Increments the float value of a field.
    """
    inner = _get_hash(store, key)
    if isinstance(inner, str):
        return store, inner
    inner = dict(inner) if inner else {}

    current = inner.get(field, "0")
    try:
        new_val = float(current) + increment
    except ValueError:
        return store, "ERR hash value is not a float"

    if math.isnan(new_val) or math.isinf(new_val):
        return store, "ERR increment would produce NaN or Infinity"

    # Redis trims unnecessary trailing zeros
    formatted = f"{new_val:.17g}"
    inner[field] = formatted
    return _put_hash(store, key, inner), formatted


# ---------------------------------------------------------------------------
# Read operations  →  return result only (store is unchanged)
# ---------------------------------------------------------------------------


def hash_get(store: dict, key: str, field: str) -> str | None:
    """HGET key field — returns value or None."""
    inner = _get_hash(store, key)
    if inner is None or isinstance(inner, str):
        return inner  # None or error string
    return inner.get(field)


def hash_mget(store: dict, key: str, *fields: str) -> list[str | None] | str:
    """HMGET key field [field ...] — returns list with None for missing fields."""
    inner = _get_hash(store, key)
    if isinstance(inner, str):
        return inner
    if inner is None:
        return [None] * len(fields)  # type: ignore
    return [inner.get(f) for f in fields]


def hash_exists(store: dict, key: str, field: str) -> int | str:
    """HEXISTS key field — 1 if field exists, 0 otherwise."""
    inner = _get_hash(store, key)
    if isinstance(inner, str):
        return inner
    if inner is None:
        return 0
    return 1 if field in inner else 0


def hash_len(store: dict, key: str) -> int | str:
    """HLEN key — number of fields."""
    inner = _get_hash(store, key)
    if isinstance(inner, str):
        return inner
    return len(inner) if inner else 0


def hash_strlen(store: dict, key: str, field: str) -> int | str:
    """HSTRLEN key field — length of field value string."""
    inner = _get_hash(store, key)
    if isinstance(inner, str):
        return inner
    if inner is None:
        return 0
    return len(inner.get(field, ""))


def hash_keys(store: dict, key: str) -> list[str] | str:
    """HKEYS key — list of all field names."""
    inner = _get_hash(store, key)
    if isinstance(inner, str):
        return inner
    return list(inner.keys()) if inner else []


def hash_vals(store: dict, key: str) -> list[str] | str:
    """HVALS key — list of all values."""
    inner = _get_hash(store, key)
    if isinstance(inner, str):
        return inner
    return list(inner.values()) if inner else []


def hash_getall(store: dict, key: str) -> list[str] | str:
    """
    HGETALL key — flat list alternating field, value (Redis wire format).
    """
    inner = _get_hash(store, key)
    if isinstance(inner, str):
        return inner
    if not inner:
        return []
    result = []
    for f, v in inner.items():
        result.append(f)
        result.append(v)
    return result


def hash_randfield(
    store: dict,
    key: str,
    count: int | None = None,
    with_values: bool = False,
) -> str | list[str] | None:
    """
    HRANDFIELD key [count [WITHVALUES]]
    - No count: returns one random field (or nil if empty).
    - Positive count: up to `count` distinct fields.
    - Negative count: exactly |count| fields, allowing repeats.
    """
    inner = _get_hash(store, key)
    if isinstance(inner, str):
        return inner
    if not inner:
        return None if count is None else []

    fields = list(inner.keys())

    if count is None:
        chosen = [random.choice(fields)]
    elif count >= 0:
        chosen = random.sample(fields, min(count, len(fields)))
    else:
        chosen = random.choices(fields, k=abs(count))

    if not with_values:
        return chosen if count is not None else chosen[0]

    result = []
    for f in chosen:
        result.append(f)
        result.append(inner[f])
    return result


def hash_scan(
    store: dict,
    key: str,
    cursor: int,
    match: str = "*",
    count: int = 10,
) -> tuple[int, list[str]] | str:
    """
    HSCAN key cursor [MATCH pattern] [COUNT count]
    Simplified full-scan implementation (cursor is always 0 on completion).
    Returns (next_cursor, [field, value, ...]).
    """
    inner = _get_hash(store, key)
    if isinstance(inner, str):
        return inner
    if not inner:
        return 0, []

    all_items = [(f, v) for f, v in inner.items() if fnmatch.fnmatch(f, match)]

    # Simple stateless scan: cursor 0 starts, we page by `count`, return 0
    # when done.  For a real implementation use cursor as an offset index.
    start = cursor
    end = start + count
    page = all_items[start:end]
    next_cursor = end if end < len(all_items) else 0

    flat = []
    for f, v in page:
        flat.append(f)
        flat.append(v)

    return next_cursor, flat
