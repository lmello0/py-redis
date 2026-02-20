from __future__ import annotations

import bisect
import math
from typing import NamedTuple

from store import StoreEntry, store_get

# ---------------------------------------------------------------------------
# Internal data structure
# ---------------------------------------------------------------------------


class SortedSet(NamedTuple):
    """
    scores : dict[member, score]   — O(1) lookup by member
    ranked : list[(score, member)] — kept sorted for range queries
    """

    scores: dict
    ranked: list


def _make_zset() -> SortedSet:
    return SortedSet({}, [])


def _wrong_type() -> str:
    return "WRONGTYPE Operation against a key holding the wrong kind of value"


def _get_zset(store: dict, key: str) -> SortedSet | None | str:
    entry = store_get(store, key)
    if entry is None:
        return None
    if not isinstance(entry.value, SortedSet):
        return _wrong_type()
    return entry.value


def _put_zset(store: dict, key: str, zset: SortedSet) -> dict:
    existing = store.get(key)
    expires_at = existing.expires_at if existing else None
    return {**store, key: StoreEntry(zset, expires_at)}


# ---------------------------------------------------------------------------
# Score boundary parsing  (shared by ZRANGEBYSCORE / ZREVRANGEBYSCORE / ZCOUNT)
# ---------------------------------------------------------------------------


def parse_score_bound(raw: str) -> tuple[float, bool]:
    """
    Parse a RESP score boundary string.
    Returns (value, exclusive).
      "-inf" → (-inf, False)
      "+inf" → (+inf, False)
      "5"    → (5.0,  False)
      "(5"   → (5.0,  True)
    """
    if raw == "-inf":
        return -math.inf, False
    if raw == "+inf":
        return math.inf, False
    exclusive = raw.startswith("(")
    value = float(raw[1:] if exclusive else raw)
    return value, exclusive


def _in_range(
    score: float,
    min_val: float,
    min_excl: bool,
    max_val: float,
    max_excl: bool,
) -> bool:
    lo_ok = score > min_val if min_excl else score >= min_val
    hi_ok = score < max_val if max_excl else score <= max_val
    return lo_ok and hi_ok


# ---------------------------------------------------------------------------
# Write operations  →  (new_store, result)
# ---------------------------------------------------------------------------


def zset_add(
    store: dict,
    key: str,
    member_score_pairs: list[tuple[str, float]],
    nx: bool = False,  # only add new
    xx: bool = False,  # only update existing
    gt: bool = False,  # only update if new score > current
    lt: bool = False,  # only update if new score < current
    ch: bool = False,  # return changed count instead of added count
) -> tuple[dict, int | str]:
    """
    ZADD key [NX|XX] [GT|LT] [CH] score member [score member ...]
    Returns number of *added* members (or changed, if CH).
    """
    zset = _get_zset(store, key)
    if isinstance(zset, str):
        return store, zset
    zset = zset or _make_zset()

    scores = dict(zset.scores)
    ranked = list(zset.ranked)
    added = changed = 0

    for member, score in member_score_pairs:
        existing_score = scores.get(member)
        is_new = existing_score is None

        if nx and not is_new:
            continue
        if xx and is_new:
            continue
        if gt and not is_new and score <= existing_score:
            continue
        if lt and not is_new and score >= existing_score:
            continue

        if is_new:
            added += 1
            changed += 1
        elif score != existing_score:
            changed += 1
            # Remove old ranked entry
            old_key = (existing_score, member)
            idx = bisect.bisect_left(ranked, old_key)
            if idx < len(ranked) and ranked[idx] == old_key:
                ranked.pop(idx)

        scores[member] = score
        bisect.insort(ranked, (score, member))

    new_zset = SortedSet(scores, ranked)
    new_store = _put_zset(store, key, new_zset)
    return new_store, changed if ch else added


def zset_rem(store: dict, key: str, *members: str) -> tuple[dict, int | str]:
    """ZREM key member [member ...] — returns count removed."""
    zset = _get_zset(store, key)
    if isinstance(zset, str):
        return store, zset
    if zset is None:
        return store, 0

    scores = dict(zset.scores)
    ranked = list(zset.ranked)
    removed = 0

    for member in members:
        score = scores.pop(member, None)
        if score is not None:
            removed += 1
            old_key = (score, member)
            idx = bisect.bisect_left(ranked, old_key)
            if idx < len(ranked) and ranked[idx] == old_key:
                ranked.pop(idx)

    if not scores:
        new_store = {k: v for k, v in store.items() if k != key}
        return new_store, removed

    return _put_zset(store, key, SortedSet(scores, ranked)), removed


def zset_incrby(store: dict, key: str, member: str, increment: float) -> tuple[dict, float | str]:
    """ZINCRBY key increment member — returns new score."""
    zset = _get_zset(store, key)
    if isinstance(zset, str):
        return store, zset
    zset = zset or _make_zset()

    current = zset.scores.get(member, 0.0)
    new_score = current + increment
    new_store, result = zset_add(store, key, [(member, new_score)])
    if isinstance(result, str):
        return store, result
    return new_store, new_score


# ---------------------------------------------------------------------------
# Read operations
# ---------------------------------------------------------------------------


def zset_score(store: dict, key: str, member: str) -> float | None | str:
    """ZSCORE key member."""
    zset = _get_zset(store, key)
    if isinstance(zset, str):
        return zset
    if zset is None:
        return None
    return zset.scores.get(member)


def zset_rank(store: dict, key: str, member: str, reverse: bool = False) -> int | None | str:
    """ZRANK / ZREVRANK key member."""
    zset = _get_zset(store, key)
    if isinstance(zset, str):
        return zset
    if zset is None or member not in zset.scores:
        return None
    score = zset.scores[member]
    idx = bisect.bisect_left(zset.ranked, (score, member))
    return (len(zset.ranked) - 1 - idx) if reverse else idx


def zset_card(store: dict, key: str) -> int | str:
    """ZCARD key."""
    zset = _get_zset(store, key)
    if isinstance(zset, str):
        return zset
    return len(zset.scores) if zset else 0


def zset_count(store: dict, key: str, min_raw: str, max_raw: str) -> int | str:
    """ZCOUNT key min max."""
    zset = _get_zset(store, key)
    if isinstance(zset, str):
        return zset
    if not zset:
        return 0
    try:
        min_val, min_excl = parse_score_bound(min_raw)
        max_val, max_excl = parse_score_bound(max_raw)
    except ValueError as e:
        return f"ERR {e}"

    return sum(1 for score, _ in zset.ranked if _in_range(score, min_val, min_excl, max_val, max_excl))


def zset_range_by_score(
    store: dict,
    key: str,
    min_raw: str,
    max_raw: str,
    with_scores: bool = False,
    offset: int = 0,
    count: int = -1,  # -1 means all
    reverse: bool = False,  # True = ZREVRANGEBYSCORE (max, min order)
) -> list[str] | str:
    """
    Core implementation for ZRANGEBYSCORE and ZREVRANGEBYSCORE.
    Returns a flat list: [member, ...] or [member, score, ...].
    """
    zset = _get_zset(store, key)
    if isinstance(zset, str):
        return zset
    if not zset:
        return []

    try:
        min_val, min_excl = parse_score_bound(min_raw)
        max_val, max_excl = parse_score_bound(max_raw)
    except ValueError as e:
        return f"ERR {e}"

    # Filter ranked list
    matching = [
        (score, member) for score, member in zset.ranked if _in_range(score, min_val, min_excl, max_val, max_excl)
    ]

    # ZREVRANGEBYSCORE returns highest score first
    if reverse:
        matching = list(reversed(matching))

    # Apply LIMIT
    if offset:
        matching = matching[offset:]
    if count != -1:
        matching = matching[:count]

    # Build output
    result = []
    for score, member in matching:
        result.append(member)
        if with_scores:
            result.append(_format_score(score))
    return result


def _format_score(score: float) -> str:
    """Format score the same way Redis does — integer scores have no decimal."""
    if math.isinf(score):
        return "+inf" if score > 0 else "-inf"
    if score == int(score):
        return str(int(score))
    return repr(score)


def zset_range(
    store: dict,
    key: str,
    start: int,
    stop: int,
    with_scores: bool = False,
    reverse: bool = False,
) -> list[str] | str:
    """ZRANGE / ZREVRANGE — by rank index."""
    zset = _get_zset(store, key)
    if isinstance(zset, str):
        return zset
    if not zset:
        return []

    ranked = zset.ranked
    length = len(ranked)

    # Normalise negative indexes
    if start < 0:
        start = max(0, length + start)
    if stop < 0:
        stop = length + stop
    stop = min(stop, length - 1)

    if start > stop:
        return []

    page = ranked[start : stop + 1]
    if reverse:
        page = list(reversed(page))

    result = []
    for score, member in page:
        result.append(member)
        if with_scores:
            result.append(_format_score(score))
    return result
