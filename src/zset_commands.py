from __future__ import annotations

from typing import Callable

import state as _state_module
from protocol import BulkString, RESPArray, RESPError, RESPValue, SimpleString
from state import _lock
from zset_store import (
    zset_add,
    zset_card,
    zset_count,
    zset_incrby,
    zset_range,
    zset_range_by_score,
    zset_rank,
    zset_rem,
    zset_score,
)

OK = SimpleString("OK")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _err(msg: str) -> RESPError:
    return RESPError(msg)


def _bulk(v: str | None) -> BulkString:
    return BulkString(v)


def _to_array(items: list[str | None]) -> RESPArray:
    return RESPArray(tuple(_bulk(i) for i in items))


def _is_wrongtype(v) -> bool:
    return isinstance(v, str) and v.startswith("WRONGTYPE")


def _check(result) -> RESPError | None:
    return _err(result) if _is_wrongtype(result) else None


def _parse_withscores_limit(args: list[str]) -> tuple[bool, int, int, str | None]:
    """
    Parse optional [WITHSCORES] [LIMIT offset count] from a tail args list.
    Returns (with_scores, offset, count, error_msg).
    """
    with_scores = False
    offset = 0
    count = -1
    i = 0
    while i < len(args):
        opt = args[i].upper()
        if opt == "WITHSCORES":
            with_scores = True
            i += 1
        elif opt == "LIMIT":
            if i + 2 >= len(args):
                return False, 0, -1, "ERR syntax error"
            try:
                offset = int(args[i + 1])
                count = int(args[i + 2])
            except ValueError:
                return False, 0, -1, "ERR LIMIT values must be integers"
            i += 3
        else:
            return False, 0, -1, f"ERR syntax error near '{args[i]}'"
    return with_scores, offset, count, None


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


async def handle_zadd(args: list[str]) -> RESPValue:
    # ZADD key [NX|XX] [GT|LT] [CH] score member [score member ...]
    if len(args) < 3:
        return _err("ERR wrong number of arguments for ZADD")

    key = args[0]
    nx = xx = gt = lt = ch = False
    i = 1

    # Consume flags
    flags = {"NX", "XX", "GT", "LT", "CH"}
    while i < len(args) and args[i].upper() in flags:
        flag = args[i].upper()
        if flag == "NX":
            nx = True
        elif flag == "XX":
            xx = True
        elif flag == "GT":
            gt = True
        elif flag == "LT":
            lt = True
        elif flag == "CH":
            ch = True
        i += 1

    if nx and xx:
        return _err("ERR XX and NX options at the same time are not compatible")
    if gt and lt:
        return _err("ERR GT and LT options at the same time are not compatible")
    if nx and (gt or lt):
        return _err("ERR GT, LT, and NX options at the same time are not compatible")

    tail = args[i:]
    if len(tail) < 2 or len(tail) % 2 != 0:
        return _err("ERR syntax error")

    try:
        pairs = [(tail[j + 1], float(tail[j])) for j in range(0, len(tail), 2)]
    except ValueError:
        return _err("ERR value is not a valid float")

    async with _lock:
        new_store, result = zset_add(
            _state_module._store,
            key,
            pairs,
            nx=nx,
            xx=xx,
            gt=gt,
            lt=lt,
            ch=ch,
        )
        _state_module._store = new_store

    if isinstance(result, str):
        return _err(result)
    return result  # int


async def handle_zrem(args: list[str]) -> RESPValue:
    if len(args) < 2:
        return _err("ERR wrong number of arguments for ZREM")
    key, *members = args
    async with _lock:
        new_store, result = zset_rem(_state_module._store, key, *members)
        _state_module._store = new_store
    if isinstance(result, str):
        return _err(result)
    return result


async def handle_zincrby(args: list[str]) -> RESPValue:
    if len(args) != 3:
        return _err("ERR wrong number of arguments for ZINCRBY")
    key, increment_raw, member = args
    try:
        increment = float(increment_raw)
    except ValueError:
        return _err("ERR value is not a valid float")
    async with _lock:
        new_store, result = zset_incrby(_state_module._store, key, member, increment)
        _state_module._store = new_store
    if isinstance(result, str):
        return _err(result)
    return _bulk(str(result))


async def handle_zscore(args: list[str]) -> RESPValue:
    if len(args) != 2:
        return _err("ERR wrong number of arguments for ZSCORE")
    result = zset_score(_state_module._store, args[0], args[1])
    if _is_wrongtype(result):
        return _err(result)
    return _bulk(str(result) if result is not None else None)


async def handle_zrank(args: list[str]) -> RESPValue:
    if len(args) != 2:
        return _err("ERR wrong number of arguments for ZRANK")
    result = zset_rank(_state_module._store, args[0], args[1], reverse=False)
    if _is_wrongtype(result):
        return _err(result)
    return result if result is not None else _bulk(None)


async def handle_zrevrank(args: list[str]) -> RESPValue:
    if len(args) != 2:
        return _err("ERR wrong number of arguments for ZREVRANK")
    result = zset_rank(_state_module._store, args[0], args[1], reverse=True)
    if _is_wrongtype(result):
        return _err(result)
    return result if result is not None else _bulk(None)


async def handle_zcard(args: list[str]) -> RESPValue:
    if len(args) != 1:
        return _err("ERR wrong number of arguments for ZCARD")
    result = zset_card(_state_module._store, args[0])
    if isinstance(result, str):
        return _err(result)
    return result


async def handle_zcount(args: list[str]) -> RESPValue:
    if len(args) != 3:
        return _err("ERR wrong number of arguments for ZCOUNT")
    result = zset_count(_state_module._store, args[0], args[1], args[2])
    if isinstance(result, str):
        return _err(result)
    return result


async def handle_zrangebyscore(args: list[str]) -> RESPValue:
    # ZRANGEBYSCORE key min max [WITHSCORES] [LIMIT offset count]
    if len(args) < 3:
        return _err("ERR wrong number of arguments for ZRANGEBYSCORE")
    key, min_raw, max_raw = args[0], args[1], args[2]

    with_scores, offset, count, err = _parse_withscores_limit(args[3:])
    if err:
        return _err(err)

    result = zset_range_by_score(
        _state_module._store,
        key,
        min_raw,
        max_raw,
        with_scores=with_scores,
        offset=offset,
        count=count,
        reverse=False,
    )
    if isinstance(result, str):
        return _err(result)
    return _to_array(result)


async def handle_zrevrangebyscore(args: list[str]) -> RESPValue:
    # ZREVRANGEBYSCORE key max min [WITHSCORES] [LIMIT offset count]
    # Note: argument order is reversed — max comes before min
    if len(args) < 3:
        return _err("ERR wrong number of arguments for ZREVRANGEBYSCORE")
    key, max_raw, min_raw = args[0], args[1], args[2]  # <-- swapped

    with_scores, offset, count, err = _parse_withscores_limit(args[3:])
    if err:
        return _err(err)

    result = zset_range_by_score(
        _state_module._store,
        key,
        min_raw,
        max_raw,
        with_scores=with_scores,
        offset=offset,
        count=count,
        reverse=True,
    )
    if isinstance(result, str):
        return _err(result)
    return _to_array(result)


async def handle_zrange(args: list[str]) -> RESPValue:
    # ZRANGE key start stop [WITHSCORES]
    if len(args) < 3:
        return _err("ERR wrong number of arguments for ZRANGE")
    key = args[0]
    try:
        start, stop = int(args[1]), int(args[2])
    except ValueError:
        return _err("ERR value is not an integer or out of range")

    with_scores = len(args) > 3 and args[3].upper() == "WITHSCORES"
    result = zset_range(_state_module._store, key, start, stop, with_scores=with_scores)
    if isinstance(result, str):
        return _err(result)
    return _to_array(result)


async def handle_zrevrange(args: list[str]) -> RESPValue:
    # ZREVRANGE key start stop [WITHSCORES]
    if len(args) < 3:
        return _err("ERR wrong number of arguments for ZREVRANGE")
    key = args[0]
    try:
        start, stop = int(args[1]), int(args[2])
    except ValueError:
        return _err("ERR value is not an integer or out of range")

    with_scores = len(args) > 3 and args[3].upper() == "WITHSCORES"
    result = zset_range(_state_module._store, key, start, stop, with_scores=with_scores, reverse=True)
    if isinstance(result, str):
        return _err(result)
    return _to_array(result)


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

ZSET_COMMAND_REGISTRY: dict[str, Callable] = {
    "ZADD": handle_zadd,
    "ZREM": handle_zrem,
    "ZINCRBY": handle_zincrby,
    "ZSCORE": handle_zscore,
    "ZRANK": handle_zrank,
    "ZREVRANK": handle_zrevrank,
    "ZCARD": handle_zcard,
    "ZCOUNT": handle_zcount,
    "ZRANGEBYSCORE": handle_zrangebyscore,
    "ZREVRANGEBYSCORE": handle_zrevrangebyscore,
    "ZRANGE": handle_zrange,
    "ZREVRANGE": handle_zrevrange,
}
