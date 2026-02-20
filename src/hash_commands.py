from __future__ import annotations

from typing import Callable

from hash_store import (
    hash_del,
    hash_exists,
    hash_get,
    hash_getall,
    hash_incrby,
    hash_incrbyfloat,
    hash_keys,
    hash_len,
    hash_mget,
    hash_randfield,
    hash_scan,
    hash_set,
    hash_setnx,
    hash_strlen,
    hash_vals,
)
from protocol import BulkString, RESPArray, RESPError, RESPValue, SimpleString
from state import _lock
import state as _state_module

OK = SimpleString("OK")


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _err(msg: str) -> RESPError:
    return RESPError(msg)


def _bulk(v: str | None) -> BulkString:
    return BulkString(v)


def _to_array(items: list[str | None]) -> RESPArray:
    return RESPArray(tuple(_bulk(i) for i in items))


def _wrong_type_check(result) -> RESPError | None:
    """If result is an error string (from _get_hash), wrap it."""
    if isinstance(result, str) and result.startswith("WRONGTYPE"):
        return RESPError(result)
    return None


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------


async def handle_hset(args: list[str]) -> RESPValue:
    # HSET key field value [field value ...]
    if len(args) < 3 or len(args) % 2 == 0:
        return _err("ERR wrong number of arguments for HSET")
    key = args[0]
    pairs = [(args[i], args[i + 1]) for i in range(1, len(args), 2)]
    async with _lock:
        new_store, result = hash_set(_state_module._store, key, pairs)
        _state_module._store = new_store
    if isinstance(result, str):
        return _err(result)
    return result  # int: number of new fields added


async def handle_hmset(args: list[str]) -> RESPValue:
    # HMSET key field value [field value ...] — deprecated alias, returns OK
    if len(args) < 3 or len(args) % 2 == 0:
        return _err("ERR wrong number of arguments for HMSET")
    key = args[0]
    pairs = [(args[i], args[i + 1]) for i in range(1, len(args), 2)]
    async with _lock:
        new_store, result = hash_set(_state_module._store, key, pairs)
        _state_module._store = new_store
    if isinstance(result, str):
        return _err(result)
    return OK


async def handle_hsetnx(args: list[str]) -> RESPValue:
    # HSETNX key field value
    if len(args) != 3:
        return _err("ERR wrong number of arguments for HSETNX")
    key, field, value = args
    async with _lock:
        new_store, result = hash_setnx(_state_module._store, key, field, value)
        _state_module._store = new_store
    if isinstance(result, str):
        return _err(result)
    return result  # 0 or 1


async def handle_hget(args: list[str]) -> RESPValue:
    # HGET key field
    if len(args) != 2:
        return _err("ERR wrong number of arguments for HGET")
    result = hash_get(_state_module._store, args[0], args[1])
    if isinstance(result, str) and result.startswith("WRONGTYPE"):
        return _err(result)
    return _bulk(result)


async def handle_hmget(args: list[str]) -> RESPValue:
    # HMGET key field [field ...]
    if len(args) < 2:
        return _err("ERR wrong number of arguments for HMGET")
    result = hash_mget(_state_module._store, args[0], *args[1:])
    if isinstance(result, str):
        return _err(result)
    return _to_array(result)


async def handle_hdel(args: list[str]) -> RESPValue:
    # HDEL key field [field ...]
    if len(args) < 2:
        return _err("ERR wrong number of arguments for HDEL")
    key, *fields = args
    async with _lock:
        new_store, result = hash_del(_state_module._store, key, *fields)
        _state_module._store = new_store
    if isinstance(result, str):
        return _err(result)
    return result  # int


async def handle_hexists(args: list[str]) -> RESPValue:
    # HEXISTS key field
    if len(args) != 2:
        return _err("ERR wrong number of arguments for HEXISTS")
    result = hash_exists(_state_module._store, args[0], args[1])
    if isinstance(result, str):
        return _err(result)
    return result  # 0 or 1


async def handle_hlen(args: list[str]) -> RESPValue:
    if len(args) != 1:
        return _err("ERR wrong number of arguments for HLEN")
    result = hash_len(_state_module._store, args[0])
    if isinstance(result, str):
        return _err(result)
    return result


async def handle_hstrlen(args: list[str]) -> RESPValue:
    if len(args) != 2:
        return _err("ERR wrong number of arguments for HSTRLEN")
    result = hash_strlen(_state_module._store, args[0], args[1])
    if isinstance(result, str):
        return _err(result)
    return result


async def handle_hkeys(args: list[str]) -> RESPValue:
    if len(args) != 1:
        return _err("ERR wrong number of arguments for HKEYS")
    result = hash_keys(_state_module._store, args[0])
    if isinstance(result, str):
        return _err(result)
    return _to_array(result)


async def handle_hvals(args: list[str]) -> RESPValue:
    if len(args) != 1:
        return _err("ERR wrong number of arguments for HVALS")
    result = hash_vals(_state_module._store, args[0])
    if isinstance(result, str):
        return _err(result)
    return _to_array(result)


async def handle_hgetall(args: list[str]) -> RESPValue:
    if len(args) != 1:
        return _err("ERR wrong number of arguments for HGETALL")
    result = hash_getall(_state_module._store, args[0])
    if isinstance(result, str):
        return _err(result)
    return _to_array(result)


async def handle_hincrby(args: list[str]) -> RESPValue:
    # HINCRBY key field increment
    if len(args) != 3:
        return _err("ERR wrong number of arguments for HINCRBY")
    key, field = args[0], args[1]
    try:
        increment = int(args[2])
    except ValueError:
        return _err("ERR value is not an integer or out of range")

    async with _lock:
        new_store, result = hash_incrby(_state_module._store, key, field, increment)
        _state_module._store = new_store
    if isinstance(result, str):
        return _err(result)
    return result  # int


async def handle_hincrbyfloat(args: list[str]) -> RESPValue:
    # HINCRBYFLOAT key field increment
    if len(args) != 3:
        return _err("ERR wrong number of arguments for HINCRBYFLOAT")
    key, field = args[0], args[1]
    try:
        increment = float(args[2])
    except ValueError:
        return _err("ERR value is not a valid float")

    async with _lock:
        new_store, result = hash_incrbyfloat(_state_module._store, key, field, increment)
        _state_module._store = new_store
    if isinstance(result, str):
        return _err(result)
    return _bulk(result)  # Redis returns float as bulk string


async def handle_hrandfield(args: list[str]) -> RESPValue:
    # HRANDFIELD key [count [WITHVALUES]]
    if not args:
        return _err("ERR wrong number of arguments for HRANDFIELD")

    key = args[0]
    count = None
    with_values = False

    if len(args) >= 2:
        try:
            count = int(args[1])
        except ValueError:
            return _err("ERR value is not an integer or out of range")
    if len(args) >= 3 and args[2].upper() == "WITHVALUES":
        with_values = True

    result = hash_randfield(_state_module._store, key, count, with_values)

    if isinstance(result, str) and result.startswith("WRONGTYPE"):
        return _err(result)
    if result is None:
        return _bulk(None)
    if isinstance(result, list):
        return _to_array(result)
    return _bulk(result)  # single field, no count given


async def handle_hscan(args: list[str]) -> RESPValue:
    # HSCAN key cursor [MATCH pattern] [COUNT count]
    if len(args) < 2:
        return _err("ERR wrong number of arguments for HSCAN")

    key = args[0]
    try:
        cursor = int(args[1])
    except ValueError:
        return _err("ERR cursor is not an integer")

    match_pattern = "*"
    count = 10
    i = 2
    while i < len(args):
        opt = args[i].upper()
        if opt == "MATCH" and i + 1 < len(args):
            match_pattern = args[i + 1]
            i += 2
        elif opt == "COUNT" and i + 1 < len(args):
            try:
                count = int(args[i + 1])
            except ValueError:
                return _err("ERR COUNT is not an integer")
            i += 2
        else:
            i += 1

    result = hash_scan(_state_module._store, key, cursor, match_pattern, count)
    if isinstance(result, str):
        return _err(result)

    next_cursor, flat = result
    return RESPArray(
        (
            _bulk(str(next_cursor)),
            _to_array(flat),
        )
    )


# ---------------------------------------------------------------------------
# Registry — plug these into the main COMMAND_REGISTRY in commands.py
# ---------------------------------------------------------------------------

HASH_COMMAND_REGISTRY: dict[str, Callable] = {
    "HSET": handle_hset,
    "HMSET": handle_hmset,
    "HSETNX": handle_hsetnx,
    "HGET": handle_hget,
    "HMGET": handle_hmget,
    "HDEL": handle_hdel,
    "HEXISTS": handle_hexists,
    "HLEN": handle_hlen,
    "HSTRLEN": handle_hstrlen,
    "HKEYS": handle_hkeys,
    "HVALS": handle_hvals,
    "HGETALL": handle_hgetall,
    "HINCRBY": handle_hincrby,
    "HINCRBYFLOAT": handle_hincrbyfloat,
    "HRANDFIELD": handle_hrandfield,
    "HSCAN": handle_hscan,
}
