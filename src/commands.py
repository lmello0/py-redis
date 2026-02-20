# cache_server/commands.py
from __future__ import annotations

from typing import Callable

import state as _state_module
from protocol import BulkString, RESPArray, RESPError, RESPValue, SimpleString
from state import _lock
from store import (
    store_delete,
    store_exists,
    store_get,
    store_keys,
    store_set,
    store_ttl,
)

OK = SimpleString("OK")
PONG = SimpleString("PONG")


async def handle_ping(args: list[str]) -> RESPValue:
    return SimpleString(args[0]) if args else PONG


async def handle_set(args: list[str]) -> RESPValue:
    if len(args) < 2:
        return RESPError("ERR wrong number of arguments for SET")

    key, value = args[0], args[1]
    ttl = None

    opts = {args[i].upper(): args[i + 1] for i in range(2, len(args) - 1, 2)}
    if "EX" in opts:
        ttl = float(opts["EX"])
    elif "PX" in opts:
        ttl = float(opts["PX"]) / 1000

    async with _lock:
        _state_module._store = store_set(_state_module._store, key, value, ttl)
    return OK


async def handle_get(args: list[str]) -> RESPValue:
    if not args:
        return RESPError("ERR wrong number of arguments for GET")
    entry = store_get(_state_module._store, args[0])
    return BulkString(entry.value if entry else None)


async def handle_del(args: list[str]) -> RESPValue:
    async with _lock:
        _state_module._store, count = store_delete(_state_module._store, *args)
    return count


async def handle_exists(args: list[str]) -> RESPValue:
    return store_exists(_state_module._store, *args)


async def handle_keys(args: list[str]) -> RESPValue:
    pattern = args[0] if args else "*"
    keys = store_keys(_state_module._store, pattern)
    return RESPArray(tuple(BulkString(k) for k in keys))


async def handle_ttl(args: list[str]) -> RESPValue:
    if not args:
        return RESPError("ERR wrong number of arguments for TTL")
    return store_ttl(_state_module._store, args[0])


async def handle_incr(args: list[str]) -> RESPValue:
    if not args:
        return RESPError("ERR wrong number of arguments for INCR")
    key = args[0]
    async with _lock:
        entry = store_get(_state_module._store, key)
        try:
            new_val = int(entry.value if entry else 0) + 1
        except ValueError:
            return RESPError("ERR value is not an integer")
        _state_module._store = store_set(_state_module._store, key, str(new_val))
    return new_val


async def handle_expire(args: list[str]) -> RESPValue:
    if len(args) < 2:
        return RESPError("ERR wrong number of arguments for EXPIRE")
    key, seconds = args[0], int(args[1])
    async with _lock:
        entry = store_get(_state_module._store, key)
        if entry is None:
            return 0
        _state_module._store = store_set(_state_module._store, key, entry.value, float(seconds))
    return 1


COMMAND_REGISTRY: dict[str, Callable] = {
    "PING": handle_ping,
    "SET": handle_set,
    "GET": handle_get,
    "DEL": handle_del,
    "EXISTS": handle_exists,
    "KEYS": handle_keys,
    "TTL": handle_ttl,
    "INCR": handle_incr,
    "EXPIRE": handle_expire,
}

# Wire in hash commands — imported here (bottom) to avoid circular import
from hash_commands import HASH_COMMAND_REGISTRY  # noqa: E402

COMMAND_REGISTRY.update(HASH_COMMAND_REGISTRY)

from zset_commands import ZSET_COMMAND_REGISTRY  # noqa: E402

COMMAND_REGISTRY.update(ZSET_COMMAND_REGISTRY)


async def dispatch(raw_args: list[str]) -> RESPValue:
    if not raw_args:
        return RESPError("ERR empty command")
    cmd = raw_args[0].upper()
    handler = COMMAND_REGISTRY.get(cmd)
    if handler is None:
        return RESPError(f"ERR unknown command '{cmd}'")
    return await handler(raw_args[1:])
