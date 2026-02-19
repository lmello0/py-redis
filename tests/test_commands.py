import pytest

import store
from commands import dispatch
from protocol import BulkString, RESPArray, RESPError, SimpleString


@pytest.mark.asyncio
async def test_dispatch_empty_and_unknown_command() -> None:
    assert await dispatch([]) == RESPError("ERR empty command")
    assert await dispatch(["UNKNOWN"]) == RESPError("ERR unknown command 'UNKNOWN'")


@pytest.mark.asyncio
async def test_ping_set_get_exists_del_flow() -> None:
    assert await dispatch(["PING"]) == SimpleString("PONG")
    assert await dispatch(["PING", "hello"]) == SimpleString("hello")

    assert await dispatch(["SET", "a", "1"]) == SimpleString("OK")
    assert await dispatch(["GET", "a"]) == BulkString("1")
    assert await dispatch(["EXISTS", "a", "b"]) == 1
    assert await dispatch(["DEL", "a", "b"]) == 1
    assert await dispatch(["GET", "a"]) == BulkString(None)


@pytest.mark.asyncio
async def test_keys_matches_pattern() -> None:
    assert await dispatch(["SET", "foo1", "v1"]) == SimpleString("OK")
    assert await dispatch(["SET", "foo2", "v2"]) == SimpleString("OK")
    assert await dispatch(["SET", "bar1", "v3"]) == SimpleString("OK")

    value = await dispatch(["KEYS", "foo*"])
    assert isinstance(value, RESPArray)
    assert [item.value for item in value.items if isinstance(item, BulkString)] == ["foo1", "foo2"]


@pytest.mark.asyncio
async def test_incr_happy_path_and_non_integer_error() -> None:
    assert await dispatch(["INCR", "counter"]) == 1
    assert await dispatch(["INCR", "counter"]) == 2
    assert await dispatch(["GET", "counter"]) == BulkString("2")

    assert await dispatch(["SET", "not-int", "abc"]) == SimpleString("OK")
    assert await dispatch(["INCR", "not-int"]) == RESPError("ERR value is not an integer")


@pytest.mark.asyncio
async def test_ttl_and_expire_with_deterministic_time(monkeypatch) -> None:
    now = {"value": 100.0}
    monkeypatch.setattr(store.time, "time", lambda: now["value"])

    assert await dispatch(["SET", "session", "v", "EX", "10"]) == SimpleString("OK")
    assert await dispatch(["TTL", "session"]) == 10

    now["value"] = 104.0
    assert await dispatch(["TTL", "session"]) == 6

    assert await dispatch(["EXPIRE", "session", "5"]) == 1
    assert await dispatch(["TTL", "session"]) == 5

    assert await dispatch(["TTL", "missing"]) == -2
    assert await dispatch(["SET", "persistent", "1"]) == SimpleString("OK")
    assert await dispatch(["TTL", "persistent"]) == -1


@pytest.mark.asyncio
async def test_argument_validation_errors() -> None:
    assert await dispatch(["SET"]) == RESPError("ERR wrong number of arguments for SET")
    assert await dispatch(["GET"]) == RESPError("ERR wrong number of arguments for GET")
    assert await dispatch(["TTL"]) == RESPError("ERR wrong number of arguments for TTL")
    assert await dispatch(["INCR"]) == RESPError("ERR wrong number of arguments for INCR")
    assert await dispatch(["EXPIRE"]) == RESPError("ERR wrong number of arguments for EXPIRE")
    assert await dispatch(["EXPIRE", "k"]) == RESPError("ERR wrong number of arguments for EXPIRE")


@pytest.mark.asyncio
async def test_invalid_numeric_values_return_resp_error() -> None:
    assert await dispatch(["SET", "k", "v", "EX", "invalid"]) == RESPError(
        "ERR value is not an integer or out of range"
    )
    assert await dispatch(["SET", "k", "v", "PX", "invalid"]) == RESPError(
        "ERR value is not an integer or out of range"
    )
    assert await dispatch(["EXPIRE", "k", "invalid"]) == RESPError(
        "ERR value is not an integer or out of range"
    )
