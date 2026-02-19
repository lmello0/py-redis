import pytest

from protocol import BulkString, RESPArray, RESPError, SimpleString, parse, serialize


def test_parse_simple_string_with_remaining_buffer() -> None:
    value, rest = parse(b"+OK\r\ntail")
    assert value == SimpleString("OK")
    assert rest == b"tail"


def test_parse_error_integer_bulk_and_array() -> None:
    value, rest = parse(b"-ERR boom\r\n")
    assert value == RESPError("ERR boom")
    assert rest == b""

    value, rest = parse(b":42\r\n")
    assert value == 42
    assert rest == b""

    value, rest = parse(b"$3\r\nfoo\r\n")
    assert value == BulkString("foo")
    assert rest == b""

    value, rest = parse(b"$-1\r\n")
    assert value == BulkString(None)
    assert rest == b""

    value, rest = parse(b"*3\r\n+OK\r\n:1\r\n$3\r\nbar\r\n")
    assert value == RESPArray((SimpleString("OK"), 1, BulkString("bar")))
    assert rest == b""


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        (SimpleString("PONG"), b"+PONG\r\n"),
        (RESPError("ERR boom"), b"-ERR boom\r\n"),
        (7, b":7\r\n"),
        (BulkString(None), b"$-1\r\n"),
        (BulkString("abc"), b"$3\r\nabc\r\n"),
        (
            RESPArray((BulkString("PING"), BulkString("hello"))),
            b"*2\r\n$4\r\nPING\r\n$5\r\nhello\r\n",
        ),
    ],
)
def test_serialize_values(value, expected: bytes) -> None:
    assert serialize(value) == expected


@pytest.mark.parametrize(
    "payload",
    [
        b"+OK",
        b"$5\r\nabc\r\n",
        b"*2\r\n+OK\r\n",
    ],
)
def test_parse_incomplete_payload_raises_value_error(payload: bytes) -> None:
    with pytest.raises(ValueError):
        parse(payload)


def test_parse_unknown_prefix_raises_value_error() -> None:
    with pytest.raises(ValueError):
        parse(b"!unknown\r\n")


def test_serialize_parse_roundtrip() -> None:
    original = RESPArray((SimpleString("HELLO"), 9, BulkString("world"), BulkString(None)))
    parsed, rest = parse(serialize(original))
    assert parsed == original
    assert rest == b""
