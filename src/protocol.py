from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Optional


@dataclass(frozen=True)
class SimpleString:
    value: str


@dataclass(frozen=True)
class BulkString:
    value: Optional[str]


@dataclass(frozen=True)
class RESPArray:
    items: tuple[Any, ...]


@dataclass(frozen=True)
class RESPError:
    message: str


RESPValue = SimpleString | BulkString | RESPArray | RESPError | int


def parse(data: bytes) -> tuple[RESPValue, bytes]:
    if not data:
        raise ValueError("Empty buffer")

    prefix, rest = chr(data[0]), data[1:]

    match prefix:
        case "+":
            return _parse_simple_string(rest)
        case "-":
            return _parse_error(rest)
        case ":":
            return _parse_integer(rest)
        case "$":
            return _parse_bulk_string(rest)
        case "*":
            return _parse_array(rest)
        case _:
            raise ValueError(f"Unknown RESP prefix: {prefix!r}")


def _read_line(data: bytes) -> tuple[bytes, bytes]:
    idx = data.find(b"\r\n")

    if idx == -1:
        raise ValueError("Incomplete: no CRLF found")

    return data[:idx], data[idx + 2 :]


def _parse_simple_string(data: bytes) -> tuple[SimpleString, bytes]:
    line, rest = _read_line(data)
    return SimpleString(line.decode()), rest


def _parse_error(data: bytes) -> tuple[RESPError, bytes]:
    line, rest = _read_line(data)
    return RESPError(line.decode()), rest


def _parse_integer(data: bytes) -> tuple[int, bytes]:
    line, rest = _read_line(data)
    return int(line), rest


def _parse_bulk_string(data: bytes) -> tuple[BulkString, bytes]:
    length_line, rest = _read_line(data)
    length = int(length_line)

    if length == -1:
        return BulkString(None), rest

    if len(rest) < length + 2:
        raise ValueError("Incomplete bulk string")

    return BulkString(rest[:length].decode()), rest[length + 2 :]


def _parse_array(data: bytes) -> tuple[RESPArray, bytes]:
    count_line, rest = _read_line(data)
    count = int(count_line)

    if count == -1:
        return RESPArray(tuple()), rest

    items = []
    for _ in range(count):
        item, rest = parse(rest)
        items.append(item)

    return RESPArray(tuple(items)), rest


def serialize(value: RESPValue) -> bytes:
    match value:
        case SimpleString(v):
            return f"+{v}\r\n".encode()
        case RESPError(msg):
            return f"-{msg}\r\n".encode()
        case int(n):
            return f":{n}\r\n".encode()
        case BulkString(None):
            return b"$-1\r\n"
        case BulkString(v):
            return f"${len(v)}\r\n{v}\r\n".encode()  # type: ignore
        case RESPArray(items):
            parts = [f"*{len(items)}\r\n".encode()]
            parts += [serialize(i) for i in items]
            return b"".join(parts)
        case _:
            raise TypeError(f"Cannot serialize {type(value)}")
