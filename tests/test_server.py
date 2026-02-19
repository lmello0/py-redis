import asyncio

import pytest

from server import handle_client


class FakeReader:
    def __init__(self, chunks: list[bytes]):
        self._chunks = list(chunks)

    async def read(self, _: int) -> bytes:
        await asyncio.sleep(0)
        if self._chunks:
            return self._chunks.pop(0)
        return b""


class FakeWriter:
    def __init__(self) -> None:
        self.writes: list[bytes] = []
        self.closed = False
        self.drain_calls = 0

    def get_extra_info(self, name: str):
        if name == "peername":
            return ("127.0.0.1", 6379)
        return None

    def write(self, data: bytes) -> None:
        self.writes.append(data)

    async def drain(self) -> None:
        self.drain_calls += 1
        await asyncio.sleep(0)

    def close(self) -> None:
        self.closed = True


async def _run_client(chunks: list[bytes]) -> FakeWriter:
    reader = FakeReader([*chunks, b""])
    writer = FakeWriter()
    await handle_client(reader, writer)
    return writer


@pytest.mark.asyncio
async def test_handle_client_processes_single_command() -> None:
    writer = await _run_client([b"*1\r\n$4\r\nPING\r\n"])
    assert writer.writes == [b"+PONG\r\n"]
    assert writer.drain_calls == 1
    assert writer.closed is True


@pytest.mark.asyncio
async def test_handle_client_processes_pipelined_commands() -> None:
    payload = b"".join(
        (
            b"*3\r\n$3\r\nSET\r\n$1\r\na\r\n$1\r\n1\r\n",
            b"*2\r\n$3\r\nGET\r\n$1\r\na\r\n",
        )
    )
    writer = await _run_client([payload])
    assert writer.writes == [b"+OK\r\n", b"$1\r\n1\r\n"]
    assert writer.drain_calls == 2
    assert writer.closed is True


@pytest.mark.asyncio
async def test_handle_client_buffers_partial_frames() -> None:
    writer = await _run_client([b"*1\r\n$4\r\nPI", b"NG\r\n"])
    assert writer.writes == [b"+PONG\r\n"]
    assert writer.drain_calls == 1
    assert writer.closed is True


@pytest.mark.asyncio
async def test_handle_client_non_array_payload_returns_error_and_continues() -> None:
    writer = await _run_client([b"+PING\r\n*1\r\n$4\r\nPING\r\n"])
    assert writer.writes == [b"-ERR expected array\r\n", b"+PONG\r\n"]
    assert writer.drain_calls == 1
    assert writer.closed is True
