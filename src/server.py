from __future__ import annotations

import asyncio
import logging

from commands import dispatch
from protocol import BulkString, RESPArray, RESPError, parse, serialize

log = logging.getLogger(__name__)


async def handle_client(
    reader: asyncio.StreamReader,
    writer: asyncio.StreamWriter,
) -> None:
    addr = writer.get_extra_info("peername")
    log.info("New connection from %s", addr)

    buf = b""

    try:
        while True:
            chunk = await reader.read(4096)
            if not chunk:
                break

            buf += chunk

            while buf:
                try:
                    value, buf = parse(buf)
                except ValueError:
                    break

                if not isinstance(value, RESPArray):
                    writer.write(serialize(RESPError("ERR expected array")))
                    continue

                args = [item.value for item in value.items if isinstance(item, BulkString) and item.value is not None]
                result = await dispatch(args)
                writer.write(serialize(result))

                await writer.drain()
    except Exception as e:
        log.error("Client error: %s", e)
    finally:
        writer.close()
        log.info("Connection closed: %s", addr)


async def start_server(host: str = "0.0.0.0", port: int = 6379) -> None:
    server = await asyncio.start_server(handle_client, host, port)
    log.info("Cache server listening on %s:%d", host, port)

    async with server:
        await server.serve_forever()
