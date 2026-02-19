import asyncio
import logging
import os

from server import start_server

logging.basicConfig(level=logging.INFO)

if __name__ == "__main__":
    host = os.getenv("CACHE_HOST", "0.0.0.0")
    port = int(os.getenv("CACHE_PORT", "6379"))

    asyncio.run(start_server(host, port))
