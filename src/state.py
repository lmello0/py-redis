import asyncio

from store import make_store

_store: dict = make_store()
_lock: asyncio.Lock = asyncio.Lock()
