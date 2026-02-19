import asyncio

import pytest

import commands
from store import make_store


@pytest.fixture(autouse=True)
def reset_command_state() -> None:
    commands._store = make_store()
    commands._lock = asyncio.Lock()
