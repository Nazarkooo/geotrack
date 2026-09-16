"""Polling helper so tests wait for a condition instead of for an arbitrary sleep."""

import asyncio
from collections.abc import Callable


async def wait_until(
    predicate: Callable[[], bool], *, timeout_s: float = 2.0, what: str = "condition"
) -> None:
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout_s
    while loop.time() < deadline:
        if predicate():
            return
        await asyncio.sleep(0.005)
    raise AssertionError(f"{what} did not happen within {timeout_s}s")
