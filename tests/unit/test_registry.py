import asyncio
from uuid import UUID

import pytest

from geotrack.clock import utc_now
from geotrack.ids import new_uuid
from geotrack.realtime.connection import ClientConnection
from geotrack.realtime.hub import PositionHub
from geotrack.realtime.protocol import SessionInfo
from geotrack.realtime.registry import ConnectionRegistry
from tests.conftest import make_settings
from tests.waiting import wait_until

ALICE = UUID("01929f6e-0000-7000-8000-00000000000a")
BOB = UUID("01929f6e-0000-7000-8000-00000000000b")
CAROL = UUID("01929f6e-0000-7000-8000-00000000000c")


def registry_users(registry: ConnectionRegistry) -> set[UUID]:
    return {connection.user_id for connection in registry.all()}


class SilentSocket:
    async def send_bytes(self, data: bytes) -> None:
        return None

    async def send_text(self, data: str) -> None:
        return None

    async def close(self, code: int = 1000, reason: str | None = None) -> None:
        return None


def connection_for(user_id: UUID) -> ClientConnection:
    return ClientConnection(
        SilentSocket(),
        user_id=user_id,
        session=SessionInfo(id=new_uuid(), label="tests", connected_at=utc_now()),
        hub=PositionHub(cell_size_deg=0.05, stale_after_s=300),
        settings=make_settings(),
    )


class Hooks:
    def __init__(self) -> None:
        self.subscribed: list[UUID] = []
        self.unsubscribed: list[UUID] = []

    async def on_first(self, user_id: UUID) -> None:
        self.subscribed.append(user_id)

    async def on_last(self, user_id: UUID) -> None:
        self.unsubscribed.append(user_id)


@pytest.fixture
def hooks() -> Hooks:
    return Hooks()


async def add(registry: ConnectionRegistry, connection: ClientConnection) -> bool:
    return await registry.add(connection, limit=10)


@pytest.fixture
def registry(hooks: Hooks) -> ConnectionRegistry:
    return ConnectionRegistry(
        on_first_user_connection=hooks.on_first, on_last_user_disconnect=hooks.on_last
    )


async def test_only_the_first_session_of_a_user_subscribes(
    registry: ConnectionRegistry, hooks: Hooks
) -> None:
    first, second = connection_for(ALICE), connection_for(ALICE)

    await add(registry, first)
    await add(registry, second)

    assert hooks.subscribed == [ALICE]
    assert len(registry.for_user(ALICE)) == 2
    assert set(registry.for_user(ALICE)) == {first, second}


async def test_only_the_last_session_of_a_user_unsubscribes(
    registry: ConnectionRegistry, hooks: Hooks
) -> None:
    first, second = connection_for(ALICE), connection_for(ALICE)
    await add(registry, first)
    await add(registry, second)

    await registry.remove(first)
    assert hooks.unsubscribed == []

    await registry.remove(second)
    assert hooks.unsubscribed == [ALICE]
    assert registry.for_user(ALICE) == ()
    assert registry.count() == 0


async def test_users_are_tracked_independently(registry: ConnectionRegistry, hooks: Hooks) -> None:
    alice, bob = connection_for(ALICE), connection_for(BOB)

    await add(registry, alice)
    await add(registry, bob)
    await registry.remove(alice)

    assert hooks.subscribed == [ALICE, BOB]
    assert hooks.unsubscribed == [ALICE]
    assert registry.count() == 1
    assert registry.all() == (bob,)


async def test_removing_an_unknown_connection_is_harmless(
    registry: ConnectionRegistry, hooks: Hooks
) -> None:
    await registry.remove(connection_for(ALICE))

    assert hooks.unsubscribed == []
    assert registry.count() == 0


async def test_removing_the_same_connection_twice_unsubscribes_once(
    registry: ConnectionRegistry, hooks: Hooks
) -> None:
    connection = connection_for(ALICE)
    await add(registry, connection)

    await registry.remove(connection)
    await registry.remove(connection)

    assert hooks.unsubscribed == [ALICE]


async def test_a_user_cannot_exceed_their_session_limit(
    registry: ConnectionRegistry, hooks: Hooks
) -> None:
    accepted, rejected = connection_for(ALICE), connection_for(ALICE)

    assert await registry.add(accepted, limit=1) is True
    assert await registry.add(rejected, limit=1) is False
    assert registry.for_user(ALICE) == (accepted,)
    assert hooks.subscribed == [ALICE]


async def test_a_rejected_first_connection_leaves_no_trace(
    registry: ConnectionRegistry, hooks: Hooks
) -> None:
    assert await registry.add(connection_for(ALICE), limit=0) is False

    assert registry.for_user(ALICE) == ()
    assert hooks.subscribed == []


async def test_a_stuck_subscribe_does_not_block_other_users(hooks: Hooks) -> None:
    """The hooks reach Redis over a connection with no socket timeout.

    One user's blackholed subscribe used to freeze every connect and every disconnect
    on the replica, because the hook was awaited while the admission lock was held.
    """
    released = asyncio.Event()

    async def hangs_for_alice(user_id: UUID) -> None:
        if user_id == ALICE:
            await released.wait()
        await hooks.on_first(user_id)

    registry = ConnectionRegistry(
        on_first_user_connection=hangs_for_alice, on_last_user_disconnect=hooks.on_last
    )
    leaving = connection_for(CAROL)
    await registry.add(leaving, limit=10)

    stuck = asyncio.create_task(registry.add(connection_for(ALICE), limit=10))
    await wait_until(lambda: ALICE in registry_users(registry), what="alice to be admitted")

    await asyncio.wait_for(registry.add(connection_for(BOB), limit=10), timeout=1)
    await asyncio.wait_for(registry.remove(leaving), timeout=1)

    assert hooks.subscribed == [CAROL, BOB]
    assert hooks.unsubscribed == [CAROL]
    assert not stuck.done()

    released.set()
    assert await stuck is True
    assert hooks.subscribed == [CAROL, BOB, ALICE]


async def test_a_disconnect_racing_a_connect_leaves_the_subscription_correct(
    hooks: Hooks,
) -> None:
    """The hook runs outside the lock, so membership can change under it.

    Whatever the registry ends up holding has to win: a subscription left behind
    wastes traffic, and a missing one loses every alert that user has coming.
    """
    slow = asyncio.Event()

    async def slow_subscribe(user_id: UUID) -> None:
        await slow.wait()
        await hooks.on_first(user_id)

    registry = ConnectionRegistry(
        on_first_user_connection=slow_subscribe, on_last_user_disconnect=hooks.on_last
    )
    connection = connection_for(ALICE)
    connecting = asyncio.create_task(registry.add(connection, limit=10))
    await wait_until(lambda: ALICE in registry_users(registry), what="alice to be admitted")

    # The client gives up while its subscribe is still in flight.
    disconnecting = asyncio.create_task(registry.remove(connection))
    await asyncio.sleep(0)
    slow.set()
    await asyncio.wait_for(asyncio.gather(connecting, disconnecting), timeout=1)

    assert hooks.subscribed == [ALICE]
    assert hooks.unsubscribed == [ALICE]
    assert registry.count() == 0


async def test_the_second_session_of_a_user_waits_for_the_first_subscription(
    hooks: Hooks,
) -> None:
    """Admission still means "your alerts are routed here", not "soon"."""
    subscribing = asyncio.Event()

    async def slow_subscribe(user_id: UUID) -> None:
        await subscribing.wait()
        await hooks.on_first(user_id)

    registry = ConnectionRegistry(
        on_first_user_connection=slow_subscribe, on_last_user_disconnect=hooks.on_last
    )
    first = asyncio.create_task(registry.add(connection_for(ALICE), limit=10))
    await wait_until(lambda: ALICE in registry_users(registry), what="alice to be admitted")
    second = asyncio.create_task(registry.add(connection_for(ALICE), limit=10))
    await asyncio.sleep(0)

    assert not second.done()

    subscribing.set()
    await asyncio.wait_for(asyncio.gather(first, second), timeout=1)
    assert hooks.subscribed == [ALICE]
