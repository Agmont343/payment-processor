import asyncio
import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.outbox.models import OutboxEvent

logger = logging.getLogger(__name__)

RELAY_INTERVAL_SECONDS = 0.5
RELAY_BATCH_SIZE = 10

PublishFn = Callable[[OutboxEvent], Awaitable[bool]]


def unpublished_events_query(*, limit: int = 1):
    return (
        select(OutboxEvent)
        .where(OutboxEvent.published_at.is_(None))
        .order_by(OutboxEvent.created_at, OutboxEvent.id)
        .limit(limit)
        .with_for_update(skip_locked=True)
    )


async def publish_one(
    session_factory: async_sessionmaker[AsyncSession],
    publish: PublishFn,
) -> bool | None:
    async with session_factory() as session:
        event = await session.scalar(unpublished_events_query(limit=1))
        if event is None:
            return None
        try:
            confirmed = await publish(event)
        except Exception:
            await session.rollback()
            raise
        if not confirmed:
            await session.rollback()
            return False
        event.published_at = datetime.now(UTC)
        await session.commit()
        return True


async def publish_unpublished_batch(
    session_factory: async_sessionmaker[AsyncSession],
    publish: PublishFn,
    *,
    limit: int = RELAY_BATCH_SIZE,
) -> int:
    published = 0
    for _ in range(limit):
        try:
            result = await publish_one(session_factory, publish)
        except Exception:
            logger.exception("Outbox relay failed to publish an event")
            continue
        if result is None:
            break
        if result:
            published += 1
    return published


class OutboxRelay:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        publish: PublishFn,
        *,
        interval_seconds: float = RELAY_INTERVAL_SECONDS,
        batch_size: int = RELAY_BATCH_SIZE,
    ) -> None:
        self._session_factory = session_factory
        self._publish = publish
        self._interval_seconds = interval_seconds
        self._batch_size = batch_size
        self._stop = asyncio.Event()
        self._task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        if self._task is not None:
            return
        self._stop.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        self._stop.set()
        if self._task is None:
            return
        await self._task
        self._task = None

    async def _run(self) -> None:
        while not self._stop.is_set():
            await publish_unpublished_batch(
                self._session_factory,
                self._publish,
                limit=self._batch_size,
            )
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=self._interval_seconds,
                )
            except TimeoutError:
                continue
