import asyncio
import logging

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.database import transaction_scope
from app.outbox.models import PublishResult
from app.outbox.repository import OutboxRepository
from app.outbox.service import OutboxService, PublishFn

logger = logging.getLogger(__name__)

RELAY_INTERVAL_SECONDS = 0.5
RELAY_BATCH_SIZE = 10


async def publish_unpublished_batch(
    session_factory: async_sessionmaker[AsyncSession],
    publish_fn: PublishFn,
    *,
    limit: int = RELAY_BATCH_SIZE,
) -> int:
    """Опубликовать batch неопубликованных событий.

    Каждое событие обрабатывается в отдельной транзакции.
    Failed events сохраняют attempts и НЕ прерывают batch.

    Возвращает количество успешно опубликованных событий.
    """
    published = 0

    for iteration in range(limit):
        try:
            async with transaction_scope(session_factory) as session:
                outbox_repo = OutboxRepository(session)
                outbox_service = OutboxService(outbox_repo)

                result, event = await outbox_service.publish_one(publish_fn)

                if result is PublishResult.NO_EVENTS:
                    # Нет готовых событий → завершаем batch
                    break

                if result is PublishResult.FAILED_ATTEMPT:
                    # Failed attempt сохранён → продолжаем batch
                    logger.info(
                        "Event %s failed attempt %d, will retry later",
                        event.id if event else "unknown",
                        event.attempts if event else 0,
                    )
                    continue

                # SUCCESS — commit произошёл при выходе из transaction_scope

            # Увеличиваем счётчик только после успешного commit
            published += 1

        except Exception:
            logger.exception("Outbox relay iteration %d failed", iteration)
            # При ошибке транзакции завершаем batch, чтобы не зацикливаться
            # на одном и том же событии
            break

    return published


class OutboxRelay:
    """Фоновый worker для публикации outbox events в RabbitMQ."""

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        publish_fn: PublishFn,
        *,
        interval_seconds: float = RELAY_INTERVAL_SECONDS,
        batch_size: int = RELAY_BATCH_SIZE,
    ) -> None:
        self._session_factory = session_factory
        self._publish_fn = publish_fn
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
                self._publish_fn,
                limit=self._batch_size,
            )
            try:
                await asyncio.wait_for(
                    self._stop.wait(),
                    timeout=self._interval_seconds,
                )
            except TimeoutError:
                continue
