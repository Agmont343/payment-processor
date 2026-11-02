from datetime import UTC, datetime, timedelta
from typing import Protocol

from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.outbox.models import OutboxEvent

MAX_ATTEMPTS = 10
RETRY_BACKOFF_BASE_SECONDS = 2  # Exponential backoff: 2s, 4s, 8s, 16s, ...


class OutboxRepositoryProtocol(Protocol):
    """Интерфейс для OutboxRepository."""

    @property
    def max_attempts(self) -> int: ...
    def add(self, event: OutboxEvent) -> None: ...
    async def get_next_unpublished_with_lock(self) -> OutboxEvent | None: ...
    def mark_published(self, event: OutboxEvent, published_at: datetime) -> None: ...
    def mark_failed_attempt(
        self, event: OutboxEvent, attempted_at: datetime
    ) -> None: ...


class OutboxRepository:
    """Репозиторий для работы с OutboxEvent."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    @property
    def max_attempts(self) -> int:
        """Максимальное количество попыток публикации."""
        return MAX_ATTEMPTS

    def add(self, event: OutboxEvent) -> None:
        """Добавить событие в сессию.

        Фактическая запись в БД произойдёт при commit транзакции.
        """
        self._session.add(event)

    async def get_next_unpublished_with_lock(self) -> OutboxEvent | None:
        """Получить одно неопубликованное событие для обработки.

        Условия:
        - published_at IS NULL (не опубликовано)
        - attempts < MAX_ATTEMPTS (не исчерпаны попытки)
        - next_attempt_at IS NULL OR next_attempt_at <= NOW (готово к retry)

        FOR UPDATE SKIP LOCKED для конкурентных workers.
        """
        now = datetime.now(UTC)
        stmt = (
            select(OutboxEvent)
            .where(
                and_(
                    OutboxEvent.published_at.is_(None),
                    OutboxEvent.attempts < MAX_ATTEMPTS,
                    (
                        OutboxEvent.next_attempt_at.is_(None)
                        | (OutboxEvent.next_attempt_at <= now)
                    ),
                )
            )
            .order_by(OutboxEvent.created_at, OutboxEvent.id)
            .limit(1)
            .with_for_update(skip_locked=True)
        )
        return await self._session.scalar(stmt)

    def mark_published(self, event: OutboxEvent, published_at: datetime) -> None:
        """Отметить событие как успешно опубликованное.

        Commit транзакции происходит снаружи.
        """
        event.published_at = published_at
        event.last_attempt_at = published_at
        event.next_attempt_at = None

    def mark_failed_attempt(self, event: OutboxEvent, attempted_at: datetime) -> None:
        """Отметить неудачную попытку публикации.

        Увеличивает attempts и рассчитывает next_attempt_at с exponential backoff.
        После достижения MAX_ATTEMPTS событие больше не будет обрабатываться.

        Commit транзакции происходит снаружи.
        """
        event.attempts += 1
        event.last_attempt_at = attempted_at

        if event.attempts < MAX_ATTEMPTS:
            # Exponential backoff: 2^(attempts) секунд
            # attempts=1 → 2s, attempts=2 → 4s, attempts=3 → 8s, ...
            backoff_seconds = RETRY_BACKOFF_BASE_SECONDS**event.attempts
            event.next_attempt_at = attempted_at + timedelta(seconds=backoff_seconds)
