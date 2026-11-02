import logging
from collections.abc import Awaitable, Callable
from datetime import UTC, datetime

from app.outbox.models import OutboxEvent, PublishResult
from app.outbox.repository import OutboxRepositoryProtocol

logger = logging.getLogger(__name__)

PublishFn = Callable[[OutboxEvent], Awaitable[bool]]


class OutboxService:
    """Сервис публикации outbox events через RabbitMQ publisher confirms."""

    def __init__(self, outbox_repo: OutboxRepositoryProtocol) -> None:
        self._outbox_repo = outbox_repo

    async def publish_one(
        self, publish_fn: PublishFn
    ) -> tuple[PublishResult, OutboxEvent | None]:
        """Опубликовать одно неопубликованное событие.

        Возвращает:
        - (SUCCESS, event): событие опубликовано и подтверждено
        - (FAILED_ATTEMPT, event): попытка неудачна, attempts сохранён
        - (NO_EVENTS, None): нет готовых к обработке событий

        Транзакция (commit/rollback) управляется снаружи.
        При FAILED_ATTEMPT транзакция делает commit для сохранения attempts.
        """
        event = await self._outbox_repo.get_next_unpublished_with_lock()

        if event is None:
            return PublishResult.NO_EVENTS, None

        try:
            # Публикация с таймаутом (реализован в publish_fn)
            confirmed = await publish_fn(event)
        except Exception:
            logger.exception("Failed to publish event %s", event.id)
            self._outbox_repo.mark_failed_attempt(event, datetime.now(UTC))

            # Проверяем, исчерпаны ли попытки (после mark_failed_attempt)
            if event.attempts >= self._outbox_repo.max_attempts:
                logger.warning(
                    "Event %s exhausted all %d attempts, will not retry",
                    event.id,
                    self._outbox_repo.max_attempts,
                )

            return PublishResult.FAILED_ATTEMPT, event

        if not confirmed:
            # RabbitMQ NACK
            logger.warning("Event %s rejected by broker (NACK)", event.id)
            self._outbox_repo.mark_failed_attempt(event, datetime.now(UTC))

            if event.attempts >= self._outbox_repo.max_attempts:
                logger.warning(
                    "Event %s exhausted all %d attempts after NACK, will not retry",
                    event.id,
                    self._outbox_repo.max_attempts,
                )

            return PublishResult.FAILED_ATTEMPT, event

        # RabbitMQ ACK
        self._outbox_repo.mark_published(event, datetime.now(UTC))
        logger.info("Published event %s", event.id)
        return PublishResult.SUCCESS, event
