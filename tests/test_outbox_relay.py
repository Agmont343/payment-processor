from datetime import UTC, datetime, timedelta
from uuid import uuid4

import pytest
from pamqp.commands import Basic
from sqlalchemy import update
from sqlalchemy.dialects import postgresql
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.database import transaction_scope
from app.outbox.models import (
    OUTBOX_EVENT_PAYMENTS_NEW,
    OutboxEvent,
    PublishResult,
)
from app.outbox.rabbit import is_publisher_ack
from app.outbox.relay import publish_unpublished_batch
from app.outbox.repository import (
    MAX_ATTEMPTS,
    RETRY_BACKOFF_BASE_SECONDS,
    OutboxRepository,
)
from app.outbox.service import OutboxService


class _FakeTransaction:
    async def __aenter__(self) -> None:
        return None

    async def __aexit__(self, *_args: object) -> None:
        return None


class FakeSession:
    """Сессия для unit-тестов: scalar() выдаёт события по очереди.

    Поддерживает async-контекст и begin(), чтобы работать и с
    transaction_scope в publish_unpublished_batch.
    """

    def __init__(self, events: list[OutboxEvent | None]) -> None:
        self._events = list(events)
        self.statements: list[object] = []
        self.scalar_calls = 0

    async def scalar(self, statement: object) -> OutboxEvent | None:
        self.scalar_calls += 1
        self.statements.append(statement)
        if not self._events:
            raise AssertionError("scalar() called more times than expected")
        return self._events.pop(0)

    def begin(self) -> _FakeTransaction:
        return _FakeTransaction()

    async def __aenter__(self) -> "FakeSession":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None


class FakeFactory:
    def __init__(self, session: FakeSession) -> None:
        self._session = session

    def __call__(self) -> FakeSession:
        return self._session


class _FailingAfterFirstSession(FakeSession):
    """Имитирует потерю соединения после первой итерации batch."""

    async def scalar(self, statement: object) -> OutboxEvent | None:
        self.scalar_calls += 1
        self.statements.append(statement)
        if self.scalar_calls > 1:
            raise RuntimeError("connection lost")
        if not self._events:
            raise AssertionError("scalar() called more times than expected")
        return self._events.pop(0)


def _event() -> OutboxEvent:
    event_id = uuid4()
    payment_id = uuid4()
    return OutboxEvent(
        id=event_id,
        event_type=OUTBOX_EVENT_PAYMENTS_NEW,
        aggregate_id=payment_id,
        payload={"payment_id": str(payment_id), "event_id": str(event_id)},
        created_at=datetime.now(UTC),
        attempts=0,
    )


async def test_selection_uses_skip_locked_and_retry_filters() -> None:
    session = FakeSession([None])
    repository = OutboxRepository(session)

    await repository.get_next_unpublished_with_lock()

    compiled = session.statements[0].compile(dialect=postgresql.dialect())
    sql = str(compiled).upper()
    assert "FOR UPDATE" in sql
    assert "SKIP LOCKED" in sql
    assert "PUBLISHED_AT IS NULL" in sql
    assert "ATTEMPTS <" in sql
    assert "NEXT_ATTEMPT_AT IS NULL" in sql
    assert "NEXT_ATTEMPT_AT <=" in sql
    # Граница попыток — именно MAX_ATTEMPTS.
    assert MAX_ATTEMPTS in list(compiled.params.values())


def test_publisher_ack_detects_basic_ack_only() -> None:
    assert is_publisher_ack(Basic.Ack(delivery_tag=1))
    assert not is_publisher_ack(Basic.Nack(delivery_tag=1))
    assert not is_publisher_ack(None)
    assert not is_publisher_ack(True)


class TestOutboxServicePublish:
    async def test_marks_published_only_after_confirm(self) -> None:
        event = _event()
        event.next_attempt_at = datetime.now(UTC) + timedelta(seconds=99)
        session = FakeSession([event])
        service = OutboxService(OutboxRepository(session))
        observed: list[datetime | None] = []

        async def publish(item: OutboxEvent) -> bool:
            observed.append(item.published_at)
            return True

        result, published_event = await service.publish_one(publish)

        assert result is PublishResult.SUCCESS
        assert published_event is event
        # published_at установлен ПОСЛЕ confirm, не в момент публикации.
        assert observed == [None]
        assert event.published_at is not None
        assert event.last_attempt_at is not None
        assert event.next_attempt_at is None
        assert event.attempts == 0

    @pytest.mark.parametrize(
        "publish_raises",
        [
            pytest.param(False, id="broker-nack"),
            pytest.param(True, id="publish-exception"),
        ],
    )
    async def test_failed_attempt_keeps_unpublished_and_counts_attempt(
        self, publish_raises: bool
    ) -> None:
        async def publish(_item: OutboxEvent) -> bool:
            if publish_raises:
                raise RuntimeError("broker down")
            return False

        event = _event()
        session = FakeSession([event])
        service = OutboxService(OutboxRepository(session))

        result, failed_event = await service.publish_one(publish)

        assert result is PublishResult.FAILED_ATTEMPT
        assert failed_event is event
        assert event.published_at is None
        assert event.attempts == 1
        assert event.next_attempt_at is not None

    async def test_no_events_when_queue_empty(self) -> None:
        session = FakeSession([None])
        service = OutboxService(OutboxRepository(session))

        result, event = await service.publish_one(lambda _event: True)

        assert result is PublishResult.NO_EVENTS
        assert event is None

    async def test_republish_uses_stable_event_id(self) -> None:
        event = _event()
        session = FakeSession([event, event])
        service = OutboxService(OutboxRepository(session))
        seen: list[str] = []

        async def publish(item: OutboxEvent) -> bool:
            seen.append(item.payload["event_id"])
            return True

        await service.publish_one(publish)
        # Crash после confirm, но до commit published_at: строка всё ещё
        # неопубликована и будет опубликована повторно.
        event.published_at = None
        await service.publish_one(publish)

        assert seen == [str(event.id), str(event.id)]


class TestRelayBatch:
    async def test_stops_when_no_unpublished_events(self) -> None:
        session = FakeSession([None])

        published = await publish_unpublished_batch(
            FakeFactory(session),
            lambda _event: True,
            limit=5,
        )

        assert published == 0
        assert session.scalar_calls == 1

    async def test_continues_after_failed_attempt(self) -> None:
        failing = _event()
        ready = _event()
        session = FakeSession([failing, ready, None])

        async def publish(item: OutboxEvent) -> bool:
            return item is not failing

        published = await publish_unpublished_batch(
            FakeFactory(session),
            publish,
            limit=5,
        )

        assert published == 1
        assert failing.attempts == 1
        assert failing.published_at is None
        assert ready.published_at is not None
        # fail → success → NO_EVENTS.
        assert session.scalar_calls == 3

    async def test_batch_breaks_on_transaction_error(self) -> None:
        session = _FailingAfterFirstSession([_event(), None])

        async def publish(_item: OutboxEvent) -> bool:
            return True

        published = await publish_unpublished_batch(
            FakeFactory(session),
            publish,
            limit=5,
        )

        assert published == 1
        assert session.scalar_calls == 2

    async def test_batch_does_not_spin_on_repeatedly_failing_event(
        self, postgres: async_sessionmaker[AsyncSession]
    ) -> None:
        event = _event()
        async with postgres() as session:
            session.add(event)
            await session.commit()

        async def publish(_item: OutboxEvent) -> bool:
            return False

        published = await publish_unpublished_batch(postgres, publish, limit=10)

        assert published == 0
        async with postgres() as session:
            row = await session.get(OutboxEvent, event.id)
            assert row is not None
            assert row.attempts == 1
            assert row.published_at is None
            assert row.next_attempt_at is not None
            assert row.next_attempt_at > datetime.now(UTC)


class TestRetrySelection:
    """Retry-выборка на реальной PostgreSQL: каждая операция — в своей
    транзакции, как в production relay."""

    async def test_event_selected_again_after_retry_is_due(
        self, postgres: async_sessionmaker[AsyncSession]
    ) -> None:
        event = _event()
        async with postgres() as session:
            session.add(event)
            await session.commit()

        # Первая попытка: событие выбирается, попытка фиксируется.
        async with transaction_scope(postgres) as session:
            repository = OutboxRepository(session)
            found = await repository.get_next_unpublished_with_lock()
            assert found is not None
            assert found.id == event.id
            repository.mark_failed_attempt(found, datetime.now(UTC))

        # Backoff: событие не выбирается, пока не наступило next_attempt_at.
        async with transaction_scope(postgres) as session:
            repository = OutboxRepository(session)
            assert await repository.get_next_unpublished_with_lock() is None

        # Эмуляция хода времени: retry созрел.
        async with postgres() as session:
            await session.execute(
                update(OutboxEvent)
                .where(OutboxEvent.id == event.id)
                .values(next_attempt_at=datetime.now(UTC) - timedelta(seconds=1))
            )
            await session.commit()

        async with transaction_scope(postgres) as session:
            repository = OutboxRepository(session)
            found = await repository.get_next_unpublished_with_lock()
            assert found is not None
            assert found.id == event.id
            assert found.attempts == 1

    async def test_event_with_max_attempts_is_never_selected(
        self, postgres: async_sessionmaker[AsyncSession]
    ) -> None:
        event = _event()
        event.attempts = MAX_ATTEMPTS
        async with postgres() as session:
            session.add(event)
            await session.commit()

        async with transaction_scope(postgres) as session:
            repository = OutboxRepository(session)
            assert await repository.get_next_unpublished_with_lock() is None


class TestRetryBackoff:
    """Retry-семантика mark_failed_attempt: exponential backoff и лимит."""

    def test_exponential_backoff_scheduling(self) -> None:
        event = _event()
        repository = OutboxRepository(FakeSession([]))
        attempted_at = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

        repository.mark_failed_attempt(event, attempted_at)
        assert event.attempts == 1
        assert event.last_attempt_at == attempted_at
        assert event.next_attempt_at == attempted_at + timedelta(
            seconds=RETRY_BACKOFF_BASE_SECONDS**1
        )

        repository.mark_failed_attempt(event, attempted_at)
        assert event.attempts == 2
        assert event.next_attempt_at == attempted_at + timedelta(
            seconds=RETRY_BACKOFF_BASE_SECONDS**2
        )

        repository.mark_failed_attempt(event, attempted_at)
        assert event.attempts == 3
        assert event.next_attempt_at == attempted_at + timedelta(
            seconds=RETRY_BACKOFF_BASE_SECONDS**3
        )

    def test_no_retry_scheduled_at_max_attempts(self) -> None:
        event = _event()
        event.attempts = MAX_ATTEMPTS - 1
        repository = OutboxRepository(FakeSession([]))
        attempted_at = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)

        repository.mark_failed_attempt(event, attempted_at)

        assert event.attempts == MAX_ATTEMPTS
        assert event.next_attempt_at is None
