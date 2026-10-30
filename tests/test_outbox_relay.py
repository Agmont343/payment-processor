from datetime import UTC, datetime
from uuid import uuid4

import pytest
from pamqp.commands import Basic
from sqlalchemy.dialects import postgresql

from app.outbox.models import OUTBOX_EVENT_PAYMENTS_NEW, OutboxEvent
from app.outbox.rabbit import is_publisher_ack
from app.outbox.relay import (
    publish_one,
    publish_unpublished_batch,
    unpublished_events_query,
)


class FakeSession:
    def __init__(self, event: OutboxEvent | None) -> None:
        self.event = event
        self.commits = 0
        self.rollbacks = 0

    async def scalar(self, _statement: object) -> OutboxEvent | None:
        return self.event

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1

    async def __aenter__(self) -> "FakeSession":
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None


class FakeFactory:
    def __init__(self, session: FakeSession) -> None:
        self.session = session

    def __call__(self) -> FakeSession:
        return self.session


def _event() -> OutboxEvent:
    event_id = uuid4()
    payment_id = uuid4()
    return OutboxEvent(
        id=event_id,
        event_type=OUTBOX_EVENT_PAYMENTS_NEW,
        aggregate_id=payment_id,
        payload={"payment_id": str(payment_id), "event_id": str(event_id)},
        created_at=datetime.now(UTC),
    )


def test_unpublished_query_uses_skip_locked() -> None:
    sql = str(
        unpublished_events_query().compile(
            dialect=postgresql.dialect(),
            compile_kwargs={"literal_binds": True},
        )
    ).upper()
    assert "FOR UPDATE" in sql
    assert "SKIP LOCKED" in sql
    assert "PUBLISHED_AT IS NULL" in sql


def test_publisher_ack_detects_basic_ack_only() -> None:
    assert is_publisher_ack(Basic.Ack(delivery_tag=1))
    assert not is_publisher_ack(Basic.Nack(delivery_tag=1))
    assert not is_publisher_ack(None)
    assert not is_publisher_ack(True)


async def test_publish_one_sets_published_at_only_after_confirm() -> None:
    event = _event()
    session = FakeSession(event)
    published: list[str] = []

    async def publish(item: OutboxEvent) -> bool:
        assert item.published_at is None
        published.append(str(item.id))
        return True

    result = await publish_one(FakeFactory(session), publish)

    assert result is True
    assert published == [str(event.id)]
    assert event.published_at is not None
    assert session.commits == 1
    assert session.rollbacks == 0


async def test_publish_one_does_not_mark_published_without_confirm() -> None:
    event = _event()
    session = FakeSession(event)

    async def publish(_item: OutboxEvent) -> bool:
        return False

    result = await publish_one(FakeFactory(session), publish)

    assert result is False
    assert event.published_at is None
    assert session.commits == 0
    assert session.rollbacks == 1


async def test_publish_one_rolls_back_when_publish_raises() -> None:
    event = _event()
    session = FakeSession(event)

    async def publish(_item: OutboxEvent) -> bool:
        raise RuntimeError("broker down")

    with pytest.raises(RuntimeError, match="broker down"):
        await publish_one(FakeFactory(session), publish)

    assert event.published_at is None
    assert session.commits == 0
    assert session.rollbacks == 1


async def test_publish_one_returns_none_when_queue_empty() -> None:
    session = FakeSession(None)
    result = await publish_one(FakeFactory(session), lambda _event: True)
    assert result is None
    assert session.commits == 0


async def test_batch_stops_when_no_unpublished_events() -> None:
    session = FakeSession(None)
    published = await publish_unpublished_batch(
        FakeFactory(session),
        lambda _event: True,
        limit=5,
    )
    assert published == 0


async def test_republish_uses_stable_event_id() -> None:
    event = _event()
    session = FakeSession(event)
    seen: list[str] = []

    async def publish(item: OutboxEvent) -> bool:
        seen.append(item.payload["event_id"])
        return True

    await publish_one(FakeFactory(session), publish)
    event.published_at = None
    await publish_one(FakeFactory(session), publish)

    assert seen == [str(event.id), str(event.id)]
