from collections.abc import Awaitable, Callable
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from faststream.exceptions import AckMessage, NackMessage, RejectMessage
from faststream.rabbit.message import RabbitMessage
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumer.gateway import GatewayResult
from app.consumer.handler import PaymentEventPayload, build_payments_new_handler
from app.consumer.webhook import WebhookResult
from app.payments.exceptions import PaymentNotFound
from app.payments.models import Currency, Payment, PaymentStatus


class FakeGateway:
    def __init__(self, results: list[bool]) -> None:
        self.results = results
        self.calls: list[UUID] = []

    async def process(self, payment_id: UUID) -> GatewayResult:
        self.calls.append(payment_id)
        return GatewayResult(success=self.results.pop(0))


@pytest.fixture
def fake_gateway() -> FakeGateway:
    return FakeGateway(results=[True, True, True])


@pytest.fixture
async def pending_payment(
    postgres: async_sessionmaker[AsyncSession],
) -> UUID:
    payment = Payment(
        id=uuid4(),
        amount=Decimal("10.00"),
        currency=Currency.RUB,
        description="Order",
        metadata_json={},
        status=PaymentStatus.PENDING,
        idempotency_key=f"key-{uuid4()}",
        request_hash="hash",
        webhook_url="https://example.com/hook",
        created_at=datetime.now(UTC),
    )
    async with postgres() as session:
        session.add(payment)
        await session.commit()
    return payment.id


@pytest.fixture
def payment_event(pending_payment: UUID) -> PaymentEventPayload:
    return PaymentEventPayload(payment_id=pending_payment, event_id=uuid4())


class StubMessage:
    def __init__(self, headers: dict[str, Any] | None = None) -> None:
        self.headers = headers or {}


class RecordingWebhook:
    def __init__(
        self, results: list[WebhookResult], *, error: Exception | None = None
    ) -> None:
        self.results = results
        self.error = error
        self.calls: list[tuple[str, dict]] = []

    async def send(self, url: str, payload: dict[str, Any]) -> WebhookResult:
        self.calls.append((url, payload))
        if self.error is not None:
            raise self.error
        return self.results.pop(0)


class RecordingRetryPublisher:
    def __init__(
        self, confirmed: list[bool], *, error: Exception | None = None
    ) -> None:
        self.confirmed = confirmed
        self.error = error
        self.calls: list[dict] = []

    async def __call__(
        self, *, payload: dict, event_id: str, next_attempt: int
    ) -> bool:
        self.calls.append(
            {
                "payload": payload,
                "event_id": event_id,
                "next_attempt": next_attempt,
            }
        )
        if self.error is not None:
            raise self.error
        return self.confirmed.pop(0)


def _payload(payment_id, event_id) -> PaymentEventPayload:
    return PaymentEventPayload(payment_id=payment_id, event_id=event_id)


def _build(
    postgres: async_sessionmaker[AsyncSession],
    gateway: FakeGateway,
    webhook: RecordingWebhook,
    retry_publisher: RecordingRetryPublisher,
) -> Callable[[PaymentEventPayload, RabbitMessage], Awaitable[None]]:
    return build_payments_new_handler(postgres, gateway, webhook, retry_publisher)


async def test_success_webhook_returns_normally(
    postgres, fake_gateway, pending_payment, payment_event
) -> None:
    webhook = RecordingWebhook([WebhookResult.SUCCESS])
    handler = _build(postgres, fake_gateway, webhook, RecordingRetryPublisher([]))

    await handler(payment_event, StubMessage())

    assert fake_gateway.calls == [pending_payment]
    assert len(webhook.calls) == 1
    url, body = webhook.calls[0]
    assert url == "https://example.com/hook"
    assert body["payment_id"] == str(pending_payment)
    assert body["event_id"] == str(payment_event.event_id)
    assert body["status"] == "succeeded"
    assert body["amount"] == "10.00"
    assert body["currency"] == "RUB"


async def test_transient_webhook_publishes_retry_and_acks(
    postgres, fake_gateway, pending_payment, payment_event
) -> None:
    webhook = RecordingWebhook([WebhookResult.TRANSIENT])
    publisher = RecordingRetryPublisher([True])
    handler = _build(postgres, fake_gateway, webhook, publisher)

    with pytest.raises(AckMessage):
        await handler(payment_event, StubMessage())

    assert publisher.calls[0]["next_attempt"] == 1
    assert publisher.calls[0]["event_id"] == str(payment_event.event_id)
    assert publisher.calls[0]["payload"] == {
        "payment_id": str(pending_payment),
        "event_id": str(payment_event.event_id),
    }


async def test_unconfirmed_retry_publish_nacks_original(
    postgres, fake_gateway, pending_payment, payment_event
) -> None:
    webhook = RecordingWebhook([WebhookResult.TRANSIENT])
    publisher = RecordingRetryPublisher([False])
    handler = _build(postgres, fake_gateway, webhook, publisher)

    with pytest.raises(NackMessage) as exc_info:
        await handler(payment_event, StubMessage())

    # NackMessage(requeue=True): исходное сообщение возвращается в очередь,
    # а не уходит в DLQ.
    assert exc_info.value.extra_options == {"requeue": True}
    assert publisher.calls[0]["next_attempt"] == 1


async def test_transient_webhook_with_header_uses_next_retry_queue(
    postgres, fake_gateway, pending_payment, payment_event
) -> None:
    webhook = RecordingWebhook([WebhookResult.TRANSIENT])
    publisher = RecordingRetryPublisher([True])
    handler = _build(postgres, fake_gateway, webhook, publisher)

    with pytest.raises(AckMessage):
        await handler(payment_event, StubMessage({"x-retry-count": 1}))

    assert publisher.calls[0]["next_attempt"] == 2


async def test_third_transient_attempt_rejects_to_dlq(
    postgres, fake_gateway, pending_payment, payment_event
) -> None:
    webhook = RecordingWebhook([WebhookResult.TRANSIENT])
    handler = _build(postgres, fake_gateway, webhook, RecordingRetryPublisher([]))

    with pytest.raises(RejectMessage) as exc_info:
        await handler(payment_event, StubMessage({"x-retry-count": 2}))

    # RejectMessage без опций: reject(requeue=False) -> DLQ.
    assert exc_info.value.extra_options == {}

    async with postgres() as session:
        row = await session.get(Payment, pending_payment)
        assert row is not None
        assert row.status == PaymentStatus.SUCCEEDED


async def test_permanent_webhook_rejects_immediately(
    postgres, fake_gateway, pending_payment, payment_event
) -> None:
    webhook = RecordingWebhook([WebhookResult.PERMANENT])
    publisher = RecordingRetryPublisher([])
    handler = _build(postgres, fake_gateway, webhook, publisher)

    with pytest.raises(RejectMessage) as exc_info:
        await handler(payment_event, StubMessage())

    # RejectMessage без опций: reject(requeue=False) -> DLQ.
    assert exc_info.value.extra_options == {}
    assert publisher.calls == []


async def test_gateway_only_called_once_for_terminal_redelivery(
    postgres, fake_gateway, pending_payment, payment_event
) -> None:
    webhook = RecordingWebhook([WebhookResult.TRANSIENT, WebhookResult.SUCCESS])
    publisher = RecordingRetryPublisher([True])
    handler = _build(postgres, fake_gateway, webhook, publisher)

    with pytest.raises(AckMessage):
        await handler(payment_event, StubMessage())
    await handler(payment_event, StubMessage({"x-retry-count": 1}))

    assert fake_gateway.calls == [pending_payment]
    assert len(webhook.calls) == 2


async def test_sender_exception_treated_as_transient(
    postgres, fake_gateway, pending_payment, payment_event
) -> None:
    webhook = RecordingWebhook([], error=httpx.ConnectError("network"))
    publisher = RecordingRetryPublisher([True])
    handler = _build(postgres, fake_gateway, webhook, publisher)

    with pytest.raises(AckMessage):
        await handler(payment_event, StubMessage())

    assert publisher.calls[0]["next_attempt"] == 1


async def test_retry_publish_exception_nacks_original(
    postgres, fake_gateway, pending_payment, payment_event
) -> None:
    """Исключение при публикации retry -> NackMessage(requeue=True):
    без подтверждения публикации исходное сообщение не ACK-нуто."""
    webhook = RecordingWebhook([WebhookResult.TRANSIENT])
    publisher = RecordingRetryPublisher([True], error=RuntimeError("broker down"))
    handler = _build(postgres, fake_gateway, webhook, publisher)

    with pytest.raises(NackMessage) as exc_info:
        await handler(payment_event, StubMessage())

    assert exc_info.value.extra_options == {"requeue": True}
    assert len(publisher.calls) == 1


async def test_invalid_retry_count_treated_as_first_attempt(
    postgres, fake_gateway, pending_payment, payment_event
) -> None:
    """Некорректный x-retry-count трактуется как первая попытка:
    retry публикуется в первую TTL-очередь (next_attempt=1)."""
    webhook = RecordingWebhook([WebhookResult.TRANSIENT])
    publisher = RecordingRetryPublisher([False])
    handler = _build(postgres, fake_gateway, webhook, publisher)

    with pytest.raises(NackMessage):
        await handler(payment_event, StubMessage({"x-retry-count": "garbage"}))

    assert publisher.calls[0]["next_attempt"] == 1


async def test_missing_payment_propagates_for_reject_policy(
    postgres, fake_gateway
) -> None:
    """Событие без платежа в БД -> PaymentNotFound прокидывается:
    дефолтная политика REJECT_ON_ERROR отправит его в DLQ."""
    publisher = RecordingRetryPublisher([])
    handler = _build(postgres, fake_gateway, RecordingWebhook([]), publisher)

    with pytest.raises(PaymentNotFound):
        await handler(
            PaymentEventPayload(payment_id=uuid4(), event_id=uuid4()),
            StubMessage(),
        )

    assert fake_gateway.calls == []
    assert publisher.calls == []
