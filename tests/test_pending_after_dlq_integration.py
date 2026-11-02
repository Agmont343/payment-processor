"""Интеграционный тест: техническая ошибка до сохранения статуса уводит
сообщение в DLQ, но платёж остаётся PENDING (не succeeded, не failed)."""

from typing import Any
from uuid import uuid4

import aio_pika
from faststream.rabbit import Channel, RabbitBroker

from app.consumer.handler import build_payments_new_handler
from app.consumer.webhook import WebhookResult
from app.outbox.rabbit import (
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
    declare_payments_topology,
)
from app.payments.models import Payment, PaymentStatus
from tests.conftest import SessionFactory
from tests.helpers import FailingGateway, make_payment
from tests.rabbit_helpers import (
    DLQ_ROUTING_KEY,
    declare_dlq_observer,
    declare_retry_observer,
    observer_saw,
    publish_event,
    publish_retry_with,
    wait_for_dead_lettered,
)


class SuccessWebhook:
    """Webhook-заглушка: всегда успех — не источник сбоя в этом тесте."""

    async def send(self, url: str, payload: dict[str, Any]) -> WebhookResult:
        return WebhookResult.SUCCESS


async def test_payment_stays_pending_after_technical_failure_exhausts_retries(
    postgres: SessionFactory,
    consumer_session_factory: SessionFactory,
    rabbit_test_url: str,
) -> None:
    """Техническая ошибка ДО сохранения статуса: платёж остаётся PENDING.

    В отличие от test_transient_webhook_goes_through_ttl_to_dlq, здесь
    падает gateway, а не webhook: платёж уходит в SUCCEEDED.
    """
    payment = make_payment("http://127.0.0.1:9999/webhook")
    async with postgres() as session:
        session.add(payment)
        await session.commit()

    # Consumer с FailingGateway: сбой до UPDATE статуса
    broker = RabbitBroker(
        rabbit_test_url, default_channel=Channel(publisher_confirms=True)
    )
    await broker.connect()
    await declare_payments_topology(broker)

    gateway = FailingGateway()
    webhook = SuccessWebhook()
    handler = build_payments_new_handler(
        consumer_session_factory,
        gateway,
        webhook,
        publish_retry_with(broker),
    )
    broker.subscriber(PAYMENTS_NEW_QUEUE, exchange=PAYMENTS_EXCHANGE)(handler)
    await broker.start()

    event_id = str(uuid4())
    payload = {"payment_id": str(payment.id), "event_id": event_id}

    conn = await aio_pika.connect(rabbit_test_url)
    try:
        dlq_observer = await declare_dlq_observer(conn)
        retry1_observer = await declare_retry_observer(conn, "retry.1")
        retry2_observer = await declare_retry_observer(conn, "retry.2")

        await publish_event(payload, broker)

        message = await wait_for_dead_lettered(dlq_observer, event_id, timeout=30)
        assert message.message_id == event_id
        assert message.headers["x-retry-count"] == 2  # Две повторных попытки
        assert message.routing_key == DLQ_ROUTING_KEY
        await message.ack()

        assert await observer_saw(retry1_observer, event_id, timeout=5)
        assert await observer_saw(retry2_observer, event_id, timeout=5)
    finally:
        await conn.close()
        await broker.stop()

    assert len(gateway.calls) == 3
    assert gateway.calls == [payment.id, payment.id, payment.id]

    async with consumer_session_factory() as session:
        row = await session.get(Payment, payment.id)
        assert row is not None
        # Технические ошибки не переводят платёж из PENDING: терминальный
        # статус пишется только после успешного завершения gateway-фазы.
        assert row.status == PaymentStatus.PENDING, f"got {row.status}"
        assert row.processed_at is None, (
            "processed_at must be NULL when status is PENDING after DLQ"
        )
