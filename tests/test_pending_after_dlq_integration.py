"""Интеграционный тест: платёж остаётся pending после исчерпания retry.

Критический сценарий из плана (этап 5): техническая ошибка ДО сохранения
статуса приводит к тому, что после трёх попыток сообщение уходит в DLQ,
но платёж остаётся в статусе PENDING (не succeeded, не failed).

Требует реальных PostgreSQL + RabbitMQ; скипается если недоступны.
"""

import asyncio
from decimal import Decimal
from uuid import UUID, uuid4

import aio_pika
import pytest
from aio_pika.abc import AbstractConnection, AbstractQueue
from faststream.rabbit import Channel, RabbitBroker
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.config import settings
from app.consumer.gateway import GatewayResult
from app.consumer.handler import build_payments_new_handler
from app.consumer.webhook import WebhookResult
from app.outbox.rabbit import (
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
    declare_payments_topology,
    publish_to_retry,
)
from app.payments.models import Currency, Payment, PaymentStatus

DLX_NAME = "payments.dlx"
DLQ_ROUTING_KEY = "payments.new.dlq"
RETRY_EXCHANGE_NAME = "payments.retry"


class FailingGateway:
    """Gateway, который падает с технической ошибкой.

    Эмулирует сбой ДО сохранения статуса: gateway выполняет логику,
    но process_payment_event не успевает записать терминальный статус
    из-за технического сбоя (сеть, БД, crash процесса).
    """

    def __init__(self) -> None:
        self.calls: list[UUID] = []

    async def process(self, payment_id: UUID) -> GatewayResult:
        self.calls.append(payment_id)
        # Техническая ошибка: gateway логически отработал, но результат
        # не сохранится из-за exception перед UPDATE статуса.
        raise RuntimeError("DB connection lost before status UPDATE")


class SuccessWebhook:
    """Webhook-заглушка: всегда успех.

    Нужна для того, чтобы код дошёл до gateway-вызова. Webhook не должен
    быть источником сбоя в этом тесте.
    """

    async def send(self, url: str, payload: dict) -> WebhookResult:
        return WebhookResult.SUCCESS


def _pending_payment(webhook_url: str) -> Payment:
    return Payment(
        id=uuid4(),
        amount=Decimal("42.00"),
        currency=Currency.RUB,
        description="pending-after-dlq scenario",
        metadata_json={},
        status=PaymentStatus.PENDING,
        idempotency_key=f"key-{uuid4()}",
        request_hash="hash-pending-dlq",
        webhook_url=webhook_url,
    )


def _publish_retry_with(broker: RabbitBroker):
    async def publish_retry(*, payload: dict, event_id: str, next_attempt: int) -> bool:
        return await publish_to_retry(
            broker, payload=payload, event_id=event_id, next_attempt=next_attempt
        )

    return publish_retry


async def _declare_dlq_observer(conn: AbstractConnection) -> AbstractQueue:
    """Временная очередь-наблюдатель на DLX.

    Позволяет проверить, что сообщение действительно ушло в DLQ, не трогая
    боевую payments.new.dlq (которая может содержать чужие сообщения).
    """
    channel = await conn.channel()
    dlx = await channel.get_exchange(DLX_NAME)
    queue = await channel.declare_queue(
        f"payments.new.dlq.test-{uuid4()}",
        exclusive=True,
    )
    await queue.bind(dlx, routing_key=DLQ_ROUTING_KEY)
    return queue


async def _declare_retry_observer(
    conn: AbstractConnection,
    routing_key: str,
) -> AbstractQueue:
    """Временная очередь-наблюдатель за публикациями в TTL-очереди."""
    channel = await conn.channel()
    retry_exchange = await channel.get_exchange(RETRY_EXCHANGE_NAME)
    queue = await channel.declare_queue(
        f"payments.retry.observer-{routing_key}-{uuid4()}",
        exclusive=True,
    )
    await queue.bind(retry_exchange, routing_key=routing_key)
    return queue


async def _observer_saw(
    queue: AbstractQueue,
    message_id: str,
    *,
    timeout: float,
) -> bool:
    """Проверить, что message_id прошёл через очередь."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        message = await queue.get(no_ack=False, fail=False, timeout=1)
        if message is None:
            continue
        saw_ours = message.message_id == message_id
        await message.ack()
        if saw_ours:
            return True
    return False


async def _wait_for_dead_lettered(
    queue: AbstractQueue,
    message_id: str,
    *,
    timeout: float,
):
    """Дождаться dead-letter сообщения по message_id."""
    deadline = asyncio.get_running_loop().time() + timeout
    while asyncio.get_running_loop().time() < deadline:
        message = await queue.get(no_ack=False, fail=False, timeout=1)
        if message is None:
            continue
        if message.message_id != message_id:
            await message.ack()
            continue
        return message
    pytest.fail(
        f"event {message_id} was not dead-lettered to {DLX_NAME} within {timeout}s",
    )


async def _publish_event(payload: dict, broker: RabbitBroker) -> None:
    event_id = payload["event_id"]
    await broker.publish(
        payload,
        exchange=PAYMENTS_EXCHANGE,
        routing_key="payments.new",
        persist=True,
        mandatory=True,
        message_id=event_id,
        correlation_id=event_id,
        message_type="payments.new",
    )


@pytest.fixture
async def consumer_session_factory(test_db_url: str):
    engine = create_async_engine(test_db_url, poolclass=NullPool)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await session.execute(text("SELECT 1"))
    except Exception as exc:
        await engine.dispose()
        pytest.skip(f"PostgreSQL is not available: {exc}")
    yield factory
    await engine.dispose()


async def test_payment_stays_pending_after_technical_failure_exhausts_retries(
    postgres, consumer_session_factory, monkeypatch, rabbit_test_url
) -> None:
    """Техническая ошибка ДО сохранения статуса: платёж остаётся PENDING.

    Сценарий:
    1. Payment создан в статусе PENDING
    2. Gateway вызывается, но падает с RuntimeError (эмулирует сбой БД/сети)
    3. Сообщение ретраится 3 раза (через TTL 1s и 2s)
    4. После 3-й попытки сообщение уходит в DLQ
    5. Payment остаётся в статусе PENDING (не succeeded, не failed)

    Это отличается от теста test_transient_webhook_goes_through_ttl_to_dlq,
    где payment успешно переходит в SUCCEEDED, а падает только webhook.
    """
    monkeypatch.setattr(settings, "WEBHOOK_ALLOW_PRIVATE", True)

    # Создаём платёж в статусе PENDING
    payment = _pending_payment("http://127.0.0.1:9999/webhook")
    async with postgres() as session:
        session.add(payment)
        await session.commit()

    # Consumer с FailingGateway, который падает до UPDATE статуса
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
        _publish_retry_with(broker),
    )
    broker.subscriber(PAYMENTS_NEW_QUEUE, exchange=PAYMENTS_EXCHANGE)(handler)
    await broker.start()

    event_id = str(uuid4())
    payload = {"payment_id": str(payment.id), "event_id": event_id}

    conn = await aio_pika.connect(rabbit_test_url)
    try:
        dlq_observer = await _declare_dlq_observer(conn)
        retry1_observer = await _declare_retry_observer(conn, "retry.1")
        retry2_observer = await _declare_retry_observer(conn, "retry.2")

        # Публикуем событие
        await _publish_event(payload, broker)

        # Ждём, что сообщение пройдёт через retry-очереди и уйдёт в DLQ
        message = await _wait_for_dead_lettered(dlq_observer, event_id, timeout=30)
        assert message.message_id == event_id
        assert message.headers["x-retry-count"] == 2  # Две повторных попытки
        assert message.routing_key == DLQ_ROUTING_KEY
        await message.ack()

        # Проверяем, что сообщение действительно проходило через TTL-очереди
        assert await _observer_saw(retry1_observer, event_id, timeout=5)
        assert await _observer_saw(retry2_observer, event_id, timeout=5)
    finally:
        await conn.close()
        await broker.stop()

    # Gateway вызывался на каждой из трёх попыток (платёж всё ещё pending)
    assert len(gateway.calls) == 3
    assert gateway.calls == [payment.id, payment.id, payment.id]

    # КРИТИЧЕСКАЯ ПРОВЕРКА: платёж остался PENDING
    async with consumer_session_factory() as session:
        row = await session.get(Payment, payment.id)
        assert row is not None
        assert row.status == PaymentStatus.PENDING, (
            f"Payment must stay PENDING after technical failures exhaust retries, "
            f"but got {row.status}. This is a critical edge case: "
            f"terminal status (succeeded/failed) means gateway completed successfully, "
            f"but PENDING after DLQ means technical error prevented status UPDATE."
        )
        assert row.processed_at is None, (
            "processed_at must be NULL when status is PENDING after DLQ"
        )
