"""Интеграционные тесты этапа 5 на реальном RabbitMQ + PostgreSQL.

Требуют поднятых сервисов; скипаются, если брокер или БД недоступны.
Проверяют поведение TTL/DLX, которое нельзя воспроизвести in-memory:
сохранение message_id, заголовков и routing key при прохождении DLX.

Изоляция: PostgreSQL — только payments_test, RabbitMQ — только тестовый
vhost (см. conftest), поэтому тесты не зависят от работающего consumer.
"""

import asyncio
import json
import threading
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from uuid import UUID, uuid4

import aio_pika
import pytest
from aio_pika.abc import AbstractConnection, AbstractMessage, AbstractQueue
from faststream.rabbit import Channel, RabbitBroker
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.config import settings
from app.consumer.gateway import GatewayResult
from app.consumer.handler import build_payments_new_handler
from app.consumer.webhook import HttpxWebhookSender
from app.outbox.rabbit import (
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
    declare_payments_topology,
    publish_to_retry,
)
from app.payments.models import Currency, Payment, PaymentStatus


class _StaticStatusHandler(BaseHTTPRequestHandler):
    status_code = 500

    def do_POST(self) -> None:  # noqa: N802
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        self.send_response(self.status_code)
        self.end_headers()

    def log_message(self, *_args: Any) -> None:
        pass


@pytest.fixture
def webhook_server():
    def make(status_code: int) -> str:
        handler = type("H", (_StaticStatusHandler,), {"status_code": status_code})
        server = HTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        return f"http://127.0.0.1:{server.server_port}/hook"

    yield make


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


class InstantGateway:
    """Stub-шлюз: успешный исход без задержки, с подсчётом вызовов."""

    def __init__(self) -> None:
        self.calls: list[UUID] = []

    async def process(self, payment_id: UUID) -> GatewayResult:
        self.calls.append(payment_id)
        return GatewayResult(success=True)


def _pending_payment(webhook_url: str) -> Payment:
    return Payment(
        id=uuid4(),
        amount=Decimal("25.50"),
        currency=Currency.USD,
        description="stage5 integration",
        metadata_json={},
        status=PaymentStatus.PENDING,
        idempotency_key=f"key-{uuid4()}",
        request_hash="hash",
        webhook_url=webhook_url,
    )


def _publish_retry_with(broker: RabbitBroker):
    async def publish_retry(*, payload: dict, event_id: str, next_attempt: int) -> bool:
        return await publish_to_retry(
            broker, payload=payload, event_id=event_id, next_attempt=next_attempt
        )

    return publish_retry


DLX_NAME = "payments.dlx"
DLQ_ROUTING_KEY = "payments.new.dlq"
RETRY_EXCHANGE_NAME = "payments.retry"


async def _declare_dlq_observer(conn: AbstractConnection) -> AbstractQueue:
    """Эксклюзивная очередь-наблюдатель на том же DLX и с тем же routing key.

    Боевая payments.new.dlq в dev содержит сообщения чужих прогонов, поэтому
    тест читает копию своего события из временной очереди и не трогает чужие
    сообщения. Копия появляется только если сообщение реально ушло в DLX с
    routing key боевой DLQ; очередь удаляется вместе с соединением.
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
    """Эксклюзивная очередь-наблюдатель за публикациями в TTL-очереди.

    Нужна потому, что x-death не показывает весь путь: публикация в
    payments.retry.N — обычная републикация нового сообщения, и накопленные
    записи x-death при ней теряются.
    """
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
) -> AbstractMessage:
    """Дождаться своего dead-letter по message_id.

    Чужие копии ack-аются только в собственной временной очереди, боевая DLQ
    остаётся нетронутой.
    """
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


def _expired_in(deaths: list[dict], queue_name: str) -> bool:
    """Прохождение через TTL-очередь: только наличие и происхождение.

    Полный путь по x-death не восстанавливается: републикация в
    payments.retry.N сбрасывает накопленные записи, поэтому проверяем
    последнюю TTL-очередь и отдельно копии публикаций через наблюдателя.
    """
    return any(
        death.get("queue") == queue_name and death.get("reason") == "expired"
        for death in deaths
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


async def test_transient_webhook_goes_through_ttl_to_dlq(
    postgres, consumer_session_factory, webhook_server, monkeypatch, rabbit_test_url
) -> None:
    monkeypatch.setattr(settings, "WEBHOOK_ALLOW_PRIVATE", True)
    payment = _pending_payment(webhook_server(500))
    async with postgres() as session:
        session.add(payment)
        await session.commit()

    broker = RabbitBroker(
        rabbit_test_url, default_channel=Channel(publisher_confirms=True)
    )
    await broker.connect()
    await declare_payments_topology(broker)
    gateway = InstantGateway()
    handler = build_payments_new_handler(
        consumer_session_factory,
        gateway,
        HttpxWebhookSender(),
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
        await _publish_event(payload, broker)

        message = await _wait_for_dead_lettered(dlq_observer, event_id, timeout=30)
        # Три попытки исчерпаны: свойства сообщения сохранены при
        # прохождении TTL-очередей и DLX.
        assert message.message_id == event_id
        assert message.headers["x-retry-count"] == 2
        assert message.routing_key == DLQ_ROUTING_KEY
        deaths = message.headers.get("x-death", [])
        assert deaths, "x-death must be present after DLX passes"
        assert _expired_in(deaths, "payments.retry.2")
        assert json_body(message) == payload
        await message.ack()

        # Сообщение действительно проходило через обе TTL-очереди, а не
        # пересылалось напрямую в DLX.
        assert await _observer_saw(retry1_observer, event_id, timeout=5)
        assert await _observer_saw(retry2_observer, event_id, timeout=5)
    finally:
        await conn.close()
        await broker.stop()

    # Повторялись только webhook-доставки: gateway вызван один раз.
    assert gateway.calls == [payment.id]
    async with consumer_session_factory() as session:
        row = await session.get(Payment, payment.id)
        assert row is not None
        assert row.status == PaymentStatus.SUCCEEDED
        assert row.processed_at is not None


async def test_permanent_4xx_goes_straight_to_dlq(
    postgres, consumer_session_factory, webhook_server, monkeypatch, rabbit_test_url
) -> None:
    monkeypatch.setattr(settings, "WEBHOOK_ALLOW_PRIVATE", True)
    payment = _pending_payment(webhook_server(400))
    async with postgres() as session:
        session.add(payment)
        await session.commit()

    broker = RabbitBroker(
        rabbit_test_url, default_channel=Channel(publisher_confirms=True)
    )
    await broker.connect()
    await declare_payments_topology(broker)
    gateway = InstantGateway()
    handler = build_payments_new_handler(
        consumer_session_factory,
        gateway,
        HttpxWebhookSender(),
        _publish_retry_with(broker),
    )
    broker.subscriber(PAYMENTS_NEW_QUEUE, exchange=PAYMENTS_EXCHANGE)(handler)
    await broker.start()

    event_id = str(uuid4())

    conn = await aio_pika.connect(rabbit_test_url)
    try:
        dlq_observer = await _declare_dlq_observer(conn)
        retry1_observer = await _declare_retry_observer(conn, "retry.1")
        retry2_observer = await _declare_retry_observer(conn, "retry.2")
        await _publish_event(
            {"payment_id": str(payment.id), "event_id": event_id}, broker
        )

        message = await _wait_for_dead_lettered(dlq_observer, event_id, timeout=15)
        # 4xx -> DLQ с первой попытки, без проходов через TTL.
        assert message.message_id == event_id
        assert message.routing_key == DLQ_ROUTING_KEY
        assert "x-retry-count" not in message.headers
        await message.ack()

        # Ни одной публикации в TTL-очередь: повторных попыток нет.
        assert not await _observer_saw(retry1_observer, event_id, timeout=2)
        assert not await _observer_saw(retry2_observer, event_id, timeout=2)
    finally:
        await conn.close()
        await broker.stop()

    assert gateway.calls == [payment.id]
    async with consumer_session_factory() as session:
        row = await session.get(Payment, payment.id)
        assert row is not None
        assert row.status == PaymentStatus.SUCCEEDED


def json_body(message) -> dict:
    return json.loads(message.body.decode("utf-8"))
