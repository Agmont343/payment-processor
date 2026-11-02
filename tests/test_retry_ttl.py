"""Интеграционные тесты TTL/DLX на реальном RabbitMQ + PostgreSQL.

Проверяют то, что нельзя воспроизвести in-memory: сохранение message_id,
заголовков и routing key при прохождении TTL-очередей и DLX. Изоляция —
payments_test и тестовый vhost (см. conftest).
"""

import json
import threading
from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any
from uuid import uuid4

import aio_pika
import pytest
from aio_pika.abc import AbstractMessage
from faststream.rabbit import Channel, RabbitBroker

from app.config import settings
from app.consumer.handler import build_payments_new_handler
from app.consumer.webhook import HttpxWebhookSender
from app.outbox.rabbit import (
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
    declare_payments_topology,
)
from app.payments.models import Payment, PaymentStatus
from tests.conftest import SessionFactory
from tests.helpers import RecordingGateway, make_payment
from tests.rabbit_helpers import (
    DLQ_ROUTING_KEY,
    declare_dlq_observer,
    declare_retry_observer,
    observer_saw,
    publish_event,
    publish_retry_with,
    wait_for_dead_lettered,
)


class _StaticStatusHandler(BaseHTTPRequestHandler):
    status_code = HTTPStatus.INTERNAL_SERVER_ERROR
    received: list[str] = []

    def do_POST(self) -> None:  # noqa: N802
        self.rfile.read(int(self.headers.get("Content-Length", 0)))
        self.received.append(self.path)
        self.send_response(self.status_code)
        self.end_headers()

    def log_message(self, *_args: Any) -> None:
        pass


@dataclass
class _WebhookServer:
    """Тестовый webhook-сервер: URL и пути фактически полученных POST."""

    url: str
    received: list[str] = field(default_factory=list)


@pytest.fixture
def webhook_server() -> Iterator[Callable[[int], _WebhookServer]]:
    def make(status_code: int) -> _WebhookServer:
        received: list[str] = []
        handler = type(
            "H",
            (_StaticStatusHandler,),
            {"status_code": status_code, "received": received},
        )
        server = HTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        return _WebhookServer(
            url=f"http://127.0.0.1:{server.server_port}/hook",
            received=received,
        )

    yield make


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


def json_body(message: AbstractMessage) -> dict[str, Any]:
    return json.loads(message.body.decode("utf-8"))


async def test_transient_webhook_goes_through_ttl_to_dlq(
    postgres: SessionFactory,
    consumer_session_factory: SessionFactory,
    webhook_server: Callable[[int], _WebhookServer],
    monkeypatch: pytest.MonkeyPatch,
    rabbit_test_url: str,
) -> None:
    # Consumer читает production settings (app.config.settings -> .env,
    # WEBHOOK_ALLOW_PRIVATE=false): приватный адрес тестового webhook-сервера
    # разрешаем явно, иначе URL блокируется до HTTP-вызова.
    monkeypatch.setattr(settings, "WEBHOOK_ALLOW_PRIVATE", True)
    webhook = webhook_server(HTTPStatus.INTERNAL_SERVER_ERROR)
    payment = make_payment(webhook.url)
    async with postgres() as session:
        session.add(payment)
        await session.commit()

    broker = RabbitBroker(
        rabbit_test_url, default_channel=Channel(publisher_confirms=True)
    )
    await broker.connect()
    await declare_payments_topology(broker)
    gateway = RecordingGateway(results=[True])
    handler = build_payments_new_handler(
        consumer_session_factory,
        gateway,
        HttpxWebhookSender(),
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
        assert await observer_saw(retry1_observer, event_id, timeout=5)
        assert await observer_saw(retry2_observer, event_id, timeout=5)

        # Webhook-сервер получил реальный HTTP-запрос каждой из трёх попыток:
        # 500 классифицирован как transient, а не блокирован URL-фильтром.
        assert webhook.received == ["/hook", "/hook", "/hook"]
    finally:
        await conn.close()
        await broker.stop()

    assert gateway.calls == [payment.id]
    async with consumer_session_factory() as session:
        row = await session.get(Payment, payment.id)
        assert row is not None
        assert row.status == PaymentStatus.SUCCEEDED
        assert row.processed_at is not None


async def test_permanent_4xx_goes_straight_to_dlq(
    postgres: SessionFactory,
    consumer_session_factory: SessionFactory,
    webhook_server: Callable[[int], _WebhookServer],
    monkeypatch: pytest.MonkeyPatch,
    rabbit_test_url: str,
) -> None:
    # Consumer читает production settings (app.config.settings -> .env,
    # WEBHOOK_ALLOW_PRIVATE=false): приватный адрес тестового webhook-сервера
    # разрешаем явно, иначе URL блокируется до HTTP-вызова.
    monkeypatch.setattr(settings, "WEBHOOK_ALLOW_PRIVATE", True)
    webhook = webhook_server(HTTPStatus.BAD_REQUEST)
    payment = make_payment(webhook.url)
    async with postgres() as session:
        session.add(payment)
        await session.commit()

    broker = RabbitBroker(
        rabbit_test_url, default_channel=Channel(publisher_confirms=True)
    )
    await broker.connect()
    await declare_payments_topology(broker)
    gateway = RecordingGateway(results=[True])
    handler = build_payments_new_handler(
        consumer_session_factory,
        gateway,
        HttpxWebhookSender(),
        publish_retry_with(broker),
    )
    broker.subscriber(PAYMENTS_NEW_QUEUE, exchange=PAYMENTS_EXCHANGE)(handler)
    await broker.start()

    event_id = str(uuid4())

    conn = await aio_pika.connect(rabbit_test_url)
    try:
        dlq_observer = await declare_dlq_observer(conn)
        retry1_observer = await declare_retry_observer(conn, "retry.1")
        retry2_observer = await declare_retry_observer(conn, "retry.2")
        await publish_event(
            {"payment_id": str(payment.id), "event_id": event_id}, broker
        )

        message = await wait_for_dead_lettered(dlq_observer, event_id, timeout=15)
        # 4xx -> DLQ с первой попытки, без проходов через TTL.
        assert message.message_id == event_id
        assert message.routing_key == DLQ_ROUTING_KEY
        assert "x-retry-count" not in message.headers
        await message.ack()

        assert not await observer_saw(retry1_observer, event_id, timeout=2)
        assert not await observer_saw(retry2_observer, event_id, timeout=2)

        # Ровно один реальный HTTP-запрос: DLQ по настоящему HTTP 400,
        # URL не блокирован фильтром, повторных вызовов нет.
        assert webhook.received == ["/hook"]
    finally:
        await conn.close()
        await broker.stop()

    assert gateway.calls == [payment.id]
    async with consumer_session_factory() as session:
        row = await session.get(Payment, payment.id)
        assert row is not None
        assert row.status == PaymentStatus.SUCCEEDED
