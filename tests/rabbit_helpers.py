"""Общая RabbitMQ-обвязка интеграционных тестов consumer'а: наблюдатели
DLX и TTL-очередей, ожидание dead-letter, публикация событий,
подтверждённый retry-publisher."""

import asyncio
from typing import Any
from uuid import uuid4

import pytest
from aio_pika.abc import AbstractConnection, AbstractMessage, AbstractQueue
from faststream.rabbit import RabbitBroker

from app.consumer.handler import RetryPublisher
from app.outbox.rabbit import PAYMENTS_EXCHANGE, publish_to_retry

DLX_NAME = "payments.dlx"
DLQ_ROUTING_KEY = "payments.new.dlq"
RETRY_EXCHANGE_NAME = "payments.retry"


async def declare_dlq_observer(conn: AbstractConnection) -> AbstractQueue:
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


async def declare_retry_observer(
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


async def observer_saw(
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


async def wait_for_dead_lettered(
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


def publish_retry_with(broker: RabbitBroker) -> RetryPublisher:
    async def publish_retry(
        *, payload: dict[str, Any], event_id: str, next_attempt: int
    ) -> bool:
        return await publish_to_retry(
            broker, payload=payload, event_id=event_id, next_attempt=next_attempt
        )

    return publish_retry


async def publish_event(payload: dict, broker: RabbitBroker) -> None:
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
