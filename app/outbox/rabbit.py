from faststream.rabbit import (
    ExchangeType,
    QueueType,
    RabbitBroker,
    RabbitExchange,
    RabbitQueue,
)
from pamqp.commands import Basic

from app.outbox.models import OUTBOX_EVENT_PAYMENTS_NEW, OutboxEvent

PAYMENTS_EXCHANGE = RabbitExchange(
    "payments",
    type=ExchangeType.TOPIC,
    durable=True,
)
PAYMENTS_DLX = RabbitExchange(
    "payments.dlx",
    type=ExchangeType.DIRECT,
    durable=True,
)
PAYMENTS_NEW_QUEUE = RabbitQueue(
    "payments.new",
    durable=True,
    queue_type=QueueType.CLASSIC,
    routing_key=OUTBOX_EVENT_PAYMENTS_NEW,
    arguments={
        "x-dead-letter-exchange": "payments.dlx",
        "x-dead-letter-routing-key": "payments.new.dlq",
    },
)
PAYMENTS_DLQ = RabbitQueue(
    "payments.new.dlq",
    durable=True,
    queue_type=QueueType.CLASSIC,
    routing_key="payments.new.dlq",
)
PAYMENTS_RETRY_EXCHANGE = RabbitExchange(
    "payments.retry",
    type=ExchangeType.DIRECT,
    durable=True,
)
RETRY_TTL_MS = {1: 1000, 2: 2000}


def retry_queue(attempt: int) -> RabbitQueue:
    """TTL-очередь попытки: истёкшее сообщение возвращается в payments.new.

    Заголовки (включая x-retry-count) и message_id переживают dead-letter.
    """
    return RabbitQueue(
        f"payments.retry.{attempt}",
        durable=True,
        queue_type=QueueType.CLASSIC,
        arguments={
            "x-message-ttl": RETRY_TTL_MS[attempt],
            "x-dead-letter-exchange": "payments",
            "x-dead-letter-routing-key": OUTBOX_EVENT_PAYMENTS_NEW,
        },
    )


def is_publisher_ack(confirm: object | None) -> bool:
    return isinstance(confirm, Basic.Ack)


async def declare_payments_topology(broker: RabbitBroker) -> None:
    payments = await broker.declare_exchange(PAYMENTS_EXCHANGE)
    dlx = await broker.declare_exchange(PAYMENTS_DLX)
    retry = await broker.declare_exchange(PAYMENTS_RETRY_EXCHANGE)
    new_queue = await broker.declare_queue(PAYMENTS_NEW_QUEUE)
    dlq = await broker.declare_queue(PAYMENTS_DLQ)
    await new_queue.bind(payments, routing_key=OUTBOX_EVENT_PAYMENTS_NEW)
    await dlq.bind(dlx, routing_key="payments.new.dlq")
    for attempt in RETRY_TTL_MS:
        queue = await broker.declare_queue(retry_queue(attempt))
        await queue.bind(retry, routing_key=f"retry.{attempt}")


async def publish_outbox_event(broker: RabbitBroker, event: OutboxEvent) -> bool:
    confirm = await broker.publish(
        event.payload,
        exchange=PAYMENTS_EXCHANGE,
        routing_key=event.event_type,
        persist=True,
        mandatory=True,
        message_id=str(event.id),
        correlation_id=str(event.id),
        message_type=event.event_type,
    )
    return is_publisher_ack(confirm)


async def publish_to_retry(
    broker: RabbitBroker,
    *,
    payload: dict,
    event_id: str,
    next_attempt: int,
) -> bool:
    """Публикация в TTL-очередь попытки next_attempt с подтверждением.

    Свойства сообщения задаются явно: свойства исходного сообщения
    RabbitMQ не копирует автоматически при повторной публикации.
    """
    confirm = await broker.publish(
        payload,
        exchange=PAYMENTS_RETRY_EXCHANGE,
        routing_key=f"retry.{next_attempt}",
        persist=True,
        mandatory=True,
        message_id=event_id,
        correlation_id=event_id,
        message_type=OUTBOX_EVENT_PAYMENTS_NEW,
        headers={"x-retry-count": next_attempt},
    )
    return is_publisher_ack(confirm)
