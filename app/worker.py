import asyncio
import logging

from faststream import FastStream
from faststream.rabbit import Channel, RabbitBroker

from app.config import settings
from app.consumer.gateway import DeterministicGateway
from app.consumer.handler import build_payments_new_handler
from app.consumer.webhook import HttpxWebhookSender
from app.database import SessionFactory, engine
from app.outbox.models import OutboxEvent
from app.outbox.rabbit import (
    PAYMENTS_EXCHANGE,
    PAYMENTS_NEW_QUEUE,
    declare_payments_topology,
    publish_outbox_event,
    publish_to_retry,
)
from app.outbox.relay import OutboxRelay

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

broker = RabbitBroker(
    str(settings.RABBITMQ_URL),
    default_channel=Channel(publisher_confirms=True, on_return_raises=True),
)
app = FastStream(broker)


async def _publish_event(event: OutboxEvent) -> bool:
    return await publish_outbox_event(broker, event)


async def _publish_retry(payload: dict, event_id: str, next_attempt: int) -> bool:
    return await publish_to_retry(
        broker, payload=payload, event_id=event_id, next_attempt=next_attempt
    )


relay = OutboxRelay(SessionFactory, _publish_event)

handle_payments_new = build_payments_new_handler(
    SessionFactory,
    DeterministicGateway(),
    HttpxWebhookSender(),
    _publish_retry,
)
broker.subscriber(PAYMENTS_NEW_QUEUE, exchange=PAYMENTS_EXCHANGE)(handle_payments_new)


@app.after_startup
async def start_outbox_relay() -> None:
    await declare_payments_topology(broker)
    await relay.start()


@app.on_shutdown
async def stop_outbox_relay() -> None:
    await relay.stop()


@app.after_shutdown
async def dispose_engine() -> None:
    await engine.dispose()


def main() -> None:
    asyncio.run(app.run())


if __name__ == "__main__":
    main()
