import logging
from collections.abc import Awaitable, Callable
from typing import Annotated, Any, Protocol
from uuid import UUID

from faststream import Context
from faststream.exceptions import AckMessage, NackMessage, RejectMessage
from faststream.rabbit.message import RabbitMessage
from pydantic import BaseModel
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumer.gateway import PaymentGateway
from app.consumer.service import PaymentProcessingService, ProcessResult
from app.consumer.webhook import WebhookResult, WebhookSender
from app.payments.exceptions import PaymentNotFound

logger = logging.getLogger(__name__)

MAX_WEBHOOK_ATTEMPTS = 3
RETRY_HEADER = "x-retry-count"


class PaymentEventPayload(BaseModel):
    payment_id: UUID
    event_id: UUID


class RetryPublisher(Protocol):
    """Публикация копии события в TTL-очередь попытки next_attempt.

    True — публикация подтверждена брокером (Basic.Ack); False или
    исключение означают, что исходное сообщение не должно быть ACK-нуто.
    """

    async def __call__(
        self, *, payload: dict[str, Any], event_id: str, next_attempt: int
    ) -> bool: ...


def build_payments_new_handler(
    session_factory: async_sessionmaker[AsyncSession],
    gateway: PaymentGateway,
    webhook_sender: WebhookSender,
    publish_retry: RetryPublisher,
) -> Callable[[PaymentEventPayload, RabbitMessage], Awaitable[None]]:
    # Создаём PaymentProcessingService один раз при инициализации
    processing_service = PaymentProcessingService(session_factory, gateway)

    async def handle_payments_new(
        message: PaymentEventPayload,
        rabbit_message: Annotated[RabbitMessage, Context("message")],
    ) -> None:
        retry_count = _retry_count(rabbit_message)
        logger.info(
            "Event %s received for payment %s (attempt %d)",
            message.event_id,
            message.payment_id,
            retry_count + 1,
        )
        try:
            result = await processing_service.process_payment(message.payment_id)
        except PaymentNotFound:
            # Яд уходит в DLQ дефолтной политикой REJECT_ON_ERROR.
            raise
        except Exception:
            # Техническая ошибка до фиксации терминального статуса:
            # платёж может остаться pending, повторяется вся обработка.
            logger.exception("Processing failed for payment %s", message.payment_id)
            await _retry_or_dlq(message, retry_count, publish_retry)
            return

        logger.info(
            "Payment %s processed: status=%s, gateway_called=%s",
            result.payment_id,
            result.status.value,
            result.gateway_called,
        )
        outcome = await _deliver_webhook(message, result, retry_count, webhook_sender)
        if outcome is WebhookResult.SUCCESS:
            return
        if outcome is WebhookResult.PERMANENT:
            logger.warning(
                "Webhook permanently failed for payment %s -> DLQ",
                result.payment_id,
            )
            raise RejectMessage()
        await _retry_or_dlq(message, retry_count, publish_retry)

    return handle_payments_new


async def _deliver_webhook(
    message: PaymentEventPayload,
    result: ProcessResult,
    retry_count: int,
    webhook_sender: WebhookSender,
) -> WebhookResult:
    body = _webhook_body(message, result)
    try:
        outcome = await webhook_sender.send(result.detail.webhook_url, body)
    except Exception:
        logger.exception("Webhook sender failed for payment %s", result.payment_id)
        return WebhookResult.TRANSIENT
    if outcome is WebhookResult.SUCCESS:
        logger.info("Webhook delivered for payment %s", result.payment_id)
    else:
        logger.warning(
            "Webhook %s for payment %s (attempt %d)",
            outcome.value,
            result.payment_id,
            retry_count + 1,
        )
    return outcome


async def _retry_or_dlq(
    message: PaymentEventPayload,
    retry_count: int,
    publish_retry: RetryPublisher,
) -> None:
    """Публикация в TTL-очередь с confirm-then-ack либо DLQ.

    x-retry-count растёт только у реально опубликованной копии;
    неподтверждённая публикация -> NackMessage(requeue=True), исходное
    сообщение возвращается в payments.new с прежним счётчиком.
    """
    if retry_count + 1 >= MAX_WEBHOOK_ATTEMPTS:
        logger.warning("Attempts exhausted for event %s -> DLQ", message.event_id)
        raise RejectMessage()
    payload = {
        "payment_id": str(message.payment_id),
        "event_id": str(message.event_id),
    }
    try:
        confirmed = await publish_retry(
            payload=payload,
            event_id=str(message.event_id),
            next_attempt=retry_count + 1,
        )
    except Exception:
        logger.exception("Retry publish failed for event %s", message.event_id)
        confirmed = False
    if confirmed:
        raise AckMessage()
    raise NackMessage(requeue=True)


def _webhook_body(
    message: PaymentEventPayload, result: ProcessResult
) -> dict[str, Any]:
    return {
        "payment_id": str(result.payment_id),
        "status": result.status.value,
        "amount": str(result.detail.amount),
        "currency": str(result.detail.currency),
        "processed_at": (
            result.detail.processed_at.isoformat()
            if result.detail.processed_at is not None
            else None
        ),
        "event_id": str(message.event_id),
    }


def _retry_count(rabbit_message: RabbitMessage) -> int:
    value = rabbit_message.headers.get(RETRY_HEADER, 0)
    try:
        return int(value)
    except (TypeError, ValueError):
        logger.warning(
            "Invalid %s header %r; treating as first attempt",
            RETRY_HEADER,
            value,
        )
        return 0
