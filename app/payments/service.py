from uuid import UUID, uuid4

from app.outbox.models import OUTBOX_EVENT_PAYMENTS_NEW, OutboxEvent
from app.outbox.repository import OutboxRepositoryProtocol
from app.payments.dto import PaymentCreateDTO, PaymentDetailDTO
from app.payments.exceptions import IdempotencyConflict, PaymentNotFound
from app.payments.hashing import canonical_request_hash
from app.payments.models import Payment, PaymentStatus
from app.payments.repository import PaymentRepositoryProtocol


class PaymentService:
    """Сервис для управления платежами."""

    def __init__(
        self,
        payment_repo: PaymentRepositoryProtocol,
        outbox_repo: OutboxRepositoryProtocol,
    ) -> None:
        self._payment_repo = payment_repo
        self._outbox_repo = outbox_repo

    async def create_payment(
        self, data: PaymentCreateDTO
    ) -> tuple[PaymentDetailDTO, bool]:
        """Создать или получить существующий платёж (idempotent).

        Возвращает (payment, created), где created=True если платёж был создан.

        Атомарность:
        - Payment создаётся через INSERT ... ON CONFLICT
        - Outbox Event создаётся только при фактическом создании Payment
        - Оба в одной транзакции

        Raises:
        - IdempotencyConflict: idempotency_key существует, но request_hash отличается
        """
        request_hash = canonical_request_hash(
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            metadata=data.metadata,
            webhook_url=data.webhook_url,
        )

        payment_id = uuid4()
        event_id = uuid4()

        payment = Payment(
            id=payment_id,
            amount=data.amount,
            currency=data.currency,
            description=data.description,
            metadata_json=data.metadata,
            status=PaymentStatus.PENDING,
            idempotency_key=data.idempotency_key,
            request_hash=request_hash,
            webhook_url=data.webhook_url,
        )

        # INSERT ... ON CONFLICT атомарно обрабатывает конкурентные вставки
        result_payment, created = await self._payment_repo.insert_or_get_existing(
            payment
        )

        if not created:
            # Payment уже существует (идемпотентный replay)
            if result_payment.request_hash != request_hash:
                raise IdempotencyConflict(data.idempotency_key)
            return PaymentDetailDTO.from_model(result_payment), False

        # Payment создан → создаём Outbox Event
        event = OutboxEvent(
            id=event_id,
            event_type=OUTBOX_EVENT_PAYMENTS_NEW,
            aggregate_id=payment_id,
            payload={
                "payment_id": str(payment_id),
                "event_id": str(event_id),
            },
        )

        self._outbox_repo.add(event)

        return PaymentDetailDTO.from_model(result_payment), True

    async def get_payment_by_id(self, payment_id: UUID) -> PaymentDetailDTO:
        """Получить платёж по ID."""
        payment = await self._payment_repo.get_by_id(payment_id)
        if payment is None:
            raise PaymentNotFound(payment_id)
        return PaymentDetailDTO.from_model(payment)
