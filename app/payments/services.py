from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession

from app.outbox.models import OUTBOX_EVENT_PAYMENTS_NEW, OutboxEvent
from app.payments.dto import PaymentCreateDTO, PaymentDetailDTO
from app.payments.exceptions import IdempotencyConflict, PaymentNotFound
from app.payments.hashing import canonical_request_hash
from app.payments.models import Payment, PaymentStatus

IDEMPOTENCY_CONSTRAINT = "uq_payments_idempotency_key"


def _is_idempotency_unique_violation(exc: IntegrityError) -> bool:
    orig = exc.orig
    constraint_name = getattr(orig, "constraint_name", None)
    if constraint_name == IDEMPOTENCY_CONSTRAINT:
        return True
    return IDEMPOTENCY_CONSTRAINT in str(orig)


async def _get_by_idempotency_key(
    session: AsyncSession,
    idempotency_key: str,
) -> Payment | None:
    return await session.scalar(
        select(Payment).where(Payment.idempotency_key == idempotency_key)
    )


async def create_payment(
    session: AsyncSession,
    data: PaymentCreateDTO,
) -> PaymentDetailDTO:
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
    event = OutboxEvent(
        id=event_id,
        event_type=OUTBOX_EVENT_PAYMENTS_NEW,
        aggregate_id=payment_id,
        payload={
            "payment_id": str(payment_id),
            "event_id": str(event_id),
        },
    )
    session.add(payment)
    session.add(event)
    try:
        await session.flush()
    except IntegrityError as exc:
        # PostgreSQL aborts the transaction on IntegrityError; rollback
        # before any further SQL, then read in a new transaction.
        await session.rollback()
        if not _is_idempotency_unique_violation(exc):
            raise
        existing = await _get_by_idempotency_key(session, data.idempotency_key)
        if existing is None:
            raise
        if existing.request_hash != request_hash:
            raise IdempotencyConflict(data.idempotency_key) from exc
        return PaymentDetailDTO.from_model(existing)
    return PaymentDetailDTO.from_model(payment)


async def get_payment_by_id(
    session: AsyncSession,
    payment_id: UUID,
) -> PaymentDetailDTO:
    payment = await session.get(Payment, payment_id)
    if payment is None:
        raise PaymentNotFound(payment_id)
    return PaymentDetailDTO.from_model(payment)
