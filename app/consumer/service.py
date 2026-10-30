from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumer.gateway import PaymentGateway
from app.payments.dto import PaymentDetailDTO
from app.payments.exceptions import PaymentNotFound
from app.payments.models import Payment, PaymentStatus


@dataclass(frozen=True, slots=True)
class ProcessResult:
    payment_id: UUID
    status: PaymentStatus
    gateway_called: bool
    detail: PaymentDetailDTO


async def process_payment_event(
    session_factory: async_sessionmaker[AsyncSession],
    gateway: PaymentGateway,
    payment_id: UUID,
) -> ProcessResult:
    """Обработка одного события payments.new.

    Шлюз вызывается только для pending-платежа и вне транзакций БД;
    терминальный статус и processed_at записываются короткой транзакцией
    под row lock. Повторная доставка для терминального платежа шлюз
    не вызывает.
    """
    async with session_factory() as session:
        payment = await session.get(Payment, payment_id)
    if payment is None:
        # Яд: события без платежа в БД уходит в DLQ (REJECT_ON_ERROR).
        raise PaymentNotFound(payment_id)
    # status приходит из БД строкой; PaymentStatus (StrEnum) равен строке,
    # поэтому сравнение и приведение — по значению.
    current_status = PaymentStatus(payment.status)
    if current_status is not PaymentStatus.PENDING:
        return ProcessResult(
            payment_id,
            current_status,
            gateway_called=False,
            detail=PaymentDetailDTO.from_model(payment),
        )

    result = await gateway.process(payment_id)
    status = PaymentStatus.SUCCEEDED if result.success else PaymentStatus.FAILED

    async with session_factory() as session:
        locked = await session.get(Payment, payment_id, with_for_update=True)
        if locked is None:
            raise PaymentNotFound(payment_id)
        if PaymentStatus(locked.status) is PaymentStatus.PENDING:
            locked.status = status
            locked.processed_at = datetime.now(UTC)
            await session.commit()
        else:
            status = PaymentStatus(locked.status)
    return ProcessResult(
        payment_id,
        status,
        gateway_called=True,
        detail=PaymentDetailDTO.from_model(locked),
    )
