from dataclasses import dataclass
from datetime import UTC, datetime
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumer.gateway import PaymentGateway
from app.database import transaction_scope
from app.payments.dto import PaymentDetailDTO
from app.payments.exceptions import PaymentNotFound
from app.payments.models import PaymentStatus
from app.payments.repository import PaymentRepository


@dataclass(frozen=True, slots=True)
class ProcessResult:
    payment_id: UUID
    status: PaymentStatus
    gateway_called: bool
    detail: PaymentDetailDTO


class PaymentProcessingService:
    """Двухфазная обработка платежа через внешний Gateway.

    Фаза 1: Чтение payment (короткая транзакция)
    Фаза 2: Вызов Gateway (вне транзакции, детерминированный)
    Фаза 3: UPDATE payment с lock (короткая транзакция)

    Gateway идемпотентен по payment_id, поэтому повторный вызов
    при crash возвращает тот же результат (но с задержкой).
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        gateway: PaymentGateway,
    ) -> None:
        self._session_factory = session_factory
        self._gateway = gateway

    async def process_payment(self, payment_id: UUID) -> ProcessResult:
        # Фаза 1: Чтение payment (короткая транзакция < 10ms)
        async with transaction_scope(self._session_factory) as read_session:
            payment_repo = PaymentRepository(read_session)
            payment = await payment_repo.get_by_id(payment_id)

            if payment is None:
                raise PaymentNotFound(payment_id)

            current_status = PaymentStatus(payment.status)

            # Если терминальный статус — skip Gateway
            if current_status is not PaymentStatus.PENDING:
                return ProcessResult(
                    payment_id=payment_id,
                    status=current_status,
                    gateway_called=False,
                    detail=PaymentDetailDTO.from_model(payment),
                )

            # Транзакция завершается (commit автоматически)

        # Фаза 2: Gateway вне транзакции (2-5 секунд)
        # Детерминирован по payment_id → повторный вызов безопасен
        gateway_result = await self._gateway.process(payment_id)
        new_status = (
            PaymentStatus.SUCCEEDED if gateway_result.success else PaymentStatus.FAILED
        )

        # Фаза 3: UPDATE с row lock (короткая транзакция < 10ms)
        async with transaction_scope(self._session_factory) as update_session:
            payment_repo = PaymentRepository(update_session)
            locked_payment = await payment_repo.get_by_id_with_lock(payment_id)

            if locked_payment is None:
                # Крайне редкий случай: payment удалён между фазами
                raise PaymentNotFound(payment_id)

            # Double-check: статус мог измениться (другой worker)
            if PaymentStatus(locked_payment.status) is PaymentStatus.PENDING:
                locked_payment.status = new_status
                locked_payment.processed_at = datetime.now(UTC)
                final_status = new_status
            else:
                # Другой worker уже обработал
                final_status = PaymentStatus(locked_payment.status)

            # Commit автоматически через transaction_scope

            return ProcessResult(
                payment_id=payment_id,
                status=final_status,
                gateway_called=True,
                detail=PaymentDetailDTO.from_model(locked_payment),
            )
