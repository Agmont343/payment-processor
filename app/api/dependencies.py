from typing import Annotated

from fastapi import Depends

from app.database import SessionDep
from app.outbox.repository import OutboxRepository
from app.payments.repository import PaymentRepository
from app.payments.service import PaymentService


def get_payment_service(session: SessionDep) -> PaymentService:
    """Создать PaymentService с репозиториями на основе текущей сессии.

    Важно: оба Repository используют одну и ту же AsyncSession,
    что гарантирует атомарность Payment + Outbox в одной транзакции.
    """
    payment_repo = PaymentRepository(session)
    outbox_repo = OutboxRepository(session)
    return PaymentService(payment_repo, outbox_repo)


PaymentServiceDep = Annotated[PaymentService, Depends(get_payment_service)]
