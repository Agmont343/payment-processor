"""Общие фабрики тестовых данных для тестов consumer и платежей."""

from dataclasses import dataclass, field
from decimal import Decimal
from uuid import UUID, uuid4

from app.consumer.gateway import GatewayResult
from app.payments.models import Currency, Payment, PaymentStatus


def make_payment(
    webhook_url: str, *, status: PaymentStatus = PaymentStatus.PENDING
) -> Payment:
    """Платёж для интеграционных тестов обработки (по умолчанию PENDING).

    idempotency_key уникален; значения остальных полей в этих тестах
    не ассертятся.
    """
    return Payment(
        id=uuid4(),
        amount=Decimal("10.00"),
        currency=Currency.RUB,
        description="integration test payment",
        metadata_json={},
        status=status,
        idempotency_key=f"key-{uuid4()}",
        request_hash="hash",
        webhook_url=webhook_url,
    )


@dataclass
class RecordingGateway:
    """Gateway-заглушка: отдаёт результаты по очереди, фиксирует вызовы."""

    results: list[bool]
    calls: list[UUID] = field(default_factory=list)

    async def process(self, payment_id: UUID) -> GatewayResult:
        self.calls.append(payment_id)
        return GatewayResult(success=self.results.pop(0))


class FailingGateway:
    """Gateway, падающий с технической ошибкой до сохранения статуса."""

    def __init__(self) -> None:
        self.calls: list[UUID] = []

    async def process(self, payment_id: UUID) -> GatewayResult:
        self.calls.append(payment_id)
        raise RuntimeError("DB connection lost before status UPDATE")
