from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Self
from uuid import UUID

from app.payments.models import Currency, Payment, PaymentStatus


@dataclass(slots=True)
class PaymentCreateDTO:
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    webhook_url: str
    idempotency_key: str


@dataclass(slots=True)
class PaymentDetailDTO:
    id: UUID
    amount: Decimal
    currency: Currency
    description: str
    metadata: dict[str, Any]
    status: PaymentStatus
    webhook_url: str
    created_at: datetime
    processed_at: datetime | None

    @classmethod
    def from_model(cls, payment: Payment) -> Self:
        return cls(
            id=payment.id,
            amount=payment.amount,
            currency=payment.currency,
            description=payment.description,
            metadata=payment.metadata_json,
            status=payment.status,
            webhook_url=payment.webhook_url,
            created_at=payment.created_at,
            processed_at=payment.processed_at,
        )
