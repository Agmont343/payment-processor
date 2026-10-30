from datetime import datetime
from decimal import Decimal
from typing import Any, Self
from uuid import UUID

from pydantic import BaseModel, ConfigDict

from app.payments.dto import PaymentDetailDTO
from app.payments.models import Currency, PaymentStatus


class PaymentResponse(BaseModel):
    model_config = ConfigDict(from_attributes=True)

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
    def from_dto(cls, dto: PaymentDetailDTO) -> Self:
        return cls(
            id=dto.id,
            amount=dto.amount,
            currency=dto.currency,
            description=dto.description,
            metadata=dto.metadata,
            status=dto.status,
            webhook_url=dto.webhook_url,
            created_at=dto.created_at,
            processed_at=dto.processed_at,
        )
