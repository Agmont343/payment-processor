from decimal import Decimal
from typing import Any

from pydantic import BaseModel, Field, field_validator

from app.common.webhook_url import InvalidWebhookUrl, validate_webhook_url
from app.config import settings
from app.payments.models import Currency

MAX_AMOUNT = Decimal("10") ** 16


class CreatePaymentRequest(BaseModel):
    amount: Decimal = Field(..., gt=0)
    currency: Currency
    description: str = Field(..., min_length=1, max_length=4096)
    metadata: dict[str, Any] = Field(default_factory=dict)
    webhook_url: str = Field(..., min_length=1)

    @field_validator("amount")
    @classmethod
    def amount_must_have_two_places(cls, value: Decimal) -> Decimal:
        exponent = value.as_tuple().exponent
        if isinstance(exponent, int) and exponent < -2:
            raise ValueError("amount must have at most 2 decimal places")
        quantized = value.quantize(Decimal("0.01"))
        if quantized <= 0:
            raise ValueError("amount must be greater than 0")
        if quantized >= MAX_AMOUNT:
            raise ValueError("amount is too large")
        return quantized

    @field_validator("webhook_url")
    @classmethod
    def webhook_url_must_be_public(cls, value: str) -> str:
        try:
            return validate_webhook_url(
                value,
                allow_private=settings.WEBHOOK_ALLOW_PRIVATE,
            )
        except InvalidWebhookUrl as exc:
            raise ValueError(str(exc)) from exc
