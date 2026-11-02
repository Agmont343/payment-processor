from http import HTTPStatus
from typing import Annotated

from fastapi import APIRouter, Header

from app.api.dependencies import PaymentServiceDep
from app.api.v1.payments.create.request import CreatePaymentRequest
from app.api.v1.payments.create.response import PaymentResponse
from app.payments.dto import PaymentCreateDTO

router = APIRouter()


@router.post(
    "/payments",
    response_model=PaymentResponse,
    status_code=HTTPStatus.ACCEPTED,
    summary="Create a payment",
)
async def create_payment_endpoint(
    service: PaymentServiceDep,
    request: CreatePaymentRequest,
    idempotency_key: Annotated[
        str,
        Header(alias="Idempotency-Key", min_length=1, max_length=128),
    ],
) -> PaymentResponse:
    dto = PaymentCreateDTO(
        amount=request.amount,
        currency=request.currency,
        description=request.description,
        metadata=request.metadata,
        webhook_url=request.webhook_url,
        idempotency_key=idempotency_key,
    )
    payment, created = await service.create_payment(dto)
    return PaymentResponse.from_dto(payment)
