from typing import Annotated

from fastapi import APIRouter, Header, status

from app.api.v1.payments.create.request import CreatePaymentRequest
from app.api.v1.payments.create.response import PaymentResponse
from app.database import SessionDep
from app.payments.dto import PaymentCreateDTO
from app.payments.services import create_payment

router = APIRouter()


@router.post(
    "/payments",
    response_model=PaymentResponse,
    status_code=status.HTTP_202_ACCEPTED,
    summary="Create a payment",
)
async def create_payment_endpoint(
    session: SessionDep,
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
    payment = await create_payment(session=session, data=dto)
    return PaymentResponse.from_dto(payment)
