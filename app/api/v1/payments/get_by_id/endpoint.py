from http import HTTPStatus
from uuid import UUID

from fastapi import APIRouter

from app.api.dependencies import PaymentServiceDep
from app.api.v1.payments.get_by_id.response import GetPaymentResponse

router = APIRouter()


@router.get(
    "/payments/{payment_id}",
    response_model=GetPaymentResponse,
    status_code=HTTPStatus.OK,
    summary="Get a payment by id",
)
async def get_payment_endpoint(
    service: PaymentServiceDep,
    payment_id: UUID,
) -> GetPaymentResponse:
    payment = await service.get_payment_by_id(payment_id=payment_id)
    return GetPaymentResponse.from_dto(payment)
