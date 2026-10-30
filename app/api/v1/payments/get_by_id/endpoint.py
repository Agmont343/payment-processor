from uuid import UUID

from fastapi import APIRouter, status

from app.api.v1.payments.get_by_id.response import GetPaymentResponse
from app.database import SessionDep
from app.payments.services import get_payment_by_id

router = APIRouter()


@router.get(
    "/payments/{payment_id}",
    response_model=GetPaymentResponse,
    status_code=status.HTTP_200_OK,
    summary="Get a payment by id",
)
async def get_payment_endpoint(
    session: SessionDep,
    payment_id: UUID,
) -> GetPaymentResponse:
    payment = await get_payment_by_id(session=session, payment_id=payment_id)
    return GetPaymentResponse.from_dto(payment)
