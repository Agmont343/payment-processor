from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette import status

from app.payments.exceptions import IdempotencyConflict, PaymentNotFound


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(PaymentNotFound)
    async def payment_not_found_handler(
        _request: Request,
        exc: PaymentNotFound,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_404_NOT_FOUND,
            content={
                "detail": str(exc),
                "payment_id": str(exc.payment_id),
            },
        )

    @app.exception_handler(IdempotencyConflict)
    async def idempotency_conflict_handler(
        _request: Request,
        exc: IdempotencyConflict,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=status.HTTP_409_CONFLICT,
            content={"detail": str(exc)},
        )
