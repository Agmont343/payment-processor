from http import HTTPStatus

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from app.payments.exceptions import IdempotencyConflict, PaymentNotFound


def register_exception_handlers(app: FastAPI) -> None:
    @app.exception_handler(PaymentNotFound)
    async def payment_not_found_handler(
        _request: Request,
        exc: PaymentNotFound,
    ) -> JSONResponse:
        return JSONResponse(
            status_code=HTTPStatus.NOT_FOUND,
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
            status_code=HTTPStatus.CONFLICT,
            content={"detail": str(exc)},
        )
