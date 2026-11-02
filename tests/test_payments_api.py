import asyncio
from decimal import Decimal
from http import HTTPStatus
from uuid import uuid4

import pytest
from httpx import AsyncClient
from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

import app.payments.service as payment_service_module
from app.outbox.models import OUTBOX_EVENT_PAYMENTS_NEW, OutboxEvent
from app.outbox.repository import OutboxRepository
from app.payments.dto import PaymentCreateDTO
from app.payments.models import Currency, Payment
from app.payments.repository import PaymentRepository
from app.payments.service import PaymentService


def _body(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "amount": "10.50",
        "currency": "RUB",
        "description": "Order 1",
        "metadata": {"order_id": "1"},
        "webhook_url": "https://example.com/webhook",
    }
    payload.update(overrides)
    return payload


def _headers(api_headers: dict[str, str], idempotency_key: str) -> dict[str, str]:
    return {**api_headers, "Idempotency-Key": idempotency_key}


async def _counts(factory: async_sessionmaker[AsyncSession]) -> tuple[int, int]:
    async with factory() as session:
        payments = await session.scalar(select(func.count()).select_from(Payment))
        events = await session.scalar(select(func.count()).select_from(OutboxEvent))
    return int(payments or 0), int(events or 0)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        pytest.param("amount", "10.123", id="amount-3-decimal-places"),
        pytest.param("amount", "0", id="amount-zero"),
        pytest.param("amount", "-5.00", id="amount-negative"),
        pytest.param("amount", "10000000000000000", id="amount-too-large"),
        pytest.param("currency", "GBP", id="currency-unsupported"),
        pytest.param("webhook_url", "http://127.0.0.1/hook", id="webhook-private-url"),
        pytest.param("description", "", id="description-empty"),
    ],
)
async def test_create_payment_rejects_invalid_body(
    client: AsyncClient,
    api_headers: dict[str, str],
    field: str,
    value: object,
) -> None:
    """Валидация тела запроса отклоняет недопустимые поля с HTTP 422.

    Границы amount соответствуют CreatePaymentRequest: gt=0, максимум
    2 знака после запятой, максимум MAX_AMOUNT.
    """
    response = await client.post(
        "/api/v1/payments",
        json=_body(**{field: value}),
        headers=_headers(api_headers, "invalid-body-key"),
    )
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY


async def test_missing_idempotency_key_returns_422(
    client: AsyncClient,
    api_headers: dict[str, str],
) -> None:
    """Отсутствие заголовка Idempotency-Key — отдельный механизм валидации
    (ограничение Header, а не тело запроса)."""
    response = await client.post(
        "/api/v1/payments",
        json=_body(),
        headers=api_headers,
    )
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY


async def test_idempotency_key_128_chars_accepted(
    client: AsyncClient,
    api_headers: dict[str, str],
    postgres: async_sessionmaker[AsyncSession],
) -> None:
    """Верхняя граница длины заголовка (max_length=128) допускается."""
    created = await client.post(
        "/api/v1/payments",
        json=_body(),
        headers=_headers(api_headers, "k" * 128),
    )
    assert created.status_code == HTTPStatus.ACCEPTED
    assert created.json()["status"] == "pending"


async def test_idempotency_key_129_chars_rejected(
    client: AsyncClient,
    api_headers: dict[str, str],
) -> None:
    response = await client.post(
        "/api/v1/payments",
        json=_body(),
        headers=_headers(api_headers, "k" * 129),
    )
    assert response.status_code == HTTPStatus.UNPROCESSABLE_ENTITY


async def test_create_get_and_replay_same_body(
    client: AsyncClient,
    api_headers: dict[str, str],
    postgres: async_sessionmaker[AsyncSession],
) -> None:
    key = uuid4().hex
    created = await client.post(
        "/api/v1/payments",
        json=_body(),
        headers=_headers(api_headers, key),
    )
    assert created.status_code == HTTPStatus.ACCEPTED
    payload = created.json()
    assert payload["status"] == "pending"
    assert payload["amount"] == "10.50"
    assert payload["metadata"] == {"order_id": "1"}
    assert payload["processed_at"] is None

    replay = await client.post(
        "/api/v1/payments",
        json=_body(),
        headers=_headers(api_headers, key),
    )
    assert replay.status_code == HTTPStatus.ACCEPTED
    assert replay.json()["id"] == payload["id"]

    fetched = await client.get(
        f"/api/v1/payments/{payload['id']}",
        headers=api_headers,
    )
    assert fetched.status_code == HTTPStatus.OK
    assert fetched.json()["id"] == payload["id"]

    payments, events = await _counts(postgres)
    assert payments == 1
    assert events == 1


async def test_same_key_different_body_returns_409(
    client: AsyncClient,
    api_headers: dict[str, str],
    postgres: async_sessionmaker[AsyncSession],
) -> None:
    key = uuid4().hex
    first = await client.post(
        "/api/v1/payments",
        json=_body(),
        headers=_headers(api_headers, key),
    )
    assert first.status_code == HTTPStatus.ACCEPTED

    second = await client.post(
        "/api/v1/payments",
        json=_body(amount="11.00"),
        headers=_headers(api_headers, key),
    )
    assert second.status_code == HTTPStatus.CONFLICT
    payments, events = await _counts(postgres)
    assert payments == 1
    assert events == 1


async def test_get_unknown_payment_returns_404(
    client: AsyncClient,
    api_headers: dict[str, str],
    postgres: async_sessionmaker[AsyncSession],
) -> None:
    response = await client.get(
        f"/api/v1/payments/{uuid4()}",
        headers=api_headers,
    )
    assert response.status_code == HTTPStatus.NOT_FOUND


async def test_concurrent_same_key_same_body_creates_one_payment_and_outbox(
    client: AsyncClient,
    api_headers: dict[str, str],
    postgres: async_sessionmaker[AsyncSession],
) -> None:
    key = uuid4().hex
    headers = _headers(api_headers, key)
    first, second = await asyncio.gather(
        client.post("/api/v1/payments", json=_body(), headers=headers),
        client.post("/api/v1/payments", json=_body(), headers=headers),
    )
    assert first.status_code == HTTPStatus.ACCEPTED
    assert second.status_code == HTTPStatus.ACCEPTED
    assert first.json()["id"] == second.json()["id"]
    payments, events = await _counts(postgres)
    assert payments == 1
    assert events == 1


async def test_concurrent_same_key_different_body_returns_conflict(
    client: AsyncClient,
    api_headers: dict[str, str],
    postgres: async_sessionmaker[AsyncSession],
) -> None:
    key = uuid4().hex
    headers = _headers(api_headers, key)
    first, second = await asyncio.gather(
        client.post("/api/v1/payments", json=_body(amount="10.50"), headers=headers),
        client.post("/api/v1/payments", json=_body(amount="11.00"), headers=headers),
    )
    codes = {first.status_code, second.status_code}
    assert codes == {HTTPStatus.ACCEPTED, HTTPStatus.CONFLICT}
    winner = first if first.status_code == HTTPStatus.ACCEPTED else second
    loser = second if first.status_code == HTTPStatus.ACCEPTED else first
    assert winner.json()["id"]
    assert loser.status_code == HTTPStatus.CONFLICT
    payments, events = await _counts(postgres)
    assert payments == 1
    assert events == 1


async def test_outbox_insert_failure_rolls_back_payment(
    postgres: async_sessionmaker[AsyncSession],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """PK-конфликт в outbox при commit → IntegrityError, платёж откатывается:
    Payment и OutboxEvent атомарны."""
    payment_id = uuid4()
    event_id = uuid4()
    reserved_ids = iter((payment_id, event_id))
    monkeypatch.setattr(payment_service_module, "uuid4", lambda: next(reserved_ids))

    # Предустановленное событие с тем же PK, который сгенерирует сервис.
    async with postgres() as session:
        session.add(
            OutboxEvent(
                id=event_id,
                event_type=OUTBOX_EVENT_PAYMENTS_NEW,
                aggregate_id=payment_id,
                payload={"payment_id": str(payment_id), "event_id": str(event_id)},
            )
        )
        await session.commit()

    dto = PaymentCreateDTO(
        amount=Decimal("10.00"),
        currency=Currency.RUB,
        description="atomic-outbox-failure",
        metadata={},
        webhook_url="https://example.com/hook",
        idempotency_key=uuid4().hex,
    )
    async with postgres() as session:
        service = PaymentService(PaymentRepository(session), OutboxRepository(session))
        with pytest.raises(IntegrityError) as exc_info:
            _, created = await service.create_payment(dto)
            assert created is True
            await session.commit()

    # Ошибка не связана с идемпотентностью платежа.
    assert "uq_payments_idempotency_key" not in str(exc_info.value)
    payments, events = await _counts(postgres)
    assert payments == 0
    assert events == 1
