from datetime import UTC, datetime
from decimal import Decimal
from uuid import uuid4

import pytest
from sqlalchemy.exc import IntegrityError

from app.payments.dto import PaymentCreateDTO
from app.payments.exceptions import IdempotencyConflict, PaymentNotFound
from app.payments.hashing import canonical_request_hash
from app.payments.models import Currency, Payment, PaymentStatus
from app.payments.services import create_payment, get_payment_by_id


def _dto(**overrides: object) -> PaymentCreateDTO:
    payload = {
        "amount": Decimal("10.00"),
        "currency": Currency.RUB,
        "description": "Order",
        "metadata": {"a": 1},
        "webhook_url": "https://example.com/hook",
        "idempotency_key": "same-key",
    }
    payload.update(overrides)
    return PaymentCreateDTO(**payload)  # type: ignore[arg-type]


def _payment(*, request_hash: str, key: str = "same-key") -> Payment:
    return Payment(
        id=uuid4(),
        amount=Decimal("10.00"),
        currency=Currency.RUB,
        description="Order",
        metadata_json={"a": 1},
        status=PaymentStatus.PENDING,
        idempotency_key=key,
        request_hash=request_hash,
        webhook_url="https://example.com/hook",
        created_at=datetime.now(UTC),
    )


class FakeSession:
    def __init__(
        self, existing: Payment | None, *, flush_error: Exception | None
    ) -> None:
        self.existing = existing
        self.flush_error = flush_error
        self.added: list[object] = []
        self.rollback_calls = 0

    def add(self, obj: object) -> None:
        self.added.append(obj)

    async def flush(self) -> None:
        if self.flush_error is not None:
            raise self.flush_error

    async def rollback(self) -> None:
        self.rollback_calls += 1

    async def scalar(self, _statement: object) -> Payment | None:
        if self.rollback_calls == 0:
            raise AssertionError(
                "SELECT after IntegrityError must run in a new transaction"
            )
        return self.existing

    async def get(self, _model: object, pk: object) -> Payment | None:
        if self.existing is not None and self.existing.id == pk:
            return self.existing
        return None


async def test_integrity_error_same_hash_returns_existing_after_rollback() -> None:
    dto = _dto()
    existing = _payment(
        request_hash=canonical_request_hash(
            amount=dto.amount,
            currency=dto.currency,
            description=dto.description,
            metadata=dto.metadata,
            webhook_url=dto.webhook_url,
        )
    )
    session = FakeSession(
        existing,
        flush_error=IntegrityError(
            "INSERT", {}, Exception("uq_payments_idempotency_key")
        ),
    )

    result = await create_payment(session, dto)

    assert session.rollback_calls == 1
    assert result.id == existing.id
    assert len(session.added) == 2


async def test_integrity_error_different_hash_raises_conflict_after_rollback() -> None:
    dto = _dto(amount=Decimal("11.00"))
    existing = _payment(request_hash="different-hash")
    session = FakeSession(
        existing,
        flush_error=IntegrityError(
            "INSERT", {}, Exception("uq_payments_idempotency_key")
        ),
    )

    with pytest.raises(IdempotencyConflict):
        await create_payment(session, dto)

    assert session.rollback_calls == 1


async def test_unrelated_integrity_error_is_reraised_after_rollback() -> None:
    session = FakeSession(
        None,
        flush_error=IntegrityError(
            "INSERT", {}, Exception("ck_payments_amount_positive")
        ),
    )

    with pytest.raises(IntegrityError):
        await create_payment(session, _dto())

    assert session.rollback_calls == 1


async def test_create_payment_does_not_touch_broker() -> None:
    import inspect

    from app.payments import services

    source = inspect.getsource(services)
    assert "faststream" not in source
    assert "broker" not in source
    assert "publish" not in source


async def test_get_payment_by_id_not_found() -> None:
    session = FakeSession(None, flush_error=None)
    with pytest.raises(PaymentNotFound):
        await get_payment_by_id(session, uuid4())
