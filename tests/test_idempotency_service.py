from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy.exc import IntegrityError

from app.outbox.models import OUTBOX_EVENT_PAYMENTS_NEW, OutboxEvent
from app.payments.dto import PaymentCreateDTO
from app.payments.exceptions import IdempotencyConflict, PaymentNotFound
from app.payments.hashing import canonical_request_hash
from app.payments.models import Currency, Payment, PaymentStatus
from app.payments.service import PaymentService


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


def _hash_of(dto: PaymentCreateDTO) -> str:
    return canonical_request_hash(
        amount=dto.amount,
        currency=dto.currency,
        description=dto.description,
        metadata=dto.metadata,
        webhook_url=dto.webhook_url,
    )


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


class FakePaymentRepository:
    """Реализует методы PaymentRepositoryProtocol, используемые PaymentService.

    insert_or_get_existing имитирует семантику INSERT ... ON CONFLICT:
    при конфликте возвращает существующий платёж, иначе — вставленный.
    """

    def __init__(
        self,
        *,
        existing: Payment | None = None,
        insert_error: Exception | None = None,
    ) -> None:
        self._existing = existing
        self._insert_error = insert_error
        self.inserted: list[Payment] = []

    async def get_by_id(self, payment_id: UUID) -> Payment | None:
        if self._existing is not None and self._existing.id == payment_id:
            return self._existing
        return None

    async def insert_or_get_existing(self, payment: Payment) -> tuple[Payment, bool]:
        if self._insert_error is not None:
            raise self._insert_error
        self.inserted.append(payment)
        if self._existing is not None:
            return self._existing, False
        return payment, True


class FakeOutboxRepository:
    """Собирает события, переданные в add()."""

    def __init__(self) -> None:
        self.added: list[OutboxEvent] = []

    def add(self, event: OutboxEvent) -> None:
        self.added.append(event)


async def test_create_payment_creates_payment_and_outbox_event_together() -> None:
    payment_repo = FakePaymentRepository()
    outbox_repo = FakeOutboxRepository()
    service = PaymentService(payment_repo, outbox_repo)
    dto = _dto()

    result, created = await service.create_payment(dto)

    assert created is True
    assert result.status == PaymentStatus.PENDING
    assert result.amount == Decimal("10.00")

    [payment] = payment_repo.inserted
    assert payment.id == result.id
    assert payment.idempotency_key == dto.idempotency_key
    assert payment.request_hash == _hash_of(dto)

    [event] = outbox_repo.added
    assert event.event_type == OUTBOX_EVENT_PAYMENTS_NEW
    assert event.aggregate_id == result.id
    assert event.payload == {
        "payment_id": str(result.id),
        "event_id": str(event.id),
    }


async def test_same_key_same_hash_returns_existing_without_outbox_event() -> None:
    dto = _dto()
    existing = _payment(request_hash=_hash_of(dto))
    payment_repo = FakePaymentRepository(existing=existing)
    outbox_repo = FakeOutboxRepository()
    service = PaymentService(payment_repo, outbox_repo)

    result, created = await service.create_payment(dto)

    assert created is False
    assert result.id == existing.id
    assert result.status == PaymentStatus.PENDING
    # Повтор не создаёт новое outbox-событие: дубликатов публикации не будет.
    assert outbox_repo.added == []


async def test_same_key_different_hash_raises_idempotency_conflict() -> None:
    dto = _dto(amount=Decimal("11.00"))
    # Существующий платёж создан другим запросом (amount=10.00).
    existing = _payment(request_hash=_hash_of(_dto()))
    payment_repo = FakePaymentRepository(existing=existing)
    outbox_repo = FakeOutboxRepository()
    service = PaymentService(payment_repo, outbox_repo)

    with pytest.raises(IdempotencyConflict) as exc_info:
        await service.create_payment(dto)

    assert exc_info.value.idempotency_key == dto.idempotency_key
    assert outbox_repo.added == []


async def test_unrelated_integrity_error_propagates_unchanged() -> None:
    """Посторонняя ошибка БД проходит сквозь сервис, а не маскируется
    под идемпотентный повтор или IdempotencyConflict."""
    payment_repo = FakePaymentRepository(
        insert_error=IntegrityError(
            "INSERT", {}, Exception("ck_payments_amount_positive")
        )
    )
    outbox_repo = FakeOutboxRepository()
    service = PaymentService(payment_repo, outbox_repo)

    with pytest.raises(IntegrityError):
        await service.create_payment(_dto())

    assert outbox_repo.added == []


async def test_create_payment_does_not_touch_broker() -> None:
    import inspect

    from app.payments import service as payment_service_module

    source = inspect.getsource(payment_service_module)
    assert "faststream" not in source
    assert "broker" not in source
    assert "publish" not in source


async def test_get_payment_by_id_not_found() -> None:
    service = PaymentService(FakePaymentRepository(), FakeOutboxRepository())
    payment_id = uuid4()

    with pytest.raises(PaymentNotFound) as exc_info:
        await service.get_payment_by_id(payment_id)

    assert exc_info.value.payment_id == payment_id
