from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumer.gateway import GatewayResult
from app.consumer.service import process_payment_event
from app.payments.exceptions import PaymentNotFound
from app.payments.models import Currency, Payment, PaymentStatus


@dataclass
class FakeGateway:
    results: list[bool]
    calls: list[UUID] = field(default_factory=list)

    async def process(self, payment_id: UUID) -> GatewayResult:
        self.calls.append(payment_id)
        return GatewayResult(success=self.results.pop(0))


def _payment(status: PaymentStatus = PaymentStatus.PENDING) -> Payment:
    return Payment(
        id=uuid4(),
        amount=Decimal("10.00"),
        currency=Currency.RUB,
        description="Order",
        metadata_json={},
        status=status,
        idempotency_key=f"key-{uuid4()}",
        request_hash="hash",
        webhook_url="https://example.com/hook",
        created_at=datetime.now(UTC),
    )


async def _insert(postgres: async_sessionmaker[AsyncSession], payment: Payment) -> UUID:
    async with postgres() as session:
        session.add(payment)
        await session.commit()
    return payment.id


async def test_pending_payment_with_gateway_success_is_committed(
    postgres: async_sessionmaker[AsyncSession],
) -> None:
    payment_id = await _insert(postgres, _payment())
    gateway = FakeGateway(results=[True])

    result = await process_payment_event(postgres, gateway, payment_id)

    assert result.payment_id == payment_id
    assert result.status == PaymentStatus.SUCCEEDED
    assert result.gateway_called is True
    assert result.detail.webhook_url == "https://example.com/hook"
    assert result.detail.amount == Decimal("10.00")
    assert gateway.calls == [payment_id]
    async with postgres() as session:
        row = await session.get(Payment, payment_id)
        assert row is not None
        assert row.status == PaymentStatus.SUCCEEDED
        assert row.processed_at is not None


async def test_pending_payment_with_gateway_business_failure_is_terminal(
    postgres: async_sessionmaker[AsyncSession],
) -> None:
    payment_id = await _insert(postgres, _payment())
    gateway = FakeGateway(results=[False])

    result = await process_payment_event(postgres, gateway, payment_id)

    assert result.payment_id == payment_id
    assert result.status == PaymentStatus.FAILED
    assert result.gateway_called is True
    async with postgres() as session:
        row = await session.get(Payment, payment_id)
        assert row is not None
        assert row.status == PaymentStatus.FAILED
        assert row.processed_at is not None


async def test_terminal_payment_does_not_call_gateway(
    postgres: async_sessionmaker[AsyncSession],
) -> None:
    processed_at = datetime(2026, 1, 1, tzinfo=UTC)
    payment = _payment(status=PaymentStatus.SUCCEEDED)
    payment.processed_at = processed_at
    payment_id = await _insert(postgres, payment)
    gateway = FakeGateway(results=[True])

    result = await process_payment_event(postgres, gateway, payment_id)

    assert result.payment_id == payment_id
    assert result.status == PaymentStatus.SUCCEEDED
    assert result.gateway_called is False
    assert gateway.calls == []
    async with postgres() as session:
        row = await session.get(Payment, payment_id)
        assert row is not None
        assert row.processed_at == processed_at


async def test_redelivery_after_terminal_status_skips_gateway(
    postgres: async_sessionmaker[AsyncSession],
) -> None:
    payment_id = await _insert(postgres, _payment())
    gateway = FakeGateway(results=[True, True])

    first = await process_payment_event(postgres, gateway, payment_id)
    second = await process_payment_event(postgres, gateway, payment_id)

    assert first.gateway_called is True
    assert second.payment_id == payment_id
    assert second.status == PaymentStatus.SUCCEEDED
    assert second.gateway_called is False
    assert gateway.calls == [payment_id]


async def test_missing_payment_raises_for_dlq_path(
    postgres: async_sessionmaker[AsyncSession],
) -> None:
    gateway = FakeGateway(results=[True])

    with pytest.raises(PaymentNotFound):
        await process_payment_event(postgres, gateway, uuid4())
    assert gateway.calls == []
