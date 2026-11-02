from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from uuid import UUID, uuid4

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.consumer.gateway import GatewayResult
from app.consumer.service import PaymentProcessingService
from app.payments.exceptions import PaymentNotFound
from app.payments.models import Payment, PaymentStatus
from app.payments.repository import PaymentRepository
from tests.helpers import FailingGateway, RecordingGateway, make_payment


@dataclass
class LockProbeGateway:
    """Успешный FOR UPDATE NOWAIT во время Gateway доказывает, что сервис
    не держит блокировку строки вне транзакций."""

    postgres: async_sessionmaker[AsyncSession]
    result_success: bool
    calls: list[UUID] = field(default_factory=list)
    nowait_acquired: list[bool] = field(default_factory=list)

    async def process(self, payment_id: UUID) -> GatewayResult:
        self.calls.append(payment_id)
        async with self.postgres() as session:
            locked = await session.get(
                Payment, payment_id, with_for_update={"nowait": True}
            )
            assert locked is not None
            self.nowait_acquired.append(True)
        return GatewayResult(success=self.result_success)


@dataclass
class RacingGateway:
    """Имитирует параллельного worker'а, успевающего закоммитить
    терминальный статус во время нашего Gateway."""

    postgres: async_sessionmaker[AsyncSession]
    status_to_set: PaymentStatus
    processed_at: datetime
    result_success: bool
    calls: list[UUID] = field(default_factory=list)
    nowait_acquired: list[bool] = field(default_factory=list)

    async def process(self, payment_id: UUID) -> GatewayResult:
        self.calls.append(payment_id)
        async with self.postgres() as session:
            locked = await session.get(
                Payment, payment_id, with_for_update={"nowait": True}
            )
            assert locked is not None
            self.nowait_acquired.append(True)
            locked.status = self.status_to_set
            locked.processed_at = self.processed_at
            await session.commit()
        return GatewayResult(success=self.result_success)


async def _insert(postgres: async_sessionmaker[AsyncSession], payment: Payment) -> UUID:
    async with postgres() as session:
        session.add(payment)
        await session.commit()
    return payment.id


@pytest.mark.parametrize(
    ("gateway_success", "expected_status"),
    [
        pytest.param(True, PaymentStatus.SUCCEEDED, id="gateway-success"),
        pytest.param(False, PaymentStatus.FAILED, id="gateway-business-failure"),
    ],
)
async def test_pending_payment_gateway_outcome_is_committed(
    postgres: async_sessionmaker[AsyncSession],
    gateway_success: bool,
    expected_status: PaymentStatus,
) -> None:
    payment_id = await _insert(postgres, make_payment("https://example.com/hook"))
    gateway = RecordingGateway(results=[gateway_success])

    result = await PaymentProcessingService(postgres, gateway).process_payment(
        payment_id
    )

    assert result.payment_id == payment_id
    assert result.status == expected_status
    assert result.gateway_called is True
    assert result.detail.webhook_url == "https://example.com/hook"
    assert result.detail.amount == Decimal("10.00")
    assert gateway.calls == [payment_id]
    async with postgres() as session:
        row = await session.get(Payment, payment_id)
        assert row is not None
        assert row.status == expected_status
        assert row.processed_at is not None


@pytest.mark.parametrize(
    "terminal_status",
    [
        pytest.param(PaymentStatus.SUCCEEDED, id="terminal-succeeded"),
        pytest.param(PaymentStatus.FAILED, id="terminal-failed"),
    ],
)
async def test_terminal_payment_does_not_call_gateway(
    postgres: async_sessionmaker[AsyncSession],
    terminal_status: PaymentStatus,
) -> None:
    processed_at = datetime(2026, 1, 1, tzinfo=UTC)
    payment = make_payment("https://example.com/hook", status=terminal_status)
    payment.processed_at = processed_at
    payment_id = await _insert(postgres, payment)
    gateway = RecordingGateway(results=[True])

    result = await PaymentProcessingService(postgres, gateway).process_payment(
        payment_id
    )

    assert result.payment_id == payment_id
    assert result.status == terminal_status
    assert result.gateway_called is False
    assert gateway.calls == []
    async with postgres() as session:
        row = await session.get(Payment, payment_id)
        assert row is not None
        assert row.processed_at == processed_at


async def test_redelivery_after_terminal_status_skips_gateway(
    postgres: async_sessionmaker[AsyncSession],
) -> None:
    payment_id = await _insert(postgres, make_payment("https://example.com/hook"))
    gateway = RecordingGateway(results=[True, True])
    service = PaymentProcessingService(postgres, gateway)

    first = await service.process_payment(payment_id)
    second = await service.process_payment(payment_id)

    assert first.gateway_called is True
    assert second.payment_id == payment_id
    assert second.status == PaymentStatus.SUCCEEDED
    assert second.gateway_called is False
    assert gateway.calls == [payment_id]


async def test_missing_payment_raises_for_dlq_path(
    postgres: async_sessionmaker[AsyncSession],
) -> None:
    gateway = RecordingGateway(results=[True])

    with pytest.raises(PaymentNotFound):
        await PaymentProcessingService(postgres, gateway).process_payment(uuid4())
    assert gateway.calls == []


async def test_gateway_exception_propagates_and_payment_stays_pending(
    postgres: async_sessionmaker[AsyncSession],
) -> None:
    payment_id = await _insert(postgres, make_payment("https://example.com/hook"))
    gateway = FailingGateway()

    with pytest.raises(RuntimeError, match="DB connection lost"):
        await PaymentProcessingService(postgres, gateway).process_payment(payment_id)

    assert gateway.calls == [payment_id]
    async with postgres() as session:
        row = await session.get(Payment, payment_id)
        assert row is not None
        assert row.status == PaymentStatus.PENDING
        assert row.processed_at is None


async def test_gateway_runs_outside_db_transaction(
    postgres: async_sessionmaker[AsyncSession],
) -> None:
    """Во время Gateway строка платежа не заблокирована сервисом:
    посторонняя сессия берёт FOR UPDATE NOWAIT без ошибки."""
    payment_id = await _insert(postgres, make_payment("https://example.com/hook"))
    gateway = LockProbeGateway(postgres, result_success=True)

    result = await PaymentProcessingService(postgres, gateway).process_payment(
        payment_id
    )

    assert result.gateway_called is True
    assert result.status == PaymentStatus.SUCCEEDED
    assert gateway.nowait_acquired == [True]


async def test_status_rechecked_under_lock_before_update(
    postgres: async_sessionmaker[AsyncSession],
) -> None:
    """Другой worker уже закоммитил SUCCEEDED, поэтому FAILED-результат
    нашего Gateway не должен перезаписать этот статус."""
    payment_id = await _insert(postgres, make_payment("https://example.com/hook"))
    other_worker_time = datetime(2026, 1, 2, 3, 4, tzinfo=UTC)
    gateway = RacingGateway(
        postgres,
        status_to_set=PaymentStatus.SUCCEEDED,
        processed_at=other_worker_time,
        result_success=False,
    )

    result = await PaymentProcessingService(postgres, gateway).process_payment(
        payment_id
    )

    assert gateway.nowait_acquired == [True]
    assert result.gateway_called is True
    assert result.status == PaymentStatus.SUCCEEDED
    async with postgres() as session:
        row = await session.get(Payment, payment_id)
        assert row is not None
        assert row.status == PaymentStatus.SUCCEEDED
        assert row.processed_at == other_worker_time


class _RecordingSession:
    """Фиксирует kwargs, переданные в session.get()."""

    def __init__(self) -> None:
        self.get_kwargs: dict[str, object] | None = None

    async def get(self, _model: object, _pk: object, **kwargs: object) -> None:
        self.get_kwargs = kwargs
        return None


async def test_get_by_id_with_lock_uses_for_update() -> None:
    session = _RecordingSession()
    repository = PaymentRepository(session)  # type: ignore[arg-type]

    await repository.get_by_id_with_lock(uuid4())

    assert session.get_kwargs == {"with_for_update": True}
