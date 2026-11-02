from typing import Protocol
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.payments.models import Payment


class PaymentRepositoryProtocol(Protocol):
    """Интерфейс для PaymentRepository."""

    async def get_by_id(self, payment_id: UUID) -> Payment | None: ...
    async def get_by_id_with_lock(self, payment_id: UUID) -> Payment | None: ...
    async def get_by_idempotency_key(self, key: str) -> Payment | None: ...
    async def insert_or_get_existing(
        self, payment: Payment
    ) -> tuple[Payment, bool]: ...
    def add(self, payment: Payment) -> None: ...


class PaymentRepository:
    """Репозиторий для работы с Payment через SQLAlchemy ORM."""

    def __init__(self, session: AsyncSession) -> None:
        self._session = session

    async def get_by_id(self, payment_id: UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id)

    async def get_by_id_with_lock(self, payment_id: UUID) -> Payment | None:
        return await self._session.get(Payment, payment_id, with_for_update=True)

    async def get_by_idempotency_key(self, key: str) -> Payment | None:
        """Получить payment по idempotency key без блокировки."""
        return await self._session.scalar(
            select(Payment).where(Payment.idempotency_key == key)
        )

    async def insert_or_get_existing(self, payment: Payment) -> tuple[Payment, bool]:
        """Вставить payment или получить существующий при конфликте idempotency_key.

        Использует PostgreSQL INSERT ... ON CONFLICT DO NOTHING для атомарной
        обработки конкурентных вставок.

        Возвращает:
        - (payment, True): payment был создан (INSERT успешен)
        - (existing_payment, False): payment с таким idempotency_key уже существует

        Конкурентные INSERT с одним idempotency_key:
        - Первый INSERT создаёт строку и возвращает её
        - Второй INSERT ничего не возвращает (conflict) → делаем SELECT
        """
        # Подготовка INSERT с ON CONFLICT
        stmt = (
            pg_insert(Payment)
            .values(
                id=payment.id,
                amount=payment.amount,
                currency=payment.currency,
                description=payment.description,
                metadata_json=payment.metadata_json,
                status=payment.status,
                idempotency_key=payment.idempotency_key,
                request_hash=payment.request_hash,
                webhook_url=payment.webhook_url,
            )
            .on_conflict_do_nothing(
                index_elements=["idempotency_key"]  # Конкретный constraint target
            )
            .returning(Payment)
        )

        result = await self._session.execute(stmt)
        inserted = result.scalar_one_or_none()

        if inserted is not None:
            # INSERT успешен → payment создан
            return inserted, True

        # Conflict: payment с таким idempotency_key уже существует
        # Получаем существующий payment отдельным SELECT
        existing = await self.get_by_idempotency_key(payment.idempotency_key)

        if existing is None:
            # Крайне редкий случай: payment был удалён между INSERT и SELECT
            raise RuntimeError(
                f"Payment with idempotency_key={payment.idempotency_key} "
                "vanished after conflict"
            )

        return existing, False

    def add(self, payment: Payment) -> None:
        """Добавить payment в сессию (для non-idempotent scenarios).

        Для idempotent создания используйте insert_or_get_existing().
        Фактическая запись в БД произойдёт при commit транзакции.
        """
        self._session.add(payment)
