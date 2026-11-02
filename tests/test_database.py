"""Тесты transaction_scope на реальной PostgreSQL."""

import pytest
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from app.database import transaction_scope
from app.payments.models import Payment
from tests.helpers import make_payment


async def test_transaction_scope_commits_on_success(
    postgres: async_sessionmaker[AsyncSession],
) -> None:
    payment = make_payment("https://example.com/hook")

    async with transaction_scope(postgres) as session:
        session.add(payment)

    async with postgres() as session:
        assert await session.get(Payment, payment.id) is not None


async def test_transaction_scope_rolls_back_on_exception(
    postgres: async_sessionmaker[AsyncSession],
) -> None:
    payment = make_payment("https://example.com/hook")

    with pytest.raises(RuntimeError, match="processing failed"):
        async with transaction_scope(postgres) as session:
            session.add(payment)
            raise RuntimeError("processing failed")

    async with postgres() as session:
        assert await session.get(Payment, payment.id) is None
