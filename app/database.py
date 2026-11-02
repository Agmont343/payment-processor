from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine

from app.config import settings

engine = create_async_engine(
    str(settings.DB_URL),
    echo=settings.DB_ECHO,
)

SessionFactory = async_sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False,
    autoflush=False,
    autocommit=False,
)


@asynccontextmanager
async def transaction_scope(
    session_factory: async_sessionmaker[AsyncSession],
) -> AsyncGenerator[AsyncSession, None]:
    """Управление транзакциями: commit при успехе, rollback при ошибке."""
    async with session_factory() as session:
        async with session.begin():
            yield session


async def get_db() -> AsyncGenerator[AsyncSession, None]:
    """FastAPI dependency для получения транзакционной сессии."""
    async with transaction_scope(SessionFactory) as session:
        yield session


SessionDep = Annotated[
    AsyncSession,
    Depends(get_db, scope="function"),
]
