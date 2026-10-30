import logging
from collections.abc import AsyncIterator
from urllib.parse import quote

import httpx
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.common.base import Base
from app.config import settings
from app.database import get_db
from app.main import app

SessionFactory = async_sessionmaker[AsyncSession]

TEST_DATABASE_NAME = "payments_test"

# Очереди тестового vhost, очищаемые в teardown фикстуры rabbit_test_url.
TEST_VHOST_QUEUE_NAMES = (
    "payments.new",
    "payments.retry.1",
    "payments.retry.2",
    "payments.new.dlq",
)

_logger = logging.getLogger(__name__)


def _require_test_database_url() -> str:
    """Защита dev-БД: тесты работают только с payments_test.

    TRUNCATE и прочие очистки выполняются исключительно после этой
    проверки; DB_URL как fallback не используется.
    """
    url = make_url(str(settings.TEST_DB_URL))
    database = url.database or ""
    if database != TEST_DATABASE_NAME:
        pytest.exit(
            f"Refusing to run tests: TEST_DB_URL must point to the "
            f"'{TEST_DATABASE_NAME}' database, got '{database or '<none>'}'. "
            "The dev database must never be cleaned by tests.",
            returncode=1,
        )
    dev_database = make_url(str(settings.DB_URL)).database or ""
    if database == dev_database:
        pytest.exit(
            "Refusing to run tests: TEST_DB_URL points to the same database "
            "as DB_URL. Tests require a separate database.",
            returncode=1,
        )
    return str(settings.TEST_DB_URL)


def _require_test_vhost() -> str:
    """Защита очередей consumer'а: тесты работают только в своём vhost."""
    vhost = (make_url(str(settings.TEST_RABBITMQ_URL)).database or "").strip()
    if vhost in {"", "/", "%2f", "%2F"}:
        pytest.exit(
            "Refusing to run tests: TEST_RABBITMQ_URL must point to a "
            f"dedicated non-root vhost, got '{vhost or '<root>'}'. The root "
            "vhost serves the running consumer.",
            returncode=1,
        )
    return vhost


async def _management_api(rabbit_url: str) -> tuple[str, tuple[str, str], str]:
    """База management API, креды и экранированное имя тестового vhost."""
    url = make_url(rabbit_url)
    host = url.host or "localhost"
    port = (url.port or 5672) + 10000
    vhost = quote(_require_test_vhost(), safe="")
    auth = (url.username or "guest", url.password or "guest")
    return f"http://{host}:{port}/api", auth, vhost


async def _ensure_vhost(rabbit_url: str) -> None:
    """Идемпотентно создать тестовый vhost через management API.

    Операция добавляющая: существующие vhost, очереди и сообщения не
    затрагиваются. Без неё тестовый брокер получит ACCESS_REFUSED.
    """
    base, auth, vhost = await _management_api(rabbit_url)
    user = auth[0]
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.put(f"{base}/vhosts/{vhost}", auth=auth)
            if response.status_code not in (201, 204):
                pytest.exit(
                    f"RabbitMQ management API refused vhost setup: HTTP "
                    f"{response.status_code} for PUT {base}/vhosts/{vhost}. "
                    f"Create it manually: rabbitmqctl add_vhost {vhost} && "
                    f"rabbitmqctl set_permissions -p {vhost} {user} '.*' '.*' '.*'",
                    returncode=1,
                )
            await client.put(
                f"{base}/permissions/{vhost}/{user}",
                auth=auth,
                json={"configure": ".*", "write": ".*", "read": ".*"},
            )
    except httpx.HTTPError as exc:
        pytest.exit(
            f"Cannot reach RabbitMQ management API at {base} to ensure "
            f"test vhost '{vhost}': {exc}. Create it manually: "
            f"rabbitmqctl add_vhost {vhost} && rabbitmqctl set_permissions "
            f"-p {vhost} {user} '.*' '.*' '.*'",
            returncode=1,
        )


async def _purge_test_vhost_queues(rabbit_url: str) -> None:
    """Очистить очереди исключительно тестового vhost (teardown тестов).

    Root-vhost с очередями работающего consumer'а не затрагивается:
    имя vhost берётся только из TEST_RABBITMQ_URL и проверено guard'ом.
    """
    base, auth, vhost = await _management_api(rabbit_url)
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            for queue_name in TEST_VHOST_QUEUE_NAMES:
                response = await client.delete(
                    f"{base}/queues/{vhost}/{quote(queue_name, safe='')}/contents",
                    auth=auth,
                )
                if response.status_code not in (200, 204, 404):
                    _logger.warning(
                        "Purge of %s in test vhost returned HTTP %s",
                        queue_name,
                        response.status_code,
                    )
    except httpx.HTTPError as exc:
        _logger.warning("Cannot purge test vhost queues: %s", exc)


@pytest.fixture
def api_headers() -> dict[str, str]:
    return {"X-API-Key": settings.API_KEY}


@pytest.fixture
async def client() -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.fixture
def test_db_url() -> str:
    return _require_test_database_url()


@pytest.fixture
async def rabbit_test_url() -> AsyncIterator[str]:
    url = str(settings.TEST_RABBITMQ_URL)
    await _ensure_vhost(url)
    yield url
    await _purge_test_vhost_queues(url)


@pytest.fixture
async def postgres() -> AsyncIterator[SessionFactory]:
    engine = create_async_engine(_require_test_database_url(), poolclass=NullPool)
    factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
        autoflush=False,
        autocommit=False,
    )

    async def override_get_db() -> AsyncIterator[AsyncSession]:
        async with factory() as session:
            try:
                yield session
                await session.commit()
            except Exception:
                await session.rollback()
                raise

    app.dependency_overrides[get_db] = override_get_db

    try:
        # Схема тестовой БД создаётся из метаданных моделей; dev-БД
        # не затрагивается.
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.create_all)
    except Exception as exc:
        app.dependency_overrides.pop(get_db, None)
        await engine.dispose()
        pytest.skip(f"PostgreSQL is not available: {exc}")

    async with factory() as session:
        await session.execute(text("TRUNCATE payments, outbox CASCADE"))
        await session.commit()

    yield factory

    async with factory() as session:
        await session.execute(text("TRUNCATE payments, outbox CASCADE"))
        await session.commit()
    app.dependency_overrides.pop(get_db, None)
    await engine.dispose()
