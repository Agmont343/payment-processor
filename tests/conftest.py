import logging
from collections.abc import AsyncIterator
from http import HTTPStatus
from urllib.parse import quote

import httpx
import pytest
from httpx import ASGITransport, AsyncClient
from sqlalchemy import text
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from sqlalchemy.pool import NullPool

from app.common.base import Base
from app.config import TestSettings
from app.database import get_db
from app.main import app

SessionFactory = async_sessionmaker[AsyncSession]

TEST_DATABASE_NAME = "payments_test"
ALLOWED_TEST_HOSTS = {"localhost", "127.0.0.1", "::1"}

# Очереди тестового vhost, очищаемые в teardown фикстуры rabbit_test_url.
TEST_VHOST_QUEUE_NAMES = (
    "payments.new",
    "payments.retry.1",
    "payments.retry.2",
    "payments.new.dlq",
)

_logger = logging.getLogger(__name__)


@pytest.fixture(scope="session")
def test_settings() -> TestSettings:
    """Тестовые настройки: TEST_DB_URL и TEST_RABBITMQ_URL, один раз на сессию."""
    return TestSettings()


def _require_test_database_url(test_db_url: str) -> None:
    """Тесты работают только с локальной payments_test БД."""
    url = make_url(test_db_url)
    database = url.database
    hostname = url.host
    port = url.port

    if not database:
        pytest.exit(
            "Refusing to run tests: TEST_DB_URL must specify database name.",
            returncode=1,
        )

    if database != TEST_DATABASE_NAME:
        pytest.exit(
            f"Refusing to run tests: TEST_DB_URL must point to "
            f"'{TEST_DATABASE_NAME}' database, got '{database}'.",
            returncode=1,
        )

    if not hostname:
        pytest.exit(
            "Refusing to run tests: TEST_DB_URL must specify hostname explicitly.",
            returncode=1,
        )

    if hostname not in ALLOWED_TEST_HOSTS:
        pytest.exit(
            f"Refusing to run tests: TEST_DB_URL must use localhost, "
            f"got '{hostname}'. Tests cannot connect to remote databases.",
            returncode=1,
        )

    if port is None:
        pytest.exit(
            "Refusing to run tests: TEST_DB_URL must specify port explicitly.",
            returncode=1,
        )


def _require_test_vhost(test_rabbitmq_url: str) -> str:
    """Тесты работают только в выделенном non-root vhost на localhost."""
    url = make_url(test_rabbitmq_url)
    vhost = (url.database or "").strip()
    hostname = url.host
    port = url.port

    if vhost in {"", "/", "%2f", "%2F"}:
        pytest.exit(
            "Refusing to run tests: TEST_RABBITMQ_URL must point to a "
            f"dedicated non-root vhost, got '{vhost or '<root>'}'. The root "
            "vhost serves the running consumer.",
            returncode=1,
        )

    if not hostname:
        pytest.exit(
            "Refusing to run tests: TEST_RABBITMQ_URL must specify "
            "hostname explicitly.",
            returncode=1,
        )

    if hostname not in ALLOWED_TEST_HOSTS:
        pytest.exit(
            f"Refusing to run tests: TEST_RABBITMQ_URL must use localhost, "
            f"got '{hostname}'. Tests cannot connect to remote RabbitMQ.",
            returncode=1,
        )

    if port is None:
        pytest.exit(
            "Refusing to run tests: TEST_RABBITMQ_URL must specify port explicitly.",
            returncode=1,
        )

    return vhost


async def _management_api(rabbit_url: str) -> tuple[str, tuple[str, str], str]:
    """База management API, креды и экранированное имя тестового vhost."""
    url = make_url(rabbit_url)
    host = url.host
    if not host:
        pytest.exit("TEST_RABBITMQ_URL must specify hostname.", returncode=1)

    port = url.port
    if port is None:
        pytest.exit("TEST_RABBITMQ_URL must specify port.", returncode=1)

    management_port = port + 10000
    vhost = quote(_require_test_vhost(rabbit_url), safe="")
    auth = (url.username or "guest", url.password or "guest")
    return f"http://{host}:{management_port}/api", auth, vhost


async def _ensure_vhost(rabbit_url: str) -> None:
    """Идемпотентно создать тестовый vhost через management API.

    Существующие vhost, очереди и сообщения не затрагиваются; без vhost
    тестовый брокер получит ACCESS_REFUSED.
    """
    base, auth, vhost = await _management_api(rabbit_url)
    user = auth[0]
    try:
        async with httpx.AsyncClient(timeout=5) as client:
            response = await client.put(f"{base}/vhosts/{vhost}", auth=auth)
            if response.status_code not in (HTTPStatus.CREATED, HTTPStatus.NO_CONTENT):
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
                if response.status_code not in (
                    HTTPStatus.OK,
                    HTTPStatus.NO_CONTENT,
                    HTTPStatus.NOT_FOUND,
                ):
                    _logger.warning(
                        "Purge of %s in test vhost returned HTTP %s",
                        queue_name,
                        response.status_code,
                    )
    except httpx.HTTPError as exc:
        _logger.warning("Cannot purge test vhost queues: %s", exc)


@pytest.fixture
def api_headers(test_settings: TestSettings) -> dict[str, str]:
    return {"X-API-Key": test_settings.API_KEY}


@pytest.fixture
async def client() -> AsyncIterator[AsyncClient]:
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac


@pytest.fixture
def test_db_url(test_settings: TestSettings) -> str:
    """Проверенный тестовый DB URL."""
    url = str(test_settings.DB_URL)
    _require_test_database_url(url)
    return url


@pytest.fixture
async def rabbit_test_url(test_settings: TestSettings) -> AsyncIterator[str]:
    """Проверенный тестовый RabbitMQ URL с setup/teardown vhost."""
    url = str(test_settings.RABBITMQ_URL)
    _require_test_vhost(url)
    await _ensure_vhost(url)
    yield url
    await _purge_test_vhost_queues(url)


@pytest.fixture
async def postgres(test_db_url: str) -> AsyncIterator[SessionFactory]:
    """Тестовая БД с dependency override для FastAPI app."""
    engine = create_async_engine(test_db_url, poolclass=NullPool)
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


@pytest.fixture
async def consumer_session_factory(
    test_db_url: str,
) -> AsyncIterator[SessionFactory]:
    """Отдельная session factory для consumer-стороны интеграционных тестов.

    Не переиспользует движок fixture postgres: обработка события в consumer
    идёт через собственные соединения, как в production.
    """
    engine = create_async_engine(test_db_url, poolclass=NullPool)
    factory = async_sessionmaker(engine, expire_on_commit=False)
    try:
        async with factory() as session:
            await session.execute(text("SELECT 1"))
    except Exception as exc:
        await engine.dispose()
        pytest.skip(f"PostgreSQL is not available: {exc}")
    yield factory
    await engine.dispose()
