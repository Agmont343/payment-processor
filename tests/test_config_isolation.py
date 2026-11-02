"""Тесты для проверки разделения production и test конфигурации."""

import pytest
from sqlalchemy.engine import make_url

from app.config import TestSettings as _TestSettings
from tests.conftest import (
    ALLOWED_TEST_HOSTS,
    TEST_DATABASE_NAME,
    _require_test_database_url,
    _require_test_vhost,
)


def test_test_settings_loads_from_env(test_settings: _TestSettings) -> None:
    assert test_settings.API_KEY
    assert test_settings.WEBHOOK_ALLOW_PRIVATE is True

    db_url = make_url(str(test_settings.DB_URL))
    assert db_url.database == TEST_DATABASE_NAME


def test_test_db_url_fixture_validates(test_db_url: str) -> None:
    url = make_url(test_db_url)

    assert url.database == TEST_DATABASE_NAME
    assert url.host in ALLOWED_TEST_HOSTS
    assert url.port is not None


def test_rabbit_test_url_fixture_validates(test_settings: _TestSettings) -> None:
    url = make_url(str(test_settings.RABBITMQ_URL))

    assert url.database  # vhost должен быть не пустым
    assert url.database not in {"/", "%2f", "%2F"}  # не root vhost
    assert url.host in ALLOWED_TEST_HOSTS
    assert url.port is not None


def test_postgres_fixture_uses_test_db(postgres) -> None:
    """Smoke-тест: fixture инициализируется и проходит защитные проверки."""
    assert postgres is not None


class TestDatabaseUrlGuard:
    """Guard test_db_url: только локальная payments_test БД."""

    def test_rejects_wrong_database(self) -> None:
        wrong_url = "postgresql://user:pass@localhost:5432/production_db"

        with pytest.raises(pytest.exit.Exception) as exc_info:
            _require_test_database_url(wrong_url)

        assert "payments_test" in str(exc_info.value)
        assert "production_db" in str(exc_info.value)

    def test_rejects_remote_host(self) -> None:
        remote_url = "postgresql://user:pass@remote.server.com:5432/payments_test"

        with pytest.raises(pytest.exit.Exception) as exc_info:
            _require_test_database_url(remote_url)

        assert "localhost" in str(exc_info.value)
        assert "remote.server.com" in str(exc_info.value)

    def test_rejects_missing_port(self) -> None:
        # SQLAlchemy URL без явного порта (port=None)
        no_port_url = "postgresql://user:pass@localhost/payments_test"

        with pytest.raises(pytest.exit.Exception) as exc_info:
            _require_test_database_url(no_port_url)

        assert "must specify port" in str(exc_info.value)


class TestRabbitVhostGuard:
    """Guard rabbit_test_url: только выделенный non-root vhost на localhost."""

    def test_rejects_root_vhost(self) -> None:
        root_url = "amqp://guest:guest@localhost:5672/"

        with pytest.raises(pytest.exit.Exception) as exc_info:
            _require_test_vhost(root_url)

        assert "non-root vhost" in str(exc_info.value)

    def test_rejects_remote_host(self) -> None:
        remote_url = "amqp://guest:guest@remote.mq.com:5672/test_vhost"

        with pytest.raises(pytest.exit.Exception) as exc_info:
            _require_test_vhost(remote_url)

        assert "localhost" in str(exc_info.value)
