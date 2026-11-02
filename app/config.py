from pydantic import AmqpDsn, Field, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Production настройки приложения."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    API_KEY: str
    DB_URL: PostgresDsn
    DB_ECHO: bool = False
    RABBITMQ_URL: AmqpDsn
    WEBHOOK_ALLOW_PRIVATE: bool = False


class TestSettings(BaseSettings):
    """Тестовые настройки. Читает TEST_DB_URL и TEST_RABBITMQ_URL из .env.

    WEBHOOK_ALLOW_PRIVATE всегда True для тестов (игнорирует значение из .env).
    """

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        # Явно указываем, какие поля читать из env
        env_prefix="",
    )

    API_KEY: str
    DB_URL: PostgresDsn = Field(validation_alias="TEST_DB_URL")
    DB_ECHO: bool = False
    RABBITMQ_URL: AmqpDsn = Field(validation_alias="TEST_RABBITMQ_URL")

    @property
    def WEBHOOK_ALLOW_PRIVATE(self) -> bool:
        """Всегда True для тестов, независимо от .env."""
        return True


settings = Settings()
