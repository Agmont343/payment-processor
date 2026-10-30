from pydantic import AmqpDsn, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    API_KEY: str
    DB_URL: PostgresDsn
    TEST_DB_URL: PostgresDsn
    DB_ECHO: bool = False
    RABBITMQ_URL: AmqpDsn
    TEST_RABBITMQ_URL: AmqpDsn
    WEBHOOK_ALLOW_PRIVATE: bool = False


settings = Settings()
