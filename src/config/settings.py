import os
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from pydantic import ConfigDict, Field, field_validator
from pydantic_settings import BaseSettings

load_dotenv()


class Settings(BaseSettings):
    """Основные настройки проекта"""

    # База данных
    DB_HOST: str = Field(default="localhost", alias="DB_HOST")
    DB_PORT: int = Field(default=5432, alias="DB_PORT")
    DB_NAME: str = Field(default="news_graph", alias="DB_NAME")
    DB_USER: str = Field(default="postgres", alias="DB_USER")
    DB_PASSWORD: str = Field(default="", alias="DB_PASSWORD")

    # LLM
    DEEPSEEK_API_KEY: str = Field(default="", alias="DEEPSEEK_API_KEY")

    # Парсеры
    PARSER_REQUEST_DELAY: float = Field(default=2.0, alias="PARSER_REQUEST_DELAY")
    PARSER_MAX_RETRIES: int = Field(default=3, alias="PARSER_MAX_RETRIES")
    PARSER_TIMEOUT: int = Field(default=30, alias="PARSER_TIMEOUT")

    # Логирование
    LOG_LEVEL: str = Field(default="INFO", alias="LOG_LEVEL")
    LOG_DIR: str = Field(default="logs", alias="LOG_DIR")

    # Telegram
    TELEGRAM_BOT_TOKEN: Optional[str] = Field(default=None, alias="TELEGRAM_BOT_TOKEN")
    ADMIN_CHAT_ID: Optional[str] = Field(default=None, alias="ADMIN_CHAT_ID")
    PROXY_URL: Optional[str] = Field(default=None, alias="PROXY_URL")

    # Время
    TIMEZONE: str = Field(default="Europe/Moscow", alias="TIMEZONE")

    model_config = ConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    @field_validator("DB_PORT", mode="before")
    @classmethod
    def parse_db_port(cls, v):
        """Преобразует строку в число, если нужно."""
        if isinstance(v, str):
            try:
                return int(v)
            except ValueError:
                return 5432
        return v

    @field_validator("PARSER_MAX_RETRIES", "PARSER_TIMEOUT", mode="before")
    @classmethod
    def parse_int(cls, v):
        """Преобразует строку в число."""
        if isinstance(v, str):
            try:
                return int(v)
            except ValueError:
                return 3 if "RETRIES" in str(v) else 30
        return v

    @field_validator("PARSER_REQUEST_DELAY", mode="before")
    @classmethod
    def parse_float(cls, v):
        """Преобразует строку в float."""
        if isinstance(v, str):
            try:
                return float(v)
            except ValueError:
                return 2.0
        return v

    @property
    def database_url(self) -> str:
        """URL для asyncpg"""
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    @property
    def database_dict(self) -> Dict[str, Any]:
        """Параметры для psycopg2 (если нужно)"""
        return {
            "host": self.DB_HOST,
            "port": self.DB_PORT,
            "database": self.DB_NAME,
            "user": self.DB_USER,
            "password": self.DB_PASSWORD,
        }


# Глобальный экземпляр
settings = Settings()
