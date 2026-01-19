import os
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from pydantic import Field, PostgresDsn
from pydantic_settings import BaseSettings

load_dotenv()


class Settings(BaseSettings):
    """Основные настройки проекта"""

    # Настройки БД
    DB_HOST: str = Field("localhost", env="DB_HOST")
    DB_PORT: int = Field(5432, env="DB_PORT")
    DB_NAME: str = Field("news_graph", env="DB_NAME")
    DB_USER: str = Field("postgres", env="DB_USER")
    DB_PASSWORD: str = Field("", env="DB_PASSWORD")

    # Настройки парсеров
    PARSER_REQUEST_DELAY: float = Field(2.0, env="PARSER_REQUEST_DELAY")
    PARSER_MAX_RETRIES: int = Field(3, env="PARSER_MAX_RETRIES")
    PARSER_TIMEOUT: int = Field(30, env="PARSER_TIMEOUT")

    # Логирование
    LOG_LEVEL: str = Field("INFO", env="LOG_LEVEL")
    LOG_DIR: str = Field("logs", env="LOG_DIR")

    # Телеграм (опционально)
    TELEGRAM_BOT_TOKEN: Optional[str] = Field(None, env="TELEGRAM_BOT_TOKEN")
    ADMIN_CHAT_ID: Optional[str] = Field(None, env="ADMIN_CHAT_ID")

    # Время
    TIMEZONE: str = Field("Europe/Moscow", env="TIMEZONE")

    @property
    def database_url(self) -> str:
        """URL подключения к PostgreSQL"""
        return f"postgresql://{self.DB_USER}:{self.DB_PASSWORD}@{self.DB_HOST}:{self.DB_PORT}/{self.DB_NAME}"

    @property
    def database_dict(self) -> Dict[str, Any]:
        """Параметры подключения как словарь для psycopg2"""
        return {
            "host": self.DB_HOST,
            "port": self.DB_PORT,
            "database": self.DB_NAME,
            "user": self.DB_USER,
            "password": self.DB_PASSWORD,
        }

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


# Глобальный экземпляр настроек
settings = Settings()
