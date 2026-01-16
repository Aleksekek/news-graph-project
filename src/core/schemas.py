"""Схемы валидации входных данных"""

from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field, HttpUrl, validator


class ParserConfigSchema(BaseModel):
    """Схема конфигурации парсера"""

    source_id: int
    source_name: str
    base_url: Optional[HttpUrl] = None
    request_delay: float = Field(1.0, ge=0.1, le=10.0)
    max_retries: int = Field(3, ge=1, le=10)
    timeout: int = Field(30, ge=5, le=120)
    user_agent: Optional[str] = None

    @validator("request_delay")
    def validate_delay(cls, v):
        if v < 0.1:
            raise ValueError("Delay too short, may cause rate limiting")
        return v


class ParseTaskSchema(BaseModel):
    """Схема задачи парсинга"""

    source_name: str
    limit: int = Field(100, ge=1, le=10000)
    start_date: Optional[datetime] = None
    end_date: Optional[datetime] = None
    categories: Optional[List[str]] = None

    @validator("end_date")
    def validate_dates(cls, v, values):
        if v and values.get("start_date") and v < values["start_date"]:
            raise ValueError("End date must be after start date")
        return v


class DatabaseConfigSchema(BaseModel):
    """Схема конфигурации БД"""

    host: str
    port: int = Field(5432, ge=1, le=65535)
    database: str
    user: str
    password: str
    pool_size: int = Field(10, ge=1, le=50)

    @validator("port")
    def validate_port(cls, v):
        if v <= 0 or v > 65535:
            raise ValueError("Port must be between 1 and 65535")
        return v
