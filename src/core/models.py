"""
Pydantic модели данных проекта.
Все модели в одном месте для чёткости.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


class ParsedItem(BaseModel):
    """
    Унифицированная модель распарсенного элемента.
    Результат работы любого парсера.
    """

    source_id: int
    source_name: str
    original_id: str
    url: str
    title: str
    content: str
    published_at: Optional[datetime] = None
    author: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)
    raw_data: Dict[str, Any] = Field(default_factory=dict)

    model_config = ConfigDict(from_attributes=True)


class ArticleForDB(BaseModel):
    """
    Модель для сохранения в БД (raw_articles).
    """

    source_id: int
    original_id: str
    url: str
    raw_title: str
    raw_text: str
    raw_html: Optional[str] = None
    published_at: Optional[datetime] = None
    author: Optional[str] = None
    language: str = "ru"
    headers: Optional[str] = None
    meta_info: Optional[str] = None
    media_content: Optional[str] = None
    status: str = "raw"

    model_config = ConfigDict(from_attributes=True)


class ProcessingStats(BaseModel):
    """
    Статистика обработки (для возврата из use cases).
    """

    total_rows: int = 0
    saved: int = 0
    skipped: int = 0
    errors: int = 0

    def add(self, other: "ProcessingStats") -> "ProcessingStats":
        return ProcessingStats(
            total_rows=self.total_rows + other.total_rows,
            saved=self.saved + other.saved,
            skipped=self.skipped + other.skipped,
            errors=self.errors + other.errors,
        )

    model_config = ConfigDict(from_attributes=True)


class ParserConfig(BaseModel):
    """
    Конфигурация парсера.
    Передаётся в конструктор парсера.
    """

    source_id: int
    source_name: str
    base_url: Optional[str] = None
    request_delay: float = 1.0
    max_retries: int = 3
    timeout: int = 30
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

    # Разрешаем дополнительные поля для конкретных парсеров
    model_config = ConfigDict(extra="allow")
