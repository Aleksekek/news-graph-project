"""
Pydantic модели данных проекта.
Все модели в одном месте для чёткости.
"""

from datetime import datetime
from typing import Any

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
    published_at: datetime | None = None
    author: str | None = None
    metadata: dict[str, Any] = Field(default_factory=dict)
    raw_data: dict[str, Any] = Field(default_factory=dict)

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
    raw_html: str | None = None
    published_at: datetime | None = None
    author: str | None = None
    language: str = "ru"
    headers: str | None = None
    meta_info: str | None = None
    media_content: str | None = None
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


class ExtractedEntity(BaseModel):
    """Сущность, извлечённая NER-моделью из текста статьи."""

    original_name: str
    normalized_name: str
    entity_type: str  # 'person' | 'organization' | 'location' | 'event' (event только из LLM-NER)
    count: int = 1
    importance_score: float = 0.3
    context_snippet: str | None = None

    model_config = ConfigDict(from_attributes=True)


class NERStats(BaseModel):
    """Статистика одного NER-прогона."""

    total_articles: int = 0
    processed: int = 0
    failed: int = 0
    total_entities: int = 0
    new_entities: int = 0

    def add(self, other: "NERStats") -> "NERStats":
        return NERStats(
            total_articles=self.total_articles + other.total_articles,
            processed=self.processed + other.processed,
            failed=self.failed + other.failed,
            total_entities=self.total_entities + other.total_entities,
            new_entities=self.new_entities + other.new_entities,
        )

    model_config = ConfigDict(from_attributes=True)


class ParserConfig(BaseModel):
    """
    Конфигурация парсера.
    Передаётся в конструктор парсера.
    """

    source_id: int
    source_name: str
    base_url: str | None = None
    request_delay: float = 1.0
    max_retries: int = 3
    timeout: int = 30
    user_agent: str = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

    # Разрешаем дополнительные поля для конкретных парсеров
    model_config = ConfigDict(extra="allow")
