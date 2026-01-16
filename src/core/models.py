"""Pydantic модели данных"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field, HttpUrl


class ParsedItem(BaseModel):
    """Унифицированная модель распарсенного элемента"""

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

    class Config:
        from_attributes = True


class ArticleForDB(BaseModel):
    """Модель для сохранения в БД"""

    source_id: int
    original_id: str
    url: str
    canonical_url: Optional[str] = None
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

    class Config:
        from_attributes = True


class ProcessingStats(BaseModel):
    """Статистика обработки"""

    total_rows: int = 0
    saved: int = 0
    skipped: int = 0
    errors: int = 0

    def add(self, other: "ProcessingStats") -> "ProcessingStats":
        """Сложение статистик"""
        return ProcessingStats(
            total_rows=self.total_rows + other.total_rows,
            saved=self.saved + other.saved,
            skipped=self.skipped + other.skipped,
            errors=self.errors + other.errors,
        )


# Для обратной совместимости с текущим кодом
class PulsePost(BaseModel):
    """Модель поста из Тинькофф Пульса (легаси)"""

    date: str
    time: str
    username: str
    post_text: str
    target_ticker: str
    mentioned_tickers: List[str]

    @property
    def published_datetime(self) -> Optional[datetime]:
        """Объединённая дата и время"""
        try:
            return datetime.strptime(f"{self.date} {self.time}", "%Y-%m-%d %H:%M:%S")
        except:
            return None


class LentaArticle(BaseModel):
    """Модель статьи Lenta.ru (легаси)"""

    guid: str
    rss_title: str
    rss_link: str
    rss_author: Optional[str] = None
    rss_published: Optional[datetime] = None
    rss_category: Optional[str] = None
    rss_summary: Optional[str] = None
    html_title: str
    full_text: str
    lead_text: Optional[str] = None
    html_author: Optional[str] = None
    published_time: Optional[datetime] = None
    keywords: List[str] = []
    description: Optional[str] = None
    canonical_url: str
    media_content: List[Dict[str, Any]] = []
    paragraphs_count: int = 0
    word_count: int = 0
    retrieved_at: datetime
    source: str = "lenta.ru"
