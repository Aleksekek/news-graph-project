"""
Конвертер для Lenta.ru.
Преобразует ParsedItem в ArticleForDB.
"""

from src.core.models import ArticleForDB, ParsedItem
from src.utils.datetime_utils import format_for_db


class LentaConverter:
    """Конвертер статей Lenta.ru."""

    def convert(self, item: ParsedItem) -> ArticleForDB:
        """Преобразование в модель БД."""
        return ArticleForDB(
            source_id=item.source_id,
            original_id=item.original_id,
            url=item.url,
            raw_title=self._prepare_title(item.title),
            raw_text=self._prepare_text(item.content),
            raw_html=item.raw_data.get("html"),
            published_at=(
                format_for_db(item.published_at) if item.published_at else None
            ),
            author=item.author,
            language="ru",
            meta_info=str(item.metadata) if item.metadata else None,
            status="raw",
        )

    def _prepare_title(self, title: str, max_length: int = 500) -> str:
        """Подготовка заголовка."""
        if not title:
            return "Без заголовка"

        cleaned = title.strip()
        if len(cleaned) > max_length:
            cleaned = cleaned[:max_length] + "..."

        return cleaned

    def _prepare_text(self, text: str, max_length: int = 10000) -> str:
        """Подготовка текста."""
        if not text:
            return ""

        cleaned = text.strip()
        if len(cleaned) > max_length:
            cleaned = cleaned[:max_length] + "... [TRUNCATED]"

        return cleaned
