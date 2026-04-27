"""
Конвертер для Lenta.ru.
Преобразует ParsedItem в ArticleForDB.
"""

import json
from typing import Any, Dict

from src.core.models import ArticleForDB, ParsedItem
from src.utils.datetime_utils import format_for_db


class LentaConverter:
    """Конвертер статей Lenta.ru."""

    def convert(self, item: ParsedItem) -> ArticleForDB:
        """Преобразование в модель БД."""
        meta_info = None
        if item.metadata:
            try:
                meta_info = json.dumps(item.metadata, ensure_ascii=False)
            except Exception:
                meta_info = str(item.metadata)

        return ArticleForDB(
            source_id=item.source_id,
            original_id=item.original_id,
            url=item.url,
            raw_title=self._prepare_title(item.title),
            raw_text=self._prepare_text(item.content),
            raw_html=item.raw_data.get("html") if item.raw_data else None,
            published_at=(format_for_db(item.published_at) if item.published_at else None),
            author=item.author,
            language="ru",
            meta_info=meta_info,
            status="raw",
        )
