"""
Конвертер для РБК.
Преобразует ParsedItem в ArticleForDB.
"""

import json

from src.core.models import ArticleForDB, ParsedItem
from src.utils.datetime_utils import naive_msk_dt


class RbcConverter:
    def convert(self, item: ParsedItem) -> ArticleForDB:
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
            raw_title=item.title[:500] if item.title else "Без заголовка",
            raw_text=item.content[:10000] if item.content else "",
            published_at=(naive_msk_dt(item.published_at) if item.published_at else None),
            language="ru",
            meta_info=meta_info,
            status="raw",
        )
