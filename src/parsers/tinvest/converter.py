"""
Конвертер для TInvest.
Преобразует ParsedItem в ArticleForDB.
"""

import json
from typing import Optional

from src.core.models import ArticleForDB, ParsedItem
from src.utils.datetime_utils import naive_msk_dt


class TInvestConverter:
    """Конвертер постов Тинькофф Пульса."""

    def convert(self, item: ParsedItem) -> ArticleForDB:
        """Преобразование в модель БД."""
        return ArticleForDB(
            source_id=item.source_id,
            original_id=item.original_id,
            url=item.url,
            raw_title=self._prepare_title(item.title),
            raw_text=self._prepare_text(item.content),
            raw_html=None,  # У TP нет HTML
            published_at=(naive_msk_dt(item.published_at) if item.published_at else None),
            author=item.author,
            language="ru",
            media_content=self._prepare_media(item.metadata.get("images", [])),
            meta_info=self._prepare_meta(item),
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

    def _prepare_media(self, images: list) -> Optional[str]:
        """Подготовка медиа-контента."""
        if not images:
            return None

        media = []
        for img in images:
            if isinstance(img, dict) and "url" in img:
                media.append(
                    {
                        "url": img["url"],
                        "type": "image",
                        "alt": img.get("alt", ""),
                    }
                )

        return json.dumps(media, ensure_ascii=False) if media else None

    def _prepare_meta(self, item: ParsedItem) -> Optional[str]:
        """Подготовка мета-информации."""
        meta = {
            "mentioned_tickers": item.metadata.get("mentioned_tickers", []),
            "total_reactions": item.metadata.get("total_reactions", 0),
            "comments_count": item.metadata.get("comments_count", 0),
            "has_images": item.metadata.get("has_images", False),
            "hashtags": item.metadata.get("hashtags", []),
            "target_ticker": item.metadata.get("target_ticker"),
        }

        return json.dumps(meta, ensure_ascii=False) if meta else None
