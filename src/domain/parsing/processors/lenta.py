"""
Процессор для статей Lenta.ru.
"""

import json
from typing import Any, Dict, Optional

from src.core.models import ArticleForDB, ParsedItem
from src.domain.parsing.processors.base import BaseProcessor
from src.utils.data import extract_domain, safe_str, dict_to_json_safe


class LentaProcessor(BaseProcessor):
    """
    Процессор для преобразования статей Lenta.ru в модели БД.
    """

    def to_article_db(self, item: ParsedItem) -> ArticleForDB:
        """
        Преобразование статьи Lenta.ru в ArticleForDB.

        Args:
            item: Распарсенный элемент Lenta.ru

        Returns:
            ArticleForDB для сохранения в БД
        """
        # Извлекаем дополнительные данные
        raw_data = item.raw_data or {}

        # HTML контент (если есть)
        raw_html = raw_data.get("html", "")

        # Медиа-контент
        media_content = self.extract_lenta_media_content(item, raw_data)
        media_content_json = dict_to_json_safe(media_content) if media_content else None

        # Мета-информация
        meta_info = self.extract_lenta_meta_info(item, raw_data)
        meta_info_json = dict_to_json_safe(meta_info) if meta_info else None

        # Канонический URL (оригинальный URL из RSS)
        canonical_url = raw_data.get("rss", {}).get("link")
        if canonical_url == item.url:
            canonical_url = None

        # Подготавливаем текст и заголовок
        prepared_title = self.prepare_title_for_db(item.title)
        prepared_text = self.prepare_text_for_db(item.content)

        # Создаем модель для БД
        return ArticleForDB(
            source_id=self.source_id,
            original_id=item.original_id,
            url=item.url,
            canonical_url=canonical_url,
            raw_title=prepared_title,
            raw_text=prepared_text,
            raw_html=raw_html if raw_html else None,
            media_content=media_content_json,
            published_at=item.published_at,
            author=item.author,
            language="ru",
            headers=self.generate_headers(item),
            meta_info=meta_info_json,
            status="raw",
        )

    def extract_lenta_media_content(
        self, item: ParsedItem, raw_data: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Извлечение медиа-контента для Lenta.ru.

        Args:
            item: ParsedItem
            raw_data: Сырые данные

        Returns:
            Словарь с медиа-контентом
        """
        media_content = []

        # Из метаданных
        if "media_content" in item.metadata:
            media_content.extend(item.metadata["media_content"])

        # Из RSS enclosure (если есть)
        rss_data = raw_data.get("rss", {})
        if rss_data.get("enclosure_url"):
            media_content.append(
                {
                    "type": "image",
                    "url": rss_data["enclosure_url"],
                    "source": "rss_enclosure",
                }
            )

        # Из HTML (упрощенно - можно расширить парсингом изображений)
        html_data = raw_data.get("html", "")
        if html_data and "img" in html_data:
            # Здесь можно добавить парсинг изображений из HTML
            pass

        return media_content if media_content else None

    def extract_lenta_meta_info(
        self, item: ParsedItem, raw_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Извлечение мета-информации для Lenta.ru.

        Args:
            item: ParsedItem
            raw_data: Сырые данные

        Returns:
            Словарь с мета-информацией
        """
        meta_info = {
            "source": "lenta.ru",
            "domain": extract_domain(item.url),
            "category": item.metadata.get("category", ""),
            "description": item.metadata.get("description", ""),
            "text_length": item.metadata.get("text_length", len(item.content)),
            "paragraphs_count": item.metadata.get("paragraphs_count"),
            "word_count": item.metadata.get("word_count"),
            "archive_date": item.metadata.get("archive_date"),
            "has_images": item.metadata.get("has_images", False),
        }

        # Добавляем данные из RSS
        rss_data = raw_data.get("rss", {})
        if rss_data:
            meta_info.update(
                {
                    "rss_guid": rss_data.get("guid"),
                    "rss_category": rss_data.get("category"),
                    "rss_summary": rss_data.get("summary"),
                }
            )

        return meta_info

    def generate_headers(self, item: ParsedItem) -> str:
        """
        Генерация заголовков HTTP для Lenta.ru.

        Args:
            item: ParsedItem

        Returns:
            JSON строка с заголовками
        """
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
            "Referer": "https://lenta.ru/",
        }

        return json.dumps(headers, ensure_ascii=False)

    def generate_canonical_url(self, item: ParsedItem) -> Optional[str]:
        """
        Генерация канонического URL для Lenta.ru.
        Использует оригинальный URL из RSS если он отличается.

        Args:
            item: ParsedItem

        Returns:
            Канонический URL
        """
        raw_data = item.raw_data or {}
        rss_url = raw_data.get("rss", {}).get("link", "")

        if rss_url and rss_url != item.url:
            return rss_url

        return item.url
