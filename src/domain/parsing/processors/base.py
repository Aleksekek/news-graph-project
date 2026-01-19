"""
Базовый класс для процессоров, которые преобразуют ParsedItem в модели БД.
"""

import json
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, Dict, List, Optional

from src.core.models import ArticleForDB, ParsedItem, ProcessingStats
from src.utils.data import (
    dict_to_json_safe,
    generate_content_hash,
    safe_str,
    truncate_text,
)


class BaseProcessor(ABC):
    """
    Базовый класс для преобразования ParsedItem в ArticleForDB.
    Каждый источник может иметь свой процессор.
    """

    def __init__(self, source_id: int, source_name: str):
        """
        Args:
            source_id: ID источника в БД
            source_name: Имя источника
        """
        self.source_id = source_id
        self.source_name = source_name

    @abstractmethod
    def to_article_db(self, item: ParsedItem) -> ArticleForDB:
        """
        Преобразование ParsedItem в ArticleForDB.

        Args:
            item: Распарсенный элемент

        Returns:
            Модель для сохранения в БД
        """
        pass

    def batch_to_articles_db(self, items: List[ParsedItem]) -> List[ArticleForDB]:
        """
        Пакетное преобразование элементов.

        Args:
            items: Список ParsedItem

        Returns:
            Список ArticleForDB
        """
        return [self.to_article_db(item) for item in items if item]

    def extract_media_content(self, item: ParsedItem) -> Optional[str]:
        """
        Извлечение медиа-контента из ParsedItem.
        По умолчанию извлекает из metadata.

        Args:
            item: ParsedItem

        Returns:
            JSON строка с медиа-контентом или None
        """
        media_content = item.metadata.get("media_content", [])
        if media_content:
            return dict_to_json_safe(media_content)
        return None

    def extract_meta_info(self, item: ParsedItem) -> Optional[str]:
        """
        Извлечение мета-информации.

        Args:
            item: ParsedItem

        Returns:
            JSON строка с мета-информацией
        """
        meta_info = {
            "source": self.source_name,
            "original_id": item.original_id,
            "content_hash": generate_content_hash(item.content),
            "text_length": len(item.content),
            "category": item.metadata.get("category"),
            "tickers": item.metadata.get("tickers", []),
            "reactions": item.metadata.get("reactions", {}),
            "parsed_at": datetime.now().isoformat(),
        }

        # Добавляем специфичные для источника данные
        source_specific = item.metadata.get("source_specific", {})
        meta_info.update(source_specific)

        return dict_to_json_safe(meta_info)

    def prepare_text_for_db(self, text: str, max_length: int = 10000) -> str:
        """
        Подготовка текста для сохранения в БД.

        Args:
            text: Исходный текст
            max_length: Максимальная длина

        Returns:
            Подготовленный текст
        """
        if not text:
            return ""

        # Очищаем текст
        cleaned = text.strip()

        # Обрезаем если слишком длинный
        if len(cleaned) > max_length:
            cleaned = truncate_text(cleaned, max_length - 100, "... [TEXT_TRUNCATED]")

        return cleaned

    def prepare_title_for_db(self, title: str, max_length: int = 500) -> str:
        """
        Подготовка заголовка для сохранения в БД.

        Args:
            title: Исходный заголовок
            max_length: Максимальная длина

        Returns:
            Подготовленный заголовок
        """
        if not title:
            return "Без заголовка"

        cleaned = safe_str(title).strip()

        if len(cleaned) > max_length:
            cleaned = cleaned[:max_length] + "..."

        return cleaned
