"""
Процессор для постов Тинькофф Пульса.
"""

import json
from typing import Any, Dict, List, Optional

from src.core.models import ArticleForDB, ParsedItem
from src.domain.parsing.processors.base import BaseProcessor
from src.utils.data import dict_to_json_safe, extract_tickers_from_text, safe_str


class TInvestProcessor(BaseProcessor):
    """
    Процессор для преобразования постов Тинькофф Пульса в модели БД.
    """

    def to_article_db(self, item: ParsedItem) -> ArticleForDB:
        """
        Преобразование поста Тинькофф Пульса в ArticleForDB.

        Args:
            item: Распарсенный пост

        Returns:
            ArticleForDB для сохранения в БД
        """
        # Извлекаем тикеры из контента
        mentioned_tickers = self.extract_tickers(item)

        # Мета-информация
        meta_info = self.extract_tinvest_meta_info(item, mentioned_tickers)
        meta_info_json = dict_to_json_safe(meta_info) if meta_info else None

        # Подготавливаем текст и заголовок
        prepared_title = self.prepare_tinvest_title(item)
        prepared_text = self.prepare_text_for_db(item.content)

        # Создаем модель для БД
        return ArticleForDB(
            source_id=self.source_id,
            original_id=item.original_id,
            url=item.url,
            canonical_url=None,
            raw_title=prepared_title,
            raw_text=prepared_text,
            raw_html=None,  # У постов ТП нет HTML
            media_content=None,
            published_at=item.published_at,
            author=item.author,
            language="ru",
            headers=self.generate_tinvest_headers(),
            meta_info=meta_info_json,
            status="raw",
        )

    def extract_tickers(self, item: ParsedItem) -> List[str]:
        """
        Извлечение тикеров из поста.

        Args:
            item: ParsedItem

        Returns:
            Список тикеров
        """
        tickers = []

        # Из метаданных
        metadata_tickers = item.metadata.get("mentioned_tickers", [])
        if metadata_tickers:
            tickers.extend(metadata_tickers)

        # Из контента (дополнительная проверка)
        content_tickers = extract_tickers_from_text(item.content)
        tickers.extend(content_tickers)

        # Целевой тикер
        target_ticker = item.metadata.get("target_ticker")
        if target_ticker and target_ticker not in tickers:
            tickers.append(target_ticker)

        # Убираем дубликаты
        unique_tickers = list(set([t.upper() for t in tickers if t]))

        return unique_tickers

    def extract_tinvest_meta_info(
        self, item: ParsedItem, tickers: List[str]
    ) -> Dict[str, Any]:
        """
        Извлечение мета-информации для Тинькофф Пульса.

        Args:
            item: ParsedItem
            tickers: Список тикеров

        Returns:
            Словарь с мета-информацией
        """
        metadata = item.metadata

        meta_info = {
            "source": "tinkoff_pulse",
            "tickers": tickers,
            "target_ticker": metadata.get("target_ticker"),
            "mentioned_tickers_count": metadata.get("mentioned_tickers_count", 0),
            "total_reactions": metadata.get("total_reactions", 0),
            "comments_count": metadata.get("comments_count", 0),
            "has_images": metadata.get("has_images", False),
            "images_count": metadata.get("images_count", 0),
            "hashtags_count": metadata.get("hashtags_count", 0),
            "reactions": metadata.get("reactions", {}),
            "post_type": "pulse_post",
        }

        return meta_info

    def prepare_tinvest_title(self, item: ParsedItem, max_length: int = 200) -> str:
        """
        Подготовка заголовка для поста Тинькофф Пульса.

        Args:
            item: ParsedItem
            max_length: Максимальная длина

        Returns:
            Заголовок
        """
        if item.title and item.title != "Без заголовка":
            return self.prepare_title_for_db(item.title, max_length)

        # Генерируем заголовок из контента
        content = item.content

        # Берем первую предложение
        first_sentence = content.split(".")[0].strip()
        if len(first_sentence) > 20:
            title = first_sentence
        else:
            # Или первые 50 символов
            title = content[:100].strip()

        # Добавляем автора если есть
        if item.author:
            title = f"{item.author}: {title}"

        # Добавляем тикеры если есть
        tickers = item.metadata.get("mentioned_tickers", [])
        if tickers:
            tickers_str = ", ".join(tickers[:3])
            title = f"{title} [{tickers_str}]"

        return self.prepare_title_for_db(title, max_length)

    def generate_tinvest_headers(self) -> str:
        """
        Генерация заголовков HTTP для Тинькофф Пульса.

        Returns:
            JSON строка с заголовками
        """
        headers = {
            "User-Agent": "TInvestParser/1.0",
            "Accept": "application/json",
            "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
        }

        return json.dumps(headers, ensure_ascii=False)

    def generate_canonical_url(self, item: ParsedItem) -> Optional[str]:
        """
        Генерация канонического URL для Тинькофф Пульса.
        Возвращает оригинальный URL.

        Args:
            item: ParsedItem

        Returns:
            Канонический URL
        """
        return item.url
