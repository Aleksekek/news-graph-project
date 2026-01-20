"""
Процессор для постов Тинькофф Пульса.
Расширенная логика с полным маппингом на ArticleForDB.
"""

from datetime import datetime
from typing import Any, Dict, List, Optional

from src.core.models import ArticleForDB, ParsedItem
from src.domain.parsing.processors.base import BaseProcessor
from src.utils.data import dict_to_json_safe, extract_tickers_from_text, safe_str


class TInvestProcessor(BaseProcessor):
    """
    Процессор для преобразования постов Тинькофф Пульса в модели БД.
    Использует все доступные метаданные для полноты информации.
    """

    def to_article_db(self, item: ParsedItem) -> ArticleForDB:
        """
        Преобразование поста Тинькофф Пульса в ArticleForDB.

        Args:
            item: Распарсенный пост

        Returns:
            ArticleForDB для сохранения в БД
        """
        # Извлекаем тикеры
        mentioned_tickers = self.extract_tickers(item)

        # Медиа-контент
        media_content = self.extract_media_content(item)

        # Мета-информация
        meta_info = self.extract_tinvest_meta_info(item, mentioned_tickers)
        meta_info_json = dict_to_json_safe(meta_info) if meta_info else None

        # Подготавливаем текст и заголовок
        prepared_title = self.prepare_tinvest_title(item)
        prepared_text = self.prepare_text_for_db(item.content, max_length=10000)

        # Заголовки (null для TP, как в оригинале)
        headers = None

        # Создаем модель для БД
        return ArticleForDB(
            source_id=self.source_id,
            original_id=item.original_id,
            url=item.url,
            raw_title=prepared_title,
            raw_text=prepared_text,
            raw_html=None,  # Посты TP - текст, без HTML
            media_content=media_content,
            published_at=item.published_at,
            author=item.author,
            language="ru",
            headers=headers,
            meta_info=meta_info_json,
            status="raw",
        )

    def extract_tickers(self, item: ParsedItem) -> List[str]:
        """
        Извлечение тикеров из поста (из метаданных и текста).
        Корректировка: учитывает, что тикеры могут быть str или dict с полем 'ticker'.

        Args:
            item: ParsedItem

        Returns:
            Список уникальных тикеров
        """
        tickers = set()

        # Из метаданных (основной источник) - может быть список str или dict
        metadata_tickers = item.metadata.get("mentioned_tickers", [])
        for ticker in metadata_tickers:
            if isinstance(ticker, str):
                tickers.add(ticker.upper())
            elif isinstance(ticker, dict) and "ticker" in ticker:
                tickers.add(ticker["ticker"].upper())

        # Из полного списка instruments (если есть дополнительные данные)
        instruments = item.metadata.get("instruments", [])
        for instr in instruments:
            if isinstance(instr, dict) and "ticker" in instr:
                tickers.add(instr["ticker"].upper())

        # Из текста дополнительно
        content_tickers = extract_tickers_from_text(item.content)
        for ticker in content_tickers:
            if isinstance(ticker, str):
                tickers.add(ticker.upper())

        # Целевой тикер
        target_ticker = item.metadata.get("target_ticker")
        if isinstance(target_ticker, str):
            tickers.add(target_ticker.upper())

        # Убираем дубликаты и нормализуем
        unique_tickers = sorted(list(tickers))
        return unique_tickers

    def extract_media_content(self, item: ParsedItem) -> Optional[str]:
        """
        Извлечение медиа-контента из поста (изображения).

        Args:
            item: ParsedItem

        Returns:
            JSON строка с медиа-контентом или None
        """
        media_content = []

        # Из метаданных (основной источник)
        images = item.metadata.get("images", [])
        for img in images:
            if isinstance(img, dict) and "url" in img:
                media_content.append(
                    {
                        "url": img.get("url", ""),
                        "type": "image",
                        "alt": img.get("alt", ""),
                    }
                )

        # Из raw_data, если есть дополнительные медиа
        raw_post = item.raw_data.get("original_post", {}).get("content", {})
        raw_images = raw_post.get("images", [])
        for img in raw_images:
            if "url" in img and img["url"] not in [m["url"] for m in media_content]:
                media_content.append(
                    {
                        "url": img.get("url", ""),
                        "type": "image",
                        "alt": img.get("alt", ""),
                    }
                )

        return dict_to_json_safe(media_content) if media_content else None

    def extract_tinvest_meta_info(
        self, item: ParsedItem, tickers: List[str]
    ) -> Dict[str, Any]:
        """
        Извлечение полной мета-информации для Тинькофф Пульса.

        Args:
            item: ParsedItem
            tickers: Список тикеров

        Returns:
            Словарь с мета-информацией
        """
        metadata = item.metadata
        raw_post = item.raw_data.get("original_post", {})

        meta_info = {
            # Основная информация
            "target_ticker": metadata.get("target_ticker"),
            "source": "tinkoff_pulse",
            "post_type": "pulse_post",
            "original_id": raw_post.get("id", ""),
            # Финансовая информация
            "tickers": tickers,
            "mentioned_tickers_count": len(tickers),
            "instruments": metadata.get(
                "instruments", []
            ),  # Полные данные инструментов
            # Социальные метрики
            "total_reactions": metadata.get("total_reactions", 0),
            "comments_count": metadata.get("comments_count", 0),
            "reactions": metadata.get("reactions", {}),
            # Контент
            "has_images": metadata.get("has_images", False),
            "images_count": metadata.get("images_count", 0),
            "images": metadata.get("images", []),  # Ссылки на изображения
            "hashtags": metadata.get("hashtags", []),
            "hashtags_count": metadata.get("hashtags_count", 0),
            "profiles": metadata.get("profiles", []),
            "profiles_count": metadata.get("profiles_count", 0),
            "strategies": metadata.get("strategies", []),
            "strategies_count": metadata.get("strategies_count", 0),
            # Автор и владелец
            "author": item.author,
            "owner": metadata.get("owner", {}),
            # Технические данные поста
            "service_tags": metadata.get("service_tags", []),
            "is_editable": metadata.get("is_editable", False),
            "base_tariff_category": metadata.get("base_tariff_category", ""),
            "status": raw_post.get("status", ""),
            "is_bookmarked": raw_post.get("isBookmarked", False),
            # Парсинг и чекпоинты
            "cursor": metadata.get("cursor"),  # Для продолжаемого парсинга
            "parsed_at": datetime.now().isoformat(),
            "content_hash": self.generate_content_hash(item.content),
            "text_length": len(item.content),
            # Дополнительно из raw_data
            "raw_service_tags": raw_post.get("serviceTags", []),
            "raw_inserted": raw_post.get("inserted", ""),
        }

        return meta_info

    def extract_meta_info(self, item: ParsedItem) -> Optional[str]:
        """
        Переопределяем базовый метод для использования extract_tinvest_meta_info.
        """
        tickers = self.extract_tickers(item)
        meta_dict = self.extract_tinvest_meta_info(item, tickers)
        return dict_to_json_safe(meta_dict) if meta_dict else None

    def prepare_tinvest_title(self, item: ParsedItem, max_length: int = 500) -> str:
        """
        Подготовка заголовка для поста Тинькофф Пульса.
        Улучшенная логика с использованием автора и тикеров.

        Args:
            item: ParsedItem
            max_length: Максимальная длина

        Returns:
            Заголовок
        """
        # Если заголовок уже есть и валиден
        if item.title and item.title != "Без заголовка" and len(item.title.strip()) > 5:
            return self.prepare_title_for_db(item.title, max_length)

        content = item.content.strip()
        if not content:
            return "Без заголовка"

        # Приоритет: первая строка
        lines = content.split("\n")
        if lines and len(lines[0].strip()) > 10:
            title = lines[0].strip()
        else:
            # Или первое предложение
            sentences = content.split(".")
            title = (
                sentences[0].strip()
                if sentences and len(sentences[0].strip()) > 10
                else content[:100].strip()
            )

        # Добавляем автора
        if item.author:
            title = f"{item.author}: {title}"

        # Добавляем тикеры
        if len(tickers := self.extract_tickers(item)) > 0:
            tickers_str = ", ".join(tickers[:2])  # Максимум 2 тикера в заголовок
            title = f"{title} [{tickers_str}]"

        return self.prepare_title_for_db(title, max_length)

    def generate_content_hash(self, content: str) -> str:
        """
        Генерация хэша контента для дедупликации.

        Args:
            content: Контент поста

        Returns:
            SHA256 хэш
        """
        import hashlib

        return hashlib.sha256(content.encode("utf-8")).hexdigest()
