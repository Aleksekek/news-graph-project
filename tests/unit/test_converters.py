"""
Тесты для конвертеров.
"""

from datetime import datetime

import pytest

from src.core.models import ParsedItem
from src.parsers.lenta.converter import LentaConverter
from src.parsers.tinvest.converter import TInvestConverter


class TestLentaConverter:
    """Тесты конвертера Lenta.ru."""

    def setup_method(self):
        self.converter = LentaConverter()

    def test_convert_minimal(self):
        """Конвертация минимального элемента."""
        item = ParsedItem(
            source_id=2,
            source_name="lenta",
            original_id="lenta_abc123",
            url="https://lenta.ru/news/2026/01/17/test/",
            title="Тестовая статья",
            content="Это тестовое содержание статьи. " * 20,
            published_at=datetime(2026, 1, 17, 19, 9),
        )

        article = self.converter.convert(item)

        assert article.source_id == 2
        assert article.original_id == "lenta_abc123"
        assert article.url == "https://lenta.ru/news/2026/01/17/test/"
        assert article.raw_title == "Тестовая статья"
        assert article.status == "raw"

    def test_convert_empty_title(self):
        """Конвертация с пустым заголовком."""
        item = ParsedItem(
            source_id=2,
            source_name="lenta",
            original_id="lenta_abc123",
            url="https://lenta.ru/test/",
            title="",
            content="Some content here",
        )

        article = self.converter.convert(item)

        assert article.raw_title == "Без заголовка"

    def test_convert_long_title(self):
        """Конвертация слишком длинного заголовка."""
        long_title = "A" * 600
        item = ParsedItem(
            source_id=2,
            source_name="lenta",
            original_id="lenta_abc123",
            url="https://lenta.ru/test/",
            title=long_title,
            content="Some content here",
        )

        article = self.converter.convert(item)

        assert len(article.raw_title) <= 503  # 500 + "..."
        assert article.raw_title.endswith("...")


class TestTInvestConverter:
    """Тесты конвертера TInvest."""

    def setup_method(self):
        self.converter = TInvestConverter()

    def test_convert_with_media(self):
        """Конвертация поста с изображениями."""
        item = ParsedItem(
            source_id=1,
            source_name="tinvest",
            original_id="tinvest_abc123",
            url="https://www.tbank.ru/invest/social/profile/user/123",
            title="User: SBER растёт",
            content="Акции Сбера показали рост...",
            published_at=datetime(2026, 1, 17, 19, 9),
            author="user",
            metadata={
                "images": [{"url": "https://image1.jpg", "alt": "Chart"}],
                "mentioned_tickers": ["SBER", "VTBR"],
                "total_reactions": 42,
            },
        )

        article = self.converter.convert(item)

        assert article.source_id == 1
        assert article.media_content is not None
        assert "image1.jpg" in article.media_content
        assert article.meta_info is not None
        assert "SBER" in article.meta_info

    def test_convert_without_media(self):
        """Конвертация поста без изображений."""
        item = ParsedItem(
            source_id=1,
            source_name="tinvest",
            original_id="tinvest_abc123",
            url="https://www.tbank.ru/invest/social/profile/user/123",
            title="User: Просто пост",
            content="Текст без картинок",
            published_at=datetime(2026, 1, 17, 19, 9),
            author="user",
            metadata={"mentioned_tickers": ["SBER"]},
        )

        article = self.converter.convert(item)

        assert article.media_content is None
        assert article.meta_info is not None
