"""
Unit-тесты для Lenta парсера.
"""

from datetime import datetime

import pytest
from bs4 import BeautifulSoup

from src.core.models import ParserConfig
from src.parsers.lenta.parser import LentaParser


class TestLentaExtractPublishedTime:
    """Тесты извлечения даты из HTML Lenta.ru."""

    @pytest.fixture
    def parser(self):
        """Фикстура парсера."""
        config = ParserConfig(source_id=2, source_name="lenta")
        return LentaParser(config)

    def test_extract_from_meta_iso(self, parser):
        """Дата из meta tag в ISO формате."""
        html = '<html><head><meta property="article:published_time" content="2026-01-17T19:09:24+03:00"></head></html>'
        soup = BeautifulSoup(html, "html.parser")
        result = parser._extract_published_time(soup)

        assert result is not None
        assert result.tzinfo is None  # naive
        assert result.hour == 19
        assert result.minute == 9
        assert result.day == 17

    def test_extract_from_time_element(self, parser):
        """Дата из элемента <time> с datetime атрибутом."""
        html = '<html><a class="topic-header__time" datetime="2026-01-17T19:09:24+03:00">19:09, 17 января 2026</a></html>'
        soup = BeautifulSoup(html, "html.parser")
        result = parser._extract_published_time(soup)

        assert result is not None
        assert result.hour == 19
        assert result.minute == 9

    def test_extract_from_russian_text(self, parser):
        """Дата из русского текста на странице."""
        html = '<html><span class="topic-header__time">19:09, 17 января 2026</span></html>'
        soup = BeautifulSoup(html, "html.parser")
        result = parser._extract_published_time(soup)

        assert result is not None
        assert result.hour == 19
        assert result.minute == 9
        assert result.day == 17
        assert result.month == 1

    def test_extract_no_date(self, parser):
        """HTML без даты."""
        html = "<html><body><h1>Title</h1></body></html>"
        soup = BeautifulSoup(html, "html.parser")
        result = parser._extract_published_time(soup)
        assert result is None

    def test_extract_date_is_msk_not_utc(self, parser):
        """Проверяем, что время НЕ сдвигается как UTC (Lenta уже в MSK)."""
        # Если бы это было UTC 19:09, то в MSK было бы 22:09
        # Но Lenta отдаёт уже в MSK, так что должно остаться 19:09
        html = '<html><meta property="article:published_time" content="2026-01-17T19:09:24+03:00"></html>'
        soup = BeautifulSoup(html, "html.parser")
        result = parser._extract_published_time(soup)

        assert result is not None
        assert result.hour == 19, (
            f"Lenta время должно оставаться 19:09 (MSK), но получено {result.hour}:{result.minute:02d}. "
            f"Возможно применена лишняя UTC->MSK конвертация."
        )


class TestLentaExtractTitle:
    """Тесты извлечения заголовка."""

    @pytest.fixture
    def parser(self):
        config = ParserConfig(source_id=2, source_name="lenta")
        return LentaParser(config)

    def test_extract_title_h1(self, parser):
        html = '<html><h1 class="topic-body__title">Test Title</h1></html>'
        soup = BeautifulSoup(html, "html.parser")
        assert parser._extract_title(soup) == "Test Title"

    def test_extract_title_fallback(self, parser):
        html = "<html><title>Fallback Title</title></html>"
        soup = BeautifulSoup(html, "html.parser")
        assert parser._extract_title(soup) == "Fallback Title"

    def test_extract_title_empty(self, parser):
        html = "<html></html>"
        soup = BeautifulSoup(html, "html.parser")
        assert parser._extract_title(soup) == "Без заголовка"


class TestLentaToParsedItem:
    """Тесты конвертации в ParsedItem."""

    @pytest.fixture
    def parser(self):
        config = ParserConfig(source_id=2, source_name="lenta")
        return LentaParser(config)

    def test_to_parsed_item(self, parser):
        raw_data = {
            "original_id": "lenta_abc123",
            "url": "https://lenta.ru/news/2026/01/17/test/",
            "title": "Test Title",
            "content": "Test content here",
            "published_at": datetime(2026, 1, 17, 19, 9),
            "author": "Test Author",
            "category": "Политика",
        }

        item = parser.to_parsed_item(raw_data)

        assert item.source_id == 2
        assert item.source_name == "lenta"
        assert item.original_id == "lenta_abc123"
        assert item.published_at == datetime(2026, 1, 17, 19, 9)
        assert item.metadata["category"] == "Политика"
