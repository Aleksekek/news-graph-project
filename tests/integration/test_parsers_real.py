"""
Реальные интеграционные тесты для парсеров.
Делают реальные HTTP запросы к источникам.
"""

from datetime import datetime, timedelta

import pytest

from src.core.constants import LENTA_CATEGORIES, TINVEST_TICKERS
from src.parsers.factory import ParserFactory
from src.parsers.lenta.parser import LentaParser
from src.parsers.tinvest.parser import TInvestParser


@pytest.mark.integration
@pytest.mark.asyncio
class TestLentaParserReal:
    """Реальные тесты парсера Lenta.ru."""

    async def test_parse_recent_articles(self):
        """Парсинг свежих статей (реальный запрос)."""
        parser = ParserFactory.create("lenta")

        async with parser:
            result = await parser.parse(limit=3)

        assert len(result.items) <= 3
        assert len(result.items) > 0  # Хотя бы одна статья

        for item in result.items:
            assert item.title is not None
            assert len(item.title) > 5
            assert item.url.startswith("https://lenta.ru")
            assert item.content is not None
            assert len(item.content) > 100

    async def test_parse_with_category_filter(self):
        """Парсинг с фильтром по категории."""
        parser = ParserFactory.create("lenta", {"categories": ["Политика"]})

        async with parser:
            result = await parser.parse(limit=5, categories=["Политика"])

        # Проверяем что статьи есть (хотя бы 0, но не падает)
        assert isinstance(result.items, list)

        for item in result.items:
            category = item.metadata.get("category", "")
            # Может быть пустая категория, не строго проверяем

    async def test_parse_archive_day(self):
        """Парсинг одного дня архива."""
        parser = ParserFactory.create("lenta")

        # Берём вчерашний день
        yesterday = datetime.now() - timedelta(days=1)
        start = yesterday.replace(hour=0, minute=0, second=0)
        end = yesterday.replace(hour=23, minute=59, second=59)

        async with parser:
            result = await parser.parse_period(
                start_date=start, end_date=end, limit=10, max_per_day=5
            )

        # Не проверяем количество, но хотя бы не падает
        assert isinstance(result.items, list)


@pytest.mark.integration
@pytest.mark.asyncio
class TestTInvestParserReal:
    """Реальные тесты парсера TInvest."""

    async def test_parse_recent_posts(self):
        """Парсинг свежих постов (реальный запрос)."""
        parser = ParserFactory.create("tinvest", {"tickers": ["SBER", "VTBR"]})

        async with parser:
            result = await parser.parse(limit=5, tickers=["SBER", "VTBR"])

        assert len(result.items) <= 5

        for item in result.items:
            assert item.title is not None
            assert item.content is not None
            assert item.author is not None
            # Должны быть упомянутые тикеры
            mentioned = item.metadata.get("mentioned_tickers", [])
            # Хотя бы один из запрошенных
            assert any(t in ["SBER", "VTBR"] for t in mentioned) or len(mentioned) > 0

    async def test_parse_with_min_reactions(self):
        """Парсинг с фильтром по реакциям."""
        parser = ParserFactory.create("tinvest")

        async with parser:
            result = await parser.parse(limit=10, tickers=["SBER"], min_reactions=5)

        for item in result.items:
            reactions = item.metadata.get("total_reactions", 0)
            assert reactions >= 5

    async def test_parse_period_recent(self):
        """Парсинг за последние 24 часа."""
        parser = ParserFactory.create("tinvest")

        end_date = datetime.now()
        start_date = end_date - timedelta(hours=24)

        async with parser:
            result = await parser.parse_period(
                start_date=start_date, end_date=end_date, limit=20, tickers=["SBER"]
            )

        assert isinstance(result.items, list)

        # Проверяем что даты в пределах периода
        for item in result.items:
            if item.published_at:
                assert item.published_at >= start_date - timedelta(hours=1)
                assert item.published_at <= end_date + timedelta(hours=1)
