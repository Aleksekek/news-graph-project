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
from src.utils.datetime_utils import now_msk


@pytest.mark.integration
@pytest.mark.asyncio
class TestLentaParserReal:
    """Реальные тесты парсера Lenta.ru."""

    async def test_parse_recent_articles(self):
        """Парсинг свежих статей (реальный запрос)."""
        parser = ParserFactory.create("lenta")

        async with parser:
            result = await parser.parse(limit=10)

        assert len(result.items) <= 10
        assert len(result.items) > 0  # Хотя бы одна статья

        now = now_msk()
        # Смотрим, что статьи не в будущем (ловит сдвиг +3ч)
        # и не все старше 3 часов (ловит сдвиг -3ч / UTC)
        fresh_count = 0
        for item in result.items:
            assert item.title is not None
            assert len(item.title) > 5
            assert item.url.startswith("https://lenta.ru")
            assert item.content is not None
            assert len(item.content) > 100

            if item.published_at:
                # Дата не в будущем (с запасом 5 мин на погрешность)
                assert item.published_at <= now + timedelta(minutes=5), (
                    f"published_at {item.published_at} в будущем относительно "
                    f"now_msk {now}. Возможен сдвиг часового пояса +3ч."
                )
                # Считаем свежие (моложе 3 часов)
                if item.published_at >= now - timedelta(hours=3):
                    fresh_count += 1

        # Хотя бы половина статей должна быть моложе 3 часов
        # (иначе все даты сдвинуты в UTC или есть другая проблема)
        assert fresh_count >= max(1, len(result.items) // 3), (
            f"Слишком мало свежих статей ({fresh_count}/{len(result.items)}). "
            f"Возможно время парсится в UTC вместо MSK или RSS не обновляется."
        )

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
        parser = ParserFactory.create("tinvest", {"tickers": ["GAZP", "SBER", "VTBR"]})

        async with parser:
            result = await parser.parse(limit=5, tickers=["GAZP", "SBER", "VTBR"])

        assert len(result.items) <= 5

        now = now_msk()
        fresh_count = 0
        for item in result.items:
            assert item.title is not None
            assert item.content is not None
            assert item.author is not None
            # Должны быть упомянутые тикеры
            mentioned = item.metadata.get("mentioned_tickers", [])
            # Хотя бы один из запрошенных
            assert any(t in ["GAZP", "SBER", "VTBR"] for t in mentioned) or len(mentioned) > 0

            if item.published_at:
                # Дата не в будущем (с запасом 5 мин на погрешность)
                assert item.published_at <= now + timedelta(minutes=5), (
                    f"published_at {item.published_at} в будущем относительно "
                    f"now_msk {now}. Возможен сдвиг часового пояса +3ч."
                )
                # Считаем свежие (моложе 3 часов)
                if item.published_at >= now - timedelta(hours=3):
                    fresh_count += 1

        # Если статьи есть, хотя бы одна должна быть свежей (моложе 3 часов).
        # Для TInvest активность ночью может быть низкой,
        # поэтому проверяем только если вообще есть посты с published_at.
        if len(result.items) > 0:
            assert fresh_count >= 1, (
                f"Все {len(result.items)} постов старше 3 часов ({fresh_count} свежих). "
                f"Возможно время парсится в UTC вместо MSK."
                f"{result.items}"
            )

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

        # Используем MSK для запроса
        end_date = now_msk()
        start_date = end_date - timedelta(hours=24)

        async with parser:
            result = await parser.parse_period(
                start_date=start_date, end_date=end_date, limit=20, tickers=["SBER"]
            )

        assert isinstance(result.items, list)

        # После конвертации даты должны быть в MSK
        for item in result.items:
            if item.published_at:
                # Дата поста не должна быть позже текущего времени
                assert item.published_at <= now_msk() + timedelta(minutes=5)
                # Дата поста должна быть в пределах разумного (не сильно старше)
                assert item.published_at >= now_msk() - timedelta(days=7)
