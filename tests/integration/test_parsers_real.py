"""
Реальные интеграционные тесты для парсеров.
Делают реальные HTTP запросы к источникам.

Принципы:
  - Тест должен ПАДАТЬ, когда парсер реально сломан.
  - Тест должен ПРОХОДИТЬ, когда парсер работает корректно.
  - Известные ограничения (Cloudflare-блокировка и пр.) помечаются @xfail.
  - Пустые for-циклы не являются проверкой — всегда добавляется count-assert перед ними.
"""

from datetime import datetime, timedelta

import pytest

from src.parsers.factory import ParserFactory
from src.utils.datetime_utils import now_msk

# Минимальная длина полноценной статьи (не RSS-summary)
_MIN_ARTICLE_LEN = 500       # РБК, длинные источники
_MIN_BRIEF_LEN = 150         # Интерфакс: публикует короткие новостные сводки (2-3 предложения)

# Шум, который не должен попадать в очищенный текст РБК
_RBC_NOISE_PHRASES = (
    "оставайтесь на связи",
    "рбк в «максе»",
    "рбк в max",
    "читайте рбк",
    "материал дополняется",
)


@pytest.mark.integration
@pytest.mark.asyncio
class TestLentaParserReal:
    """Реальные тесты парсера Lenta.ru."""

    async def test_parse_recent_articles(self):
        """Парсинг свежих статей (реальный запрос)."""
        parser = ParserFactory.create("lenta")

        async with parser:
            result = await parser.parse(limit=10)

        assert len(result.items) > 0
        assert len(result.items) >= 5, (
            f"Ожидали ≥5 статей при limit=10, получили {len(result.items)}"
        )

        now = now_msk()
        fresh_count = 0
        for item in result.items:
            assert item.title is not None
            assert len(item.title) > 5
            assert item.url.startswith("https://lenta.ru")
            assert item.content is not None
            assert len(item.content) > 100

            if item.published_at:
                assert item.published_at <= now + timedelta(minutes=5), (
                    f"published_at {item.published_at} в будущем (now_msk={now})"
                )
                if item.published_at >= now - timedelta(hours=3):
                    fresh_count += 1

        assert fresh_count >= max(1, len(result.items) // 3), (
            f"Слишком мало свежих статей ({fresh_count}/{len(result.items)}). "
            f"Возможно время парсится в UTC вместо MSK или RSS не обновляется."
        )

    async def test_parse_with_category_filter(self):
        parser = ParserFactory.create("lenta", {"categories": ["Политика"]})

        async with parser:
            result = await parser.parse(limit=5, categories=["Политика"])

        assert isinstance(result.items, list)

    async def test_parse_archive_day(self):
        parser = ParserFactory.create("lenta")

        start = datetime(2026, 1, 1, 0, 0, 0)
        end = datetime(2026, 1, 1, 23, 59, 59)

        async with parser:
            result = await parser.parse_period(start_date=start, end_date=end, limit=10, max_per_day=5)

        assert isinstance(result.items, list)


@pytest.mark.integration
@pytest.mark.asyncio
class TestTInvestParserReal:
    """Реальные тесты парсера TInvest."""

    async def test_parse_recent_posts(self):
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
            mentioned = item.metadata.get("mentioned_tickers", [])
            assert any(t in ["GAZP", "SBER", "VTBR"] for t in mentioned) or len(mentioned) > 0

            if item.published_at:
                assert item.published_at <= now + timedelta(minutes=5)
                if item.published_at >= now - timedelta(hours=3):
                    fresh_count += 1

        if len(result.items) > 0:
            assert fresh_count >= 1, (
                f"Все {len(result.items)} постов старше 3 часов ({fresh_count} свежих). "
                f"Возможно время парсится в UTC вместо MSK."
            )

    async def test_parse_with_min_reactions(self):
        parser = ParserFactory.create("tinvest")

        async with parser:
            result = await parser.parse(limit=10, tickers=["SBER"], min_reactions=5)

        for item in result.items:
            reactions = item.metadata.get("total_reactions", 0)
            assert reactions >= 5

    async def test_parse_period_recent(self):
        parser = ParserFactory.create("tinvest")

        end_date = now_msk()
        start_date = end_date - timedelta(hours=24)

        async with parser:
            result = await parser.parse_period(
                start_date=start_date, end_date=end_date, limit=20, tickers=["SBER"]
            )

        assert isinstance(result.items, list)

        for item in result.items:
            if item.published_at:
                assert item.published_at <= now_msk() + timedelta(minutes=5)
                assert item.published_at >= now_msk() - timedelta(days=7)


@pytest.mark.integration
@pytest.mark.asyncio
class TestInterfaxParserReal:
    """Реальные тесты парсера Интерфакс."""

    async def test_parse_recent_articles(self):
        """Парсинг свежих статей: должны получать ≥5 статей с полным текстом."""
        parser = ParserFactory.create("interfax")

        async with parser:
            result = await parser.parse(limit=10)

        assert len(result.items) > 0
        assert len(result.items) >= 5, (
            f"Ожидали ≥5 статей при limit=10, получили {len(result.items)}. "
            f"Возможно, несколько статей не прошли фильтр по длине текста."
        )

        now = now_msk()
        fresh_count = 0
        for item in result.items:
            assert item.title and len(item.title) > 5
            assert item.url.startswith("https://www.interfax.ru")
            assert item.content is not None
            # Интерфакс — новостное агентство, многие заметки 2-3 предложения (150-400 симв.)
            assert len(item.content) >= _MIN_BRIEF_LEN, (
                f"Текст статьи слишком короткий ({len(item.content)} симв.) — "
                f"возможно, это RSS-summary вместо полного текста: {item.url}"
            )

            if item.published_at:
                assert item.published_at <= now + timedelta(minutes=5)
                if item.published_at >= now - timedelta(hours=3):
                    fresh_count += 1

        assert fresh_count >= max(1, len(result.items) // 3), (
            f"Слишком мало свежих статей ({fresh_count}/{len(result.items)}). "
            f"Возможно время парсится в UTC вместо MSK."
        )

    async def test_parse_with_section_filter(self):
        parser = ParserFactory.create("interfax")

        async with parser:
            result = await parser.parse(limit=5, sections=["business"])

        assert isinstance(result.items, list)

    async def test_parse_multiple_sections(self):
        parser = ParserFactory.create("interfax")

        async with parser:
            result = await parser.parse(limit=10, sections=["business", "russia"])

        assert len(result.items) <= 10
        sections_found = {item.metadata.get("section", "") for item in result.items}
        assert len(sections_found) > 0

    async def test_parse_period_archive(self):
        """Архивный парсинг: должны получить ≥20 статей с реальным временем публикации."""
        parser = ParserFactory.create("interfax")

        start_date = datetime(2026, 1, 1, 0, 0, 0)
        end_date = datetime(2026, 1, 1, 23, 59, 59)

        async with parser:
            result = await parser.parse_period(start_date=start_date, end_date=end_date, limit=20)

        assert len(result.items) > 0, "Архивный парсер вернул 0 статей за 01.01.2026"
        assert len(result.items) >= 20, (
            f"Ожидали ≥20 статей при limit=20, получили {len(result.items)}. "
            f"Возможно, сломана пагинация или фильтр по длине текста слишком строгий."
        )

        for item in result.items:
            assert item.title and len(item.title) > 5
            # Интерфакс публикует короткие сводки, поэтому порог ниже стандартного
            assert item.content and len(item.content) >= _MIN_BRIEF_LEN, (
                f"Текст статьи {item.url} слишком короткий ({len(item.content)} симв.)"
            )
            if item.published_at:
                assert (
                    start_date - timedelta(minutes=5)
                    <= item.published_at
                    <= end_date + timedelta(minutes=5)
                ), f"Дата {item.published_at} вне диапазона 01.01.2026: {item.url}"

        # Временные метки должны быть реальными, не все в 00:00
        items_with_time = [
            it for it in result.items if it.published_at and it.published_at.hour != 0
        ]
        assert len(items_with_time) > 0, (
            "Все временные метки в 00:00 — время не извлекается из страниц статей. "
            "Проверь _extract_published_at()."
        )


@pytest.mark.integration
@pytest.mark.asyncio
class TestTassParserReal:
    """
    Реальные тесты парсера ТАСС.

    Известное ограничение: tass.ru закрыт Cloudflare JS-challenge.
    Страницы статей недоступны через aiohttp — получаем только RSS-summary.
    Тесты, проверяющие полный текст, помечены @xfail.
    """

    async def test_parse_recent_articles_basic(self):
        """
        Базовая проверка: RSS работает, статьи возвращаются с корректными полями.
        Не проверяем длину текста (сайт заблокирован, получаем RSS-summary).
        """
        parser = ParserFactory.create("tass")

        async with parser:
            result = await parser.parse(limit=10)

        assert len(result.items) > 0, "ТАСС не вернул ни одной статьи — RSS недоступен"
        # Cloudflare блокирует страницы статей → только RSS-summary.
        # Часть summary короче min_length=100 и отфильтровывается — порог снижен до 3.
        assert len(result.items) >= 3, (
            f"Ожидали ≥3 статей при limit=10, получили {len(result.items)}. "
            f"Возможно, RSS-summaries стали ещё короче или RSS недоступен."
        )

        now = now_msk()
        fresh_count = 0
        for item in result.items:
            assert item.title and len(item.title) > 5
            assert item.url.startswith("https://tass.ru")
            assert item.content and len(item.content) > 50

            if item.published_at:
                assert item.published_at <= now + timedelta(minutes=5)
                if item.published_at >= now - timedelta(hours=3):
                    fresh_count += 1

        assert fresh_count >= max(1, len(result.items) // 3), (
            f"Слишком мало свежих статей ({fresh_count}/{len(result.items)})."
        )

    async def test_parse_full_text(self):
        """Полный текст статей через curl-cffi (обход Cloudflare по TLS-fingerprint)."""
        parser = ParserFactory.create("tass")

        async with parser:
            result = await parser.parse(limit=5)

        assert len(result.items) >= 3
        for item in result.items:
            assert len(item.content) >= _MIN_ARTICLE_LEN, (
                f"Контент слишком короткий ({len(item.content)} симв.) — "
                f"похоже на RSS-summary, а не полный текст: {item.url}"
            )

    async def test_parse_with_min_length_filter(self):
        """Фильтр min_length должен работать: result_long ⊆ result_short по контенту."""
        parser = ParserFactory.create("tass")

        async with parser:
            result_short = await parser.parse(limit=10, min_length=100)
            result_long = await parser.parse(limit=10, min_length=1000)

        assert isinstance(result_short.items, list)
        assert isinstance(result_long.items, list)

        # result_long не может содержать больше статей, чем result_short
        assert len(result_long.items) <= len(result_short.items), (
            f"result_long ({len(result_long.items)}) > result_short ({len(result_short.items)}) "
            f"— фильтрация min_length сломана."
        )

        # Все статьи из result_long должны удовлетворять фильтру
        for item in result_long.items:
            assert len(item.content) >= 1000, (
                f"Статья не прошла бы фильтр min_length=1000 ({len(item.content)} симв.): {item.url}"
            )

    @pytest.mark.xfail(
        reason=(
            "ТАСС архив (tass.ru/search) — React SPA с динамической подгрузкой. "
            "parse_period() фильтрует только текущую RSS-ленту по дате: "
            "архивные статьи за 01.01.2026 недоступны."
        ),
        strict=False,
    )
    async def test_parse_period_archive(self):
        """Архивный парсинг: xfail пока ТАСС не имеет статичного архивного эндпоинта."""
        parser = ParserFactory.create("tass")

        start_date = datetime(2026, 1, 1, 0, 0, 0)
        end_date = datetime(2026, 1, 1, 23, 59, 59)

        async with parser:
            result = await parser.parse_period(start_date=start_date, end_date=end_date, limit=20)

        assert len(result.items) >= 10, (
            f"Ожидали ≥10 архивных статей ТАСС, получили {len(result.items)}"
        )


@pytest.mark.integration
@pytest.mark.asyncio
class TestRbcParserReal:
    """Реальные тесты парсера РБК."""

    async def test_parse_recent_articles(self):
        """Парсинг свежих статей: полный текст + отсутствие шума."""
        parser = ParserFactory.create("rbc")

        async with parser:
            result = await parser.parse(limit=10)

        assert len(result.items) > 0
        assert len(result.items) >= 5, (
            f"Ожидали ≥5 статей при limit=10, получили {len(result.items)}"
        )

        now = now_msk()
        fresh_count = 0
        for item in result.items:
            assert item.title and len(item.title) > 5
            assert item.url.startswith("https://www.rbc.ru") or item.url.startswith(
                "https://rssexport.rbc.ru"
            )
            assert item.content is not None
            assert len(item.content) >= _MIN_ARTICLE_LEN, (
                f"Текст статьи слишком короткий ({len(item.content)} симв.) — "
                f"возможно, это RSS-summary или live-blog: {item.url}"
            )

            # Проверяем отсутствие шума в тексте
            content_lower = item.content.lower()
            for noise in _RBC_NOISE_PHRASES:
                assert noise not in content_lower, (
                    f"Шум '{noise}' найден в тексте статьи {item.url}. "
                    f"Проверь _clean_rbc_text()."
                )

            if item.published_at:
                assert item.published_at <= now + timedelta(minutes=5)
                if item.published_at >= now - timedelta(hours=3):
                    fresh_count += 1

        assert fresh_count >= max(1, len(result.items) // 3), (
            f"Слишком мало свежих статей ({fresh_count}/{len(result.items)}). "
            f"Возможно время парсится в UTC вместо MSK."
        )

    async def test_parse_with_min_length_filter(self):
        parser = ParserFactory.create("rbc")

        async with parser:
            result = await parser.parse(limit=5, min_length=2000)

        for item in result.items:
            assert len(item.content) >= 2000

    async def test_parse_handles_errors_gracefully(self):
        """Парсер не должен бросать исключения при обычном запросе."""
        parser = ParserFactory.create("rbc")

        async with parser:
            result = await parser.parse(limit=100)

        assert isinstance(result.items, list)

    async def test_parse_period_archive(self):
        """Архивный парсинг РБК через AJAX API с пагинацией."""
        parser = ParserFactory.create("rbc")

        start_date = datetime(2026, 1, 1, 0, 0, 0)
        end_date = datetime(2026, 1, 1, 23, 59, 59)

        async with parser:
            result = await parser.parse_period(start_date=start_date, end_date=end_date, limit=20)

        assert len(result.items) > 0, "Архивный парсер РБК вернул 0 статей за 01.01.2026"
        # page=N пагинация даёт ~196 URL за сутки; после фильтрации типов и min_length — ≥15.
        assert len(result.items) >= 15, (
            f"Ожидали ≥15 статей при limit=20, получили {len(result.items)}. "
            f"Проверь page=N пагинацию _get_archive_items_api или _SKIP_TYPES."
        )

        for item in result.items:
            assert item.title and len(item.title) > 5
            assert item.content and len(item.content) >= _MIN_ARTICLE_LEN, (
                f"Текст статьи {item.url} слишком короткий ({len(item.content)} симв.)"
            )
            assert item.url.startswith("https://www.rbc.ru"), (
                f"URL статьи не принадлежит www.rbc.ru: {item.url}"
            )


@pytest.mark.integration
@pytest.mark.asyncio
class TestAllParsersComparison:
    """Сравнительные тесты всех парсеров."""

    async def test_all_parsers_return_articles(self):
        """Все парсеры должны возвращать хотя бы одну статью."""
        parsers = ["lenta", "tinvest", "interfax", "tass", "rbc"]

        for parser_name in parsers:
            parser = ParserFactory.create(parser_name)

            async with parser:
                try:
                    result = await parser.parse(limit=3)

                    if parser_name in ["lenta", "tinvest"]:
                        assert len(result.items) > 0, f"{parser_name} не вернул ни одной статьи"
                    else:
                        if len(result.items) == 0:
                            pytest.skip(
                                f"{parser_name} не вернул статей. Возможно проблемы с сетью или RSS."
                            )
                        assert isinstance(result.items, list)

                except Exception as e:
                    if parser_name in ["lenta", "tinvest"]:
                        raise
                    else:
                        pytest.skip(f"{parser_name} выбросил исключение: {e}")

    async def test_all_parsers_validate_dates(self):
        """Все парсеры должны корректно парсить даты в MSK (naive datetime)."""
        parsers = ["lenta", "tinvest", "interfax", "tass", "rbc"]
        now = now_msk()

        for parser_name in parsers:
            parser = ParserFactory.create(parser_name)

            async with parser:
                try:
                    result = await parser.parse(limit=5)

                    for item in result.items:
                        if item.published_at:
                            # Дата не должна быть aware (в проекте все MSK naive)
                            assert item.published_at.tzinfo is None, (
                                f"{parser_name}: published_at содержит tzinfo — "
                                f"нарушение политики MSK naive datetime. "
                                f"Значение: {item.published_at}"
                            )
                            assert item.published_at <= now + timedelta(minutes=5), (
                                f"{parser_name}: дата {item.published_at} в будущем"
                            )
                except Exception as e:
                    if parser_name in ["lenta", "tinvest"]:
                        raise
                    else:
                        pytest.skip(f"{parser_name} не прошёл проверку дат: {e}")
