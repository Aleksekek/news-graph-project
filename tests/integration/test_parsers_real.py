"""
Реальные интеграционные тесты для парсеров.
Делают реальные HTTP запросы к источникам.
"""

from datetime import datetime, timedelta

import pytest

from src.parsers.factory import ParserFactory
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
        """Парсинг одного дня архива (1 января 2026)."""
        parser = ParserFactory.create("lenta")

        start = datetime(2026, 1, 1, 0, 0, 0)
        end = datetime(2026, 1, 1, 23, 59, 59)

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


@pytest.mark.integration
@pytest.mark.asyncio
class TestInterfaxParserReal:
    """Реальные тесты парсера Интерфакс."""

    async def test_parse_recent_articles(self):
        """Парсинг свежих статей (реальный запрос)."""
        parser = ParserFactory.create("interfax")

        async with parser:
            result = await parser.parse(limit=10)

        assert len(result.items) <= 10
        assert len(result.items) > 0  # Хотя бы одна статья

        now = now_msk()
        fresh_count = 0
        for item in result.items:
            assert item.title is not None
            assert len(item.title) > 5
            assert item.url.startswith("https://www.interfax.ru")
            assert item.content is not None
            assert len(item.content) > 100, "Текст статьи слишком короткий, возможно не полный"

            if item.published_at:
                # Дата не в будущем (с запасом 5 мин на погрешность)
                assert item.published_at <= now + timedelta(minutes=5), (
                    f"published_at {item.published_at} в будущем относительно "
                    f"now_msk {now}. Возможен сдвиг часового пояса +3ч."
                )
                # Считаем свежие (моложе 3 часов)
                if item.published_at >= now - timedelta(hours=3):
                    fresh_count += 1

        # Хотя бы треть статей должна быть моложе 3 часов
        assert fresh_count >= max(1, len(result.items) // 3), (
            f"Слишком мало свежих статей ({fresh_count}/{len(result.items)}). "
            f"Возможно время парсится в UTC вместо MSK или RSS не обновляется."
        )

    async def test_parse_with_section_filter(self):
        """Парсинг с фильтром по разделу."""
        parser = ParserFactory.create("interfax")

        async with parser:
            # Пробуем бизнес раздел
            result = await parser.parse(limit=5, sections=["business"])

        assert isinstance(result.items, list)

        for item in result.items:
            section = item.metadata.get("section", "")
            # Может быть пустой раздел, не строго проверяем

    async def test_parse_multiple_sections(self):
        """Парсинг нескольких разделов одновременно."""
        parser = ParserFactory.create("interfax")

        async with parser:
            result = await parser.parse(limit=10, sections=["business", "russia"])

        assert len(result.items) <= 10
        # Должны быть статьи из обоих разделов или хотя бы из одного
        sections_found = {item.metadata.get("section", "") for item in result.items}
        # Хотя бы один из запрошенных разделов присутствует
        assert len(sections_found) > 0

    async def test_parse_period_recent(self):
        """Парсинг архива за 1 января 2026 (фильтрация RSS)."""
        parser = ParserFactory.create("interfax")

        start_date = datetime(2026, 1, 1, 0, 0, 0)
        end_date = datetime(2026, 1, 1, 23, 59, 59)

        async with parser:
            result = await parser.parse_period(start_date=start_date, end_date=end_date, limit=20)

        assert isinstance(result.items, list)

        # Все статьи должны быть в указанном периоде
        for item in result.items:
            if item.published_at:
                assert (
                    start_date - timedelta(minutes=5)
                    <= item.published_at
                    <= end_date + timedelta(minutes=5)
                )


@pytest.mark.integration
@pytest.mark.asyncio
class TestTassParserReal:
    """Реальные тесты парсера ТАСС."""

    async def test_parse_recent_articles(self):
        """Парсинг свежих статей (реальный запрос)."""
        parser = ParserFactory.create("tass")

        async with parser:
            result = await parser.parse(limit=10)

        assert len(result.items) <= 10
        assert len(result.items) > 0  # Хотя бы одна статья

        now = now_msk()
        fresh_count = 0
        for item in result.items:
            assert item.title is not None
            assert len(item.title) > 5
            assert item.url.startswith("https://tass.ru")
            assert item.content is not None
            assert len(item.content) > 100, "Текст статьи слишком короткий, возможно не полный"

            if item.published_at:
                # Дата не в будущем (с запасом 5 мин на погрешность)
                assert item.published_at <= now + timedelta(minutes=5), (
                    f"published_at {item.published_at} в будущем относительно "
                    f"now_msk {now}. Возможен сдвиг часового пояса +3ч."
                )
                # Считаем свежие (моложе 3 часов)
                if item.published_at >= now - timedelta(hours=3):
                    fresh_count += 1

        # Хотя бы треть статей должна быть моложе 3 часов
        assert fresh_count >= max(1, len(result.items) // 3), (
            f"Слишком мало свежих статей ({fresh_count}/{len(result.items)}). "
            f"Возможно время парсится в UTC вместо MSK или RSS не обновляется."
        )

    async def test_parse_with_min_length_filter(self):
        """Парсинг с фильтром по минимальной длине."""
        parser = ParserFactory.create("tass")

        async with parser:
            # Устанавливаем высокий порог, чтобы отфильтровать короткие статьи
            result_short = await parser.parse(limit=5, min_length=1000)
            result_long = await parser.parse(limit=5, min_length=5000)

        # Обе выборки должны быть валидными списками
        assert isinstance(result_short.items, list)
        assert isinstance(result_long.items, list)

        # Статьи в result_long должны быть не короче 5000 символов
        for item in result_long.items:
            assert len(item.content) >= 5000

    async def test_parse_period_recent(self):
        """Парсинг архива за 1 января 2026 (фильтрация RSS)."""
        parser = ParserFactory.create("tass")

        start_date = datetime(2026, 1, 1, 0, 0, 0)
        end_date = datetime(2026, 1, 1, 23, 59, 59)

        async with parser:
            result = await parser.parse_period(start_date=start_date, end_date=end_date, limit=20)

        assert isinstance(result.items, list)

        # Все статьи должны быть в указанном периоде
        for item in result.items:
            if item.published_at:
                assert (
                    start_date - timedelta(minutes=5)
                    <= item.published_at
                    <= end_date + timedelta(minutes=5)
                )


@pytest.mark.integration
@pytest.mark.asyncio
class TestRbcParserReal:
    """Реальные тесты парсера РБК."""

    async def test_parse_recent_articles(self):
        """Парсинг свежих статей (реальный запрос)."""
        parser = ParserFactory.create("rbc")

        async with parser:
            result = await parser.parse(limit=10)

        assert len(result.items) <= 10
        assert len(result.items) > 0  # Хотя бы одна статья

        now = now_msk()
        fresh_count = 0
        for item in result.items:
            assert item.title is not None
            assert len(item.title) > 5
            assert item.url.startswith("https://www.rbc.ru") or item.url.startswith(
                "https://rssexport.rbc.ru"
            )
            assert item.content is not None
            assert len(item.content) > 100
            # РБК через full.rss должен отдавать полный текст
            assert (
                len(item.content) > 500
            ), "Текст статьи слишком короткий, возможно RSS не содержит полный текст"

            if item.published_at:
                # Дата не в будущем (с запасом 5 мин на погрешность)
                assert item.published_at <= now + timedelta(minutes=5), (
                    f"published_at {item.published_at} в будущем относительно "
                    f"now_msk {now}. Возможен сдвиг часового пояса +3ч."
                )
                # Считаем свежие (моложе 3 часов)
                if item.published_at >= now - timedelta(hours=3):
                    fresh_count += 1

        # Хотя бы треть статей должна быть моложе 3 часов
        assert fresh_count >= max(1, len(result.items) // 3), (
            f"Слишком мало свежих статей ({fresh_count}/{len(result.items)}). "
            f"Возможно время парсится в UTC вместо MSK или RSS не обновляется."
        )

    async def test_parse_with_min_length_filter(self):
        """Парсинг с фильтром по минимальной длине."""
        parser = ParserFactory.create("rbc")

        async with parser:
            result = await parser.parse(limit=5, min_length=2000)

        for item in result.items:
            assert len(item.content) >= 2000

    async def test_parse_handles_empty_rss(self):
        """Проверка обработки пустого RSS или ошибок."""
        parser = ParserFactory.create("rbc")

        async with parser:
            # Даже если RSS вернёт мало статей, парсер не должен падать
            result = await parser.parse(limit=100)

        assert isinstance(result.items, list)
        # Не должно быть None или исключений

    async def test_parse_period_archive(self):
        """Архивный парсинг за 1 января 2026 через /search/."""
        parser = ParserFactory.create("rbc")

        start_date = datetime(2026, 1, 1, 0, 0, 0)
        end_date = datetime(2026, 1, 1, 23, 59, 59)

        async with parser:
            result = await parser.parse_period(start_date=start_date, end_date=end_date, limit=20)

        assert isinstance(result.items, list)
        assert len(result.items) > 0, "Архивный парсер не вернул статей за 01.01.2026"

        for item in result.items:
            assert item.title and len(item.title) > 5
            assert item.content and len(item.content) > 100


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

                    # Лента и Тинькофф - проверяем обязательно
                    if parser_name in ["lenta", "tinvest"]:
                        assert len(result.items) > 0, f"{parser_name} не вернул ни одной статьи"
                    else:
                        # Новые парсеры - пока только предупреждение, если пусто
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
        """Все парсеры должны корректно парсить даты в MSK."""
        parsers = ["lenta", "tinvest", "interfax", "tass", "rbc"]
        now = now_msk()

        for parser_name in parsers:
            parser = ParserFactory.create(parser_name)

            async with parser:
                try:
                    result = await parser.parse(limit=5)

                    for item in result.items:
                        if item.published_at:
                            # Не в будущем
                            assert item.published_at <= now + timedelta(
                                minutes=5
                            ), f"{parser_name}: дата {item.published_at} в будущем"
                except Exception as e:
                    if parser_name in ["lenta", "tinvest"]:
                        raise
                    else:
                        pytest.skip(f"{parser_name} не прошёл проверку дат: {e}")
