"""
Реальные интеграционные тесты для репозиториев.
Используют существующую БД, но с временными данными.
"""

import uuid
from datetime import datetime, timedelta

import pytest

from src.core.models import ArticleForDB
from src.database.pool import DatabasePoolManager
from src.database.repositories.article_repository import ArticleRepository
from src.database.repositories.summary_repository import SummaryRepository


@pytest.mark.skip(reason="Проблемы с event loop на Windows, требуют доработки")
@pytest.mark.integration
@pytest.mark.asyncio
class TestArticleRepositoryReal:
    """Реальные тесты ArticleRepository с БД."""

    @pytest.fixture
    def unique_id(self):
        """Генератор уникального ID для тестов."""
        return f"test_{uuid.uuid4().hex[:16]}"

    @pytest.fixture
    async def cleanup_articles(self):
        """Очистка тестовых статей после теста."""
        test_urls = []
        yield test_urls
        # Очищаем только созданные в тесте URL
        if test_urls:
            async with DatabasePoolManager.connection() as conn:
                await conn.execute(
                    "DELETE FROM raw_articles WHERE url = ANY($1)", test_urls
                )

    async def test_save_and_retrieve_article(self, unique_id, cleanup_articles):
        """Сохранение и получение статьи."""
        repo = ArticleRepository()

        test_url = f"https://test.com/{unique_id}"
        cleanup_articles.append(test_url)

        article = ArticleForDB(
            source_id=2,  # lenta
            original_id=unique_id,
            url=test_url,
            raw_title="Тестовая статья",
            raw_text="Это тестовое содержание статьи для проверки сохранения в БД. "
            * 10,
            published_at=datetime.now(),
            author="Test Author",
            language="ru",
            status="raw",
        )

        # Сохраняем
        stats = await repo.save_batch([article])
        assert stats.saved == 1
        assert stats.total_rows == 1

        # Проверяем что сохранилось
        urls = await repo.get_existing_urls(source_id=2)
        assert test_url in urls

    async def test_save_duplicate_article(self, unique_id, cleanup_articles):
        """Сохранение дубликата (должен пропустить)."""
        repo = ArticleRepository()

        test_url = f"https://test.com/{unique_id}"
        cleanup_articles.append(test_url)

        article = ArticleForDB(
            source_id=2,
            original_id=unique_id,
            url=test_url,
            raw_title="Test",
            raw_text="Test content",
            published_at=datetime.now(),
        )

        # Первое сохранение
        stats1 = await repo.save_batch([article])
        assert stats1.saved == 1

        # Второе сохранение (дубликат)
        stats2 = await repo.save_batch([article])
        assert stats2.saved == 0
        assert stats2.skipped == 1

    async def test_get_unprocessed_articles(self, unique_id, cleanup_articles):
        """Получение необработанных статей."""
        repo = ArticleRepository()

        test_url = f"https://test.com/{unique_id}"
        cleanup_articles.append(test_url)

        article = ArticleForDB(
            source_id=2,
            original_id=unique_id,
            url=test_url,
            raw_title="Test for unprocessed",
            raw_text="Content for unprocessed test",
            published_at=datetime.now(),
            status="raw",
        )

        await repo.save_batch([article])

        # Получаем необработанные
        unprocessed = await repo.get_unprocessed(limit=10)

        # Проверяем что наша статья там
        found = any(a["original_id"] == unique_id for a in unprocessed)
        assert found

    async def test_mark_processed(self, unique_id, cleanup_articles):
        """Пометка статьи как обработанной."""
        repo = ArticleRepository()

        test_url = f"https://test.com/{unique_id}"
        cleanup_articles.append(test_url)

        article = ArticleForDB(
            source_id=2,
            original_id=unique_id,
            url=test_url,
            raw_title="Test for marking",
            raw_text="Content for marking",
            published_at=datetime.now(),
            status="raw",
        )

        stats = await repo.save_batch([article])
        assert stats.saved == 1

        # Получаем ID статьи
        unprocessed = await repo.get_unprocessed(limit=10)
        article_id = None
        for a in unprocessed:
            if a["original_id"] == unique_id:
                article_id = a["id"]
                break

        assert article_id is not None

        # Помечаем как обработанную
        result = await repo.mark_processed(article_id)
        assert result is True

        # Проверяем что больше не в raw
        unprocessed_again = await repo.get_unprocessed(limit=10)
        found = any(a["id"] == article_id for a in unprocessed_again)
        assert not found


@pytest.mark.skip(reason="Проблемы с event loop на Windows, требуют доработки")
@pytest.mark.integration
@pytest.mark.asyncio
class TestSummaryRepositoryReal:
    """Реальные тесты SummaryRepository с БД."""

    @pytest.fixture
    def unique_date(self):
        """Уникальная дата для теста (текущий час + смещение)."""
        now = datetime.now()
        # Используем текущий час + уникальное смещение
        return now.replace(minute=0, second=0, microsecond=0)

    async def test_save_and_get_summary(self, unique_date):
        """Сохранение и получение суммаризации."""
        repo = SummaryRepository()

        period_start = unique_date
        period_end = period_start + timedelta(hours=1)

        content = {
            "topics": ["Тест", "Интеграция"],
            "summary": "Тестовая суммаризация",
            "trend": "Тестовый тренд",
            "important_events": ["Тестовое событие"],
        }

        # Сохраняем с уникальным периодом
        summary_id = await repo.save(
            period_start=period_start,
            period_end=period_end,
            period_type="test_hour",
            content=content,
            model_used="test_model",
            prompt_tokens=100,
            completion_tokens=50,
            cost_usd=0.001,
        )

        # ID должен быть > 0
        assert summary_id > 0

        # Получаем за период
        results = await repo.get_for_period(
            start=period_start, end=period_end, period_type="test_hour"
        )

        assert len(results) >= 1
        assert results[0]["id"] == summary_id

        # Получаем последнюю
        last = await repo.get_last("test_hour")
        assert last is not None

        # Очищаем тестовые данные
        async with DatabasePoolManager.connection() as conn:
            await conn.execute(
                "DELETE FROM summarizations WHERE period_type = 'test_hour'"
            )
