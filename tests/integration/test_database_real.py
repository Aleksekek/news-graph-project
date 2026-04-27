"""
Реальные интеграционные тесты с БД.
Требуют запущенную PostgreSQL для тестов.
"""

from datetime import datetime

import pytest

from src.core.models import ArticleForDB
from src.database.pool import DatabasePoolManager
from src.database.repositories.article_repository import ArticleRepository


@pytest.mark.integration
@pytest.mark.asyncio
async def test_real_database_save_and_retrieve():
    """Реальный тест с БД (требует test database)."""

    # Для реального теста нужно настроить test database
    # Пока пропускаем, если нет БД
    pytest.skip("Requires test database setup")

    # Пример реального теста:
    repo = ArticleRepository()

    article = ArticleForDB(
        source_id=1,
        original_id="test_integration_1",
        url="https://test.com/integration",
        raw_title="Integration Test",
        raw_text="Test content for integration",
        published_at=datetime.now(),
    )

    stats = await repo.save_batch([article])

    assert stats.saved == 1

    # Очистка
    async with DatabasePoolManager.connection() as conn:
        await conn.execute(
            "DELETE FROM raw_articles WHERE original_id = $1", "test_integration_1"
        )

    await DatabasePoolManager.close()
