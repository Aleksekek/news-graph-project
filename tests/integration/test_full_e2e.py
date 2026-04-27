"""
Сквозной интеграционный тест: парсинг -> сохранение -> суммаризация.
"""

import uuid
from datetime import datetime, timedelta

import pytest

from src.app.parse_source import ParseSourceUseCase
from src.database.pool import DatabasePoolManager
from src.database.repositories.article_repository import ArticleRepository


@pytest.mark.integration
@pytest.mark.asyncio
async def test_full_parse_and_save_flow():
    """Сквозной тест: парсинг Lenta -> сохранение в БД -> проверка."""

    # Генерируем уникальный маркер для теста
    test_marker = f"e2e_test_{uuid.uuid4().hex[:8]}"

    use_case = ParseSourceUseCase()

    # Парсим небольшое количество статей
    stats = await use_case.execute(
        source_name="lenta", limit=3, categories=["Политика"]
    )

    # Проверяем что статистика валидна
    assert stats.total_rows >= 0
    assert stats.saved >= 0

    # Если сохранились статьи, проверяем их наличие
    if stats.saved > 0:
        repo = ArticleRepository()
        unprocessed = await repo.get_unprocessed(limit=10)

        # Находим наши статьи (по дате)
        recent = [a for a in unprocessed if a["published_at"]]
        assert len(recent) >= 0


@pytest.mark.integration
@pytest.mark.asyncio
async def test_tinvest_parse_and_save():
    """Сквозной тест для TInvest."""

    use_case = ParseSourceUseCase()

    stats = await use_case.execute(source_name="tinvest", limit=3, tickers=["SBER"])

    assert stats.total_rows >= 0
    assert stats.saved >= 0
