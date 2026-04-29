"""
Тесты для репозиториев с упрощёнными моками.
"""

from datetime import datetime
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.core.models import ArticleForDB, ProcessingStats


def _mock_pool_manager(mock_fetch_result=None, mock_fetchrow_result=None):
    """Хелпер для мока DatabasePoolManager.connection() как async context manager."""
    mock_conn = AsyncMock()
    if mock_fetch_result is not None:
        mock_conn.fetch = AsyncMock(return_value=mock_fetch_result)
    if mock_fetchrow_result is not None:
        mock_conn.fetchrow = AsyncMock(return_value=mock_fetchrow_result)
    mock_conn.executemany = AsyncMock(return_value=None)
    mock_conn.execute = AsyncMock(return_value="UPDATE 1")

    # connection() — возвращает async context manager (НЕ корутину!)
    mock_cm = AsyncMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_cm.__aexit__ = AsyncMock(return_value=None)

    # Важно: используем Mock, не AsyncMock, чтобы возвращать mock_cm напрямую
    mock_connection_method = Mock(return_value=mock_cm)

    return mock_connection_method, mock_conn


class TestArticleRepository:
    """Тесты ArticleRepository."""

    @pytest.mark.asyncio
    async def test_save_batch_empty(self):
        """Сохранение пустого списка."""
        from src.database.repositories.article_repository import ArticleRepository

        repo = ArticleRepository()
        stats = await repo.save_batch([])

        assert stats.total_rows == 0
        assert stats.saved == 0

    @pytest.mark.asyncio
    async def test_save_batch_with_existing_urls(self):
        """Сохранение с уже существующими URL — новый URL должен сохраниться."""
        from src.database.repositories.article_repository import ArticleRepository

        mock_connection_method, mock_conn = _mock_pool_manager(
            mock_fetch_result=[{"url": "https://test.com/existing"}]
        )

        with patch("src.database.repositories.article_repository.DatabasePoolManager") as MockPool:
            MockPool.connection = mock_connection_method

            repo = ArticleRepository()

            article = ArticleForDB(
                source_id=1,
                original_id="test_123",
                url="https://test.com/new",
                raw_title="Test",
                raw_text="Test content",
                published_at=datetime(2026, 1, 17, 19, 9),
            )

            stats = await repo.save_batch([article])

            assert stats.total_rows == 1
            assert stats.saved == 1  # Новый URL сохранён
            assert stats.skipped == 0
            mock_conn.executemany.assert_called_once()

    @pytest.mark.asyncio
    async def test_save_batch_all_existing(self):
        """Все URL уже существуют — ничего не сохраняется."""
        from src.database.repositories.article_repository import ArticleRepository

        mock_connection_method, mock_conn = _mock_pool_manager(
            mock_fetch_result=[{"url": "https://test.com/existing"}]
        )

        with patch("src.database.repositories.article_repository.DatabasePoolManager") as MockPool:
            MockPool.connection = mock_connection_method

            repo = ArticleRepository()

            article = ArticleForDB(
                source_id=1,
                original_id="test_123",
                url="https://test.com/existing",  # Уже есть в БД
                raw_title="Test",
                raw_text="Test content",
                published_at=datetime(2026, 1, 17, 19, 9),
            )

            stats = await repo.save_batch([article])

            assert stats.total_rows == 1
            assert stats.saved == 0  # Не сохранено
            assert stats.skipped == 1  # Пропущено
            mock_conn.executemany.assert_not_called()

    @pytest.mark.asyncio
    async def test_get_existing_urls(self):
        """Получение существующих URL."""
        from src.database.repositories.article_repository import ArticleRepository

        mock_connection_method, mock_conn = _mock_pool_manager(
            mock_fetch_result=[
                {"url": "https://test1.com"},
                {"url": "https://test2.com"},
            ]
        )

        with patch("src.database.repositories.article_repository.DatabasePoolManager") as MockPool:
            MockPool.connection = mock_connection_method

            repo = ArticleRepository()
            urls = await repo.get_existing_urls(source_id=1)

            assert len(urls) == 2
            assert "https://test1.com" in urls
            assert "https://test2.com" in urls
            mock_conn.fetch.assert_called_once()


class TestSummaryRepository:
    """Тесты SummaryRepository."""

    @pytest.mark.asyncio
    async def test_save_summary(self):
        """Сохранение суммаризации."""
        from src.database.repositories.summary_repository import SummaryRepository

        mock_connection_method, mock_conn = _mock_pool_manager(mock_fetchrow_result={"id": 123})

        with patch("src.database.repositories.summary_repository.DatabasePoolManager") as MockPool:
            MockPool.connection = mock_connection_method

            summary_id = await SummaryRepository.save(
                period_start=datetime(2026, 1, 17, 0, 0),
                period_end=datetime(2026, 1, 17, 1, 0),
                period_type="hour",
                content={"summary": "Test"},
            )

            assert summary_id == 123
            mock_conn.fetchrow.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_last_summary(self):
        """Получение последней суммаризации."""
        from src.database.repositories.summary_repository import SummaryRepository

        mock_connection_method, mock_conn = _mock_pool_manager(
            mock_fetchrow_result={
                "id": 1,
                "period_start": datetime(2026, 1, 17, 0, 0),
                "period_end": datetime(2026, 1, 17, 1, 0),
                "content": '{"summary": "Test"}',
            }
        )

        with patch("src.database.repositories.summary_repository.DatabasePoolManager") as MockPool:
            MockPool.connection = mock_connection_method

            result = await SummaryRepository.get_last("hour")

            assert result is not None
            assert result["id"] == 1
            assert result["content"]["summary"] == "Test"

    @pytest.mark.asyncio
    async def test_get_last_summary_empty(self):
        """Получение последней суммаризации когда нет данных."""
        from src.database.repositories.summary_repository import SummaryRepository

        mock_connection_method, mock_conn = _mock_pool_manager(mock_fetchrow_result=None)

        with patch("src.database.repositories.summary_repository.DatabasePoolManager") as MockPool:
            MockPool.connection = mock_connection_method

            result = await SummaryRepository.get_last("hour")

            assert result is None
