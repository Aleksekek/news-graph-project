"""
Тесты для репозитория статей.
Запуск: pytest tests/unit/test_article_repository.py -v
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.database.repositories.article_repository import ArticleRepository


class TestArticleRepository:
    """Тесты для ArticleRepository."""

    @pytest.fixture
    def repo(self):
        """Фикстура репозитория."""
        return ArticleRepository()

    @pytest.mark.asyncio
    async def test_get_hourly_stats_24h_success(self, repo):
        """Тест получения почасовой статистики."""
        with patch("src.database.repositories.article_repository.DatabasePoolManager") as mock_pool:
            mock_conn = AsyncMock()
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            # Мокаем результат запроса
            now = datetime.now(timezone(timedelta(hours=3)))
            mock_conn.fetch = AsyncMock(
                return_value=[
                    {"hour_start": now - timedelta(hours=3), "count": 10},
                    {"hour_start": now - timedelta(hours=2), "count": 20},
                    {"hour_start": now - timedelta(hours=1), "count": 15},
                ]
            )

            result = await repo.get_hourly_stats_24h()

            # Проверяем, что результат содержит 24 элемента (все часы)
            assert len(result) == 24

            # Проверяем, что запрос был выполнен
            mock_conn.fetch.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_hourly_stats_24h_empty(self, repo):
        """Тест получения пустой статистики."""
        with patch("src.database.repositories.article_repository.DatabasePoolManager") as mock_pool:
            mock_conn = AsyncMock()
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            mock_conn.fetch = AsyncMock(return_value=[])

            result = await repo.get_hourly_stats_24h()

            # Должен быть полный набор из 24 часов с нулями
            assert len(result) == 24
            assert all(count == 0 for _, count in result)

    @pytest.mark.asyncio
    async def test_get_hourly_stats_24h_error(self, repo):
        """Тест ошибки при получении статистики."""
        with patch("src.database.repositories.article_repository.DatabasePoolManager") as mock_pool:
            mock_conn = AsyncMock()
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            mock_conn.fetch = AsyncMock(side_effect=Exception("DB Error"))

            result = await repo.get_hourly_stats_24h()

            # При ошибке возвращаем пустой список
            assert result == []

    @pytest.mark.asyncio
    async def test_search_with_urls(self, repo):
        """Тест поиска с URL."""
        with patch("src.database.repositories.article_repository.DatabasePoolManager") as mock_pool:
            mock_conn = AsyncMock()
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            mock_conn.fetch = AsyncMock(
                return_value=[
                    {"raw_title": "Новость 1", "raw_text": "Текст", "published_at": datetime.now(), "url": "http://example.com/1"}
                ]
            )

            result = await repo.search("тест", limit=5, with_urls=True)

            # Проверяем, что в SQL есть поле url
            sql_call = mock_conn.fetch.call_args[0][0]
            assert "url" in sql_call

    @pytest.mark.asyncio
    async def test_search_without_urls(self, repo):
        """Тест поиска без URL."""
        with patch("src.database.repositories.article_repository.DatabasePoolManager") as mock_pool:
            mock_conn = AsyncMock()
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            mock_conn.fetch = AsyncMock(return_value=[])

            result = await repo.search("тест", limit=5, with_urls=False)

            # Проверяем, что в SQL нет поля url
            sql_call = mock_conn.fetch.call_args[0][0]
            assert "url" not in sql_call or "url" in sql_call is False


if __name__ == "__main__":
    pytest.main([__file__, "-v"])