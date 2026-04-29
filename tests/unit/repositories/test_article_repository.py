"""
Тесты для репозитория статей.
Запуск: pytest tests/unit/test_article_repository.py -v
"""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, Mock, patch
import pytest

from src.database.repositories.article_repository import ArticleRepository
from src.utils.datetime_utils import now_msk_aware


class TestArticleRepository:
    """Тесты для ArticleRepository."""

    @pytest.fixture
    def repo(self):
        """Фикстура репозитория."""
        return ArticleRepository()

    @pytest.mark.asyncio
    async def test_get_hourly_stats_24h_success(self, repo):
        """Тест получения почасовой статистики (23 полных часа + текущий)."""
        with patch("src.database.repositories.article_repository.DatabasePoolManager") as mock_pool:
            mock_conn = AsyncMock()
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            from src.utils.datetime_utils import now_msk
            from unittest.mock import patch as mock_patch

            fixed_now = datetime(2024, 1, 15, 11, 30, 0)  # 11:30
            current_hour_start = fixed_now.replace(minute=0, second=0, microsecond=0)
            twenty_four_hours_ago = current_hour_start - timedelta(hours=23)

            with mock_patch(
                "src.database.repositories.article_repository.now_msk", return_value=fixed_now
            ):
                # Мокаем результат запроса
                mock_conn.fetch = AsyncMock(return_value=[])

                result = await repo.get_hourly_stats_24h()

                # Проверяем, что результат содержит 24 элемента
                assert len(result) == 24

                # Проверяем последовательность времени
                for i in range(24):
                    expected_time = twenty_four_hours_ago + timedelta(hours=i)
                    assert result[i][0] == expected_time

                # Проверяем, что fetch был вызван ровно 1 раз
                assert mock_conn.fetch.call_count == 1

                # Проверяем аргументы вызова (для SQL с параметрами)
                # Если в функции используется параметризованный запрос с $1
                args, kwargs = mock_conn.fetch.call_args
                if len(args) > 1:  # если есть параметры
                    assert args[1] == twenty_four_hours_ago

    @pytest.mark.asyncio
    async def test_get_hourly_stats_24h_current_hour_only(self, repo):
        """Тест: данные только за текущий час."""
        with patch("src.database.repositories.article_repository.DatabasePoolManager") as mock_pool:
            mock_conn = AsyncMock()
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            from src.utils.datetime_utils import now_msk
            from unittest.mock import patch as mock_patch

            fixed_now = datetime(2024, 1, 15, 11, 30, 0)
            current_hour_start = fixed_now.replace(minute=0, second=0, microsecond=0)
            twenty_four_hours_ago = current_hour_start - timedelta(hours=23)

            with mock_patch(
                "src.database.repositories.article_repository.now_msk", return_value=fixed_now
            ):
                # Только текущий час имеет данные
                mock_conn.fetch = AsyncMock(
                    return_value=[
                        {"hour_start": current_hour_start, "count": 5},
                    ]
                )

                result = await repo.get_hourly_stats_24h()

                assert len(result) == 24
                # Последний элемент (индекс 23) - текущий час
                assert result[23][0] == current_hour_start
                assert result[23][1] == 5

                # Все остальные часы должны быть с нулями
                for i in range(23):
                    assert result[i][1] == 0

    @pytest.mark.asyncio
    async def test_get_hourly_stats_24h_timeline(self, repo):
        """Тест корректности временной линии."""
        with patch("src.database.repositories.article_repository.DatabasePoolManager") as mock_pool:
            mock_conn = AsyncMock()
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            from src.utils.datetime_utils import now_msk
            from unittest.mock import patch as mock_patch

            fixed_now = datetime(2024, 1, 15, 11, 30, 0)
            current_hour_start = fixed_now.replace(minute=0, second=0, microsecond=0)
            twenty_four_hours_ago = current_hour_start - timedelta(hours=23)

            with mock_patch(
                "src.database.repositories.article_repository.now_msk", return_value=fixed_now
            ):
                mock_conn.fetch = AsyncMock(return_value=[])

                result = await repo.get_hourly_stats_24h()

                # Проверяем, что результат начинается с 23 часов назад от текущего часа
                assert result[0][0] == twenty_four_hours_ago

                # Проверяем, что каждый следующий час +1
                for i in range(1, 24):
                    assert result[i][0] == result[i - 1][0] + timedelta(hours=1)

                # Проверяем, что последний час - это текущий час
                assert result[23][0] == current_hour_start

    @pytest.mark.asyncio
    async def test_search_with_urls(self, repo):
        """Тест поиска с URL."""
        with patch("src.database.repositories.article_repository.DatabasePoolManager") as mock_pool:
            mock_conn = AsyncMock()
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            mock_conn.fetch = AsyncMock(
                return_value=[
                    {
                        "raw_title": "Новость 1",
                        "raw_text": "Текст",
                        "published_at": datetime.now(),
                        "url": "http://example.com/1",
                    }
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
