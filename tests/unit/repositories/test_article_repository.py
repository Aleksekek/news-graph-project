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
        """Тест получения почасовой статистики."""
        with patch("src.database.repositories.article_repository.DatabasePoolManager") as mock_pool:
            mock_conn = AsyncMock()
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            # Мокаем результат запроса - БД возвращает час (0-23) и количество
            mock_conn.fetch = AsyncMock(
                return_value=[
                    {"hour": 10, "count": 10},  # например, в 10 час утра было 10 статей
                    {"hour": 14, "count": 20},  # в 14 часов - 20 статей
                    {"hour": 22, "count": 15},  # в 22 часа - 15 статей
                ]
            )

            result = await repo.get_hourly_stats_24h()

            # Проверяем, что результат содержит 24 элемента (все часы)
            assert len(result) == 24

            # Проверяем структуру данных
            for hour_start, count in result:
                assert isinstance(hour_start, datetime)
                assert isinstance(count, int)
                # Проверяем, что время - начало часа
                assert hour_start.minute == 0
                assert hour_start.second == 0
                assert hour_start.microsecond == 0

            # Проверяем, что времена идут в хронологическом порядке (по возрастанию)
            times = [item[0] for item in result]
            assert times == sorted(times)

            # Проверяем, что все времена уникальны
            assert len(set(times)) == 24

            # Проверяем, что часы идут по порядку (могут быть циклическими)
            hours = [item[0].hour for item in result]
            # Проверяем, что каждый следующий час либо больше предыдущего,
            # либо меньше (если перешли через полночь)
            for i in range(1, len(hours)):
                diff = hours[i] - hours[i - 1]
                assert diff == 1 or diff == -23  # нормальный переход или через полночь

            # Проверяем, что запрос был выполнен
            mock_conn.fetch.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_hourly_stats_24h_empty(self, repo):
        """Тест получения пустой статистики (нет данных в БД)."""
        with patch("src.database.repositories.article_repository.DatabasePoolManager") as mock_pool:
            mock_conn = AsyncMock()
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            # БД вернула пустой результат
            mock_conn.fetch = AsyncMock(return_value=[])

            result = await repo.get_hourly_stats_24h()

            # Должен быть полный набор из 24 часов с нулями
            assert len(result) == 24
            assert all(count == 0 for _, count in result)

            # Проверяем, что все часы разные и идут по порядку
            hours = set(item[0].hour for item in result)
            assert len(hours) == 24  # Все часы от 0 до 23 должны присутствовать

    @pytest.mark.asyncio
    async def test_get_hourly_stats_24h_partial_data(self, repo):
        """Тест с частичными данными (не для всех часов есть статистика)."""
        with patch("src.database.repositories.article_repository.DatabasePoolManager") as mock_pool:
            mock_conn = AsyncMock()
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            # БД вернула данные только для некоторых часов
            mock_conn.fetch = AsyncMock(
                return_value=[
                    {"hour": 9, "count": 5},
                    {"hour": 15, "count": 12},
                    {"hour": 18, "count": 7},
                ]
            )

            result = await repo.get_hourly_stats_24h()

            # Должно быть 24 часа
            assert len(result) == 24

            # Находим часы с ненулевыми значениями
            non_zero_hours = [(item[0].hour, item[1]) for item in result if item[1] > 0]
            assert len(non_zero_hours) == 3
            assert (9, 5) in non_zero_hours
            assert (15, 12) in non_zero_hours
            assert (18, 7) in non_zero_hours

            # Проверяем, что остальные часы имеют 0
            zero_hours = [item[0].hour for item in result if item[1] == 0]
            assert 0 in zero_hours  # какой-то час точно будет с нулём

    @pytest.mark.asyncio
    async def test_get_hourly_stats_24h_error(self, repo):
        """Тест ошибки при получении статистики."""
        with patch("src.database.repositories.article_repository.DatabasePoolManager") as mock_pool:
            mock_conn = AsyncMock()
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            # Симулируем ошибку БД
            mock_conn.fetch = AsyncMock(side_effect=Exception("DB Error"))

            result = await repo.get_hourly_stats_24h()

            # При ошибке возвращаем пустой список
            assert result == []

    @pytest.mark.asyncio
    async def test_get_hourly_stats_24h_timeline_correctness(self, repo):
        """Тест корректности временной шкалы (24 часа от текущего момента)."""
        with patch("src.database.repositories.article_repository.DatabasePoolManager") as mock_pool:
            mock_conn = AsyncMock()
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            fixed_now = datetime(2024, 1, 15, 14, 30, 0)  # 14:30
            with patch(
                "src.database.repositories.article_repository.now_msk_aware", return_value=fixed_now
            ):
                mock_conn.fetch = AsyncMock(return_value=[])

                result = await repo.get_hourly_stats_24h()

                # Должно быть 24 записи
                assert len(result) == 24

                # Проверяем последовательность часов
                # Первый элемент - самый старый час, последний - самый новый
                expected_hours = []
                for i in range(23, -1, -1):  # от 23 до 0 часов назад
                    hour_dt = fixed_now - timedelta(hours=i)
                    expected_hours.append(hour_dt.replace(minute=0, second=0, microsecond=0))

                for expected, actual in zip(expected_hours, result):
                    assert expected == actual[0]
                    assert actual[1] == 0

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
