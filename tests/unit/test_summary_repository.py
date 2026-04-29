"""
Тесты для SummaryRepository.
"""

from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestSummaryRepository:
    """Тесты SummaryRepository."""

    @pytest.mark.asyncio
    async def test_get_for_period_empty(self):
        """Получение суммаризаций за период - пустой результат."""
        from src.database.repositories.summary_repository import SummaryRepository

        # Создаём мок для asyncpg соединения
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])

        # Создаём асинхронный контекстный менеджер
        @asynccontextmanager
        async def mock_connection():
            yield mock_conn

        with patch("src.database.repositories.summary_repository.DatabasePoolManager") as MockPool:
            MockPool.connection = mock_connection

            result = await SummaryRepository.get_for_period(
                start=datetime(2026, 1, 1), end=datetime(2026, 1, 2), period_type="hour"
            )

            assert result == []

    @pytest.mark.asyncio
    async def test_get_for_period_with_aware_datetime(self):
        """Получение суммаризаций с aware datetime (с таймзоной)."""
        from src.database.repositories.summary_repository import SummaryRepository

        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])

        @asynccontextmanager
        async def mock_connection():
            yield mock_conn

        with patch("src.database.repositories.summary_repository.DatabasePoolManager") as MockPool:
            MockPool.connection = mock_connection

            # Aware datetime с таймзоной MSK
            msk_tz = timezone(timedelta(hours=3))
            start = datetime(2026, 4, 28, 0, 0, tzinfo=msk_tz)
            end = datetime(2026, 4, 28, 23, 59, tzinfo=msk_tz)

            result = await SummaryRepository.get_for_period(start, end, "hour")

            # Проверяем, что запрос был выполнен
            assert mock_conn.fetch.called

            # Получаем аргументы вызова
            call_args = mock_conn.fetch.call_args
            assert call_args is not None

            # call_args[0] - это кортеж позиционных аргументов
            args = call_args[0]

            # args[0] - SQL запрос
            assert isinstance(args[0], str)
            assert "SELECT" in args[0]

            # args[1], args[2], args[3] - параметры (start, end, period_type)
            assert len(args) >= 4

            # Проверяем, что переданные datetime имеют таймзону
            assert args[1].tzinfo is not None
            assert args[2].tzinfo is not None
            assert args[1].tzinfo == msk_tz
            assert args[2].tzinfo == msk_tz
            assert args[3] == "hour"

    @pytest.mark.asyncio
    async def test_get_for_period_without_period_type(self):
        """Получение суммаризаций без указания типа периода."""
        from src.database.repositories.summary_repository import SummaryRepository

        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])

        @asynccontextmanager
        async def mock_connection():
            yield mock_conn

        with patch("src.database.repositories.summary_repository.DatabasePoolManager") as MockPool:
            MockPool.connection = mock_connection

            start = datetime(2026, 1, 1)
            end = datetime(2026, 1, 2)

            result = await SummaryRepository.get_for_period(start, end)  # Без period_type

            # Проверяем, что запрос был выполнен
            assert mock_conn.fetch.called

            # Получаем аргументы вызова
            call_args = mock_conn.fetch.call_args
            assert call_args is not None

            args = call_args[0]

            # args[0] - SQL запрос
            assert isinstance(args[0], str)

            # В SQL запросе НЕ должно быть "AND period_type ="
            assert "AND period_type" not in args[0]

            # Параметров должно быть ровно 2 (только start и end)
            assert len(args) == 3  # SQL + start + end

    @pytest.mark.asyncio
    async def test_get_for_period_with_results(self):
        """Получение суммаризаций с результатами."""
        from src.database.repositories.summary_repository import SummaryRepository

        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(
            return_value=[
                {
                    "id": 1,
                    "period_start": datetime(2026, 1, 1, 10, 0),
                    "period_end": datetime(2026, 1, 1, 11, 0),
                    "period_type": "hour",
                    "content": '{"summary": "Тестовая сводка", "topics": ["Тема 1"]}',
                    "created_at": datetime(2026, 1, 1, 11, 5),
                    "model_used": "deepseek",
                    "prompt_tokens": 100,
                    "completion_tokens": 50,
                }
            ]
        )

        @asynccontextmanager
        async def mock_connection():
            yield mock_conn

        with patch("src.database.repositories.summary_repository.DatabasePoolManager") as MockPool:
            MockPool.connection = mock_connection

            result = await SummaryRepository.get_for_period(
                start=datetime(2026, 1, 1), end=datetime(2026, 1, 2), period_type="hour"
            )

            assert len(result) == 1
            assert result[0]["id"] == 1
            # content должен быть распарсен как dict
            assert isinstance(result[0]["content"], dict)
            assert result[0]["content"]["summary"] == "Тестовая сводка"

    @pytest.mark.asyncio
    async def test_get_smart_articles_empty(self):
        """Умная выборка - пустой результат."""
        from src.database.repositories.summary_repository import SummaryRepository

        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])  # Нет источников

        @asynccontextmanager
        async def mock_connection():
            yield mock_conn

        with patch("src.database.repositories.summary_repository.DatabasePoolManager") as MockPool:
            MockPool.connection = mock_connection

            result = await SummaryRepository.get_smart_articles(
                start=datetime(2026, 1, 1), end=datetime(2026, 1, 2), total_limit=10
            )

            assert result == []

    @pytest.mark.asyncio
    async def test_get_smart_articles_with_results(self):
        """Умная выборка с результатами."""
        from src.database.repositories.summary_repository import SummaryRepository

        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(
            return_value=[
                {
                    "title": "Новость 1",
                    "text": "Текст новости 1",
                    "published_at": datetime(2026, 1, 1, 10, 0),
                    "url": "https://example.com/1",
                    "source_name": "Источник 1",
                },
                {
                    "title": "Новость 2",
                    "text": "Текст новости 2",
                    "published_at": datetime(2026, 1, 1, 11, 0),
                    "url": "https://example.com/2",
                    "source_name": "Источник 2",
                },
            ]
        )

        @asynccontextmanager
        async def mock_connection():
            yield mock_conn

        with patch("src.database.repositories.summary_repository.DatabasePoolManager") as MockPool:
            MockPool.connection = mock_connection

            result = await SummaryRepository.get_smart_articles(
                start=datetime(2026, 1, 1), end=datetime(2026, 1, 2), total_limit=10
            )

            assert len(result) == 2
            assert result[0]["title"] == "Новость 1"
            assert result[1]["source_name"] == "Источник 2"

    @pytest.mark.asyncio
    async def test_save_summary(self):
        """Сохранение суммаризации."""
        from src.database.repositories.summary_repository import SummaryRepository

        mock_conn = AsyncMock()
        mock_conn.fetchrow = AsyncMock(return_value={"id": 42})

        @asynccontextmanager
        async def mock_connection():
            yield mock_conn

        with patch("src.database.repositories.summary_repository.DatabasePoolManager") as MockPool:
            MockPool.connection = mock_connection

            result = await SummaryRepository.save(
                period_start=datetime(2026, 1, 1, 10, 0),
                period_end=datetime(2026, 1, 1, 11, 0),
                period_type="hour",
                content={"summary": "Тест", "topics": ["Тема"]},
                model_used="deepseek",
                prompt_tokens=100,
                completion_tokens=50,
                cost_usd=0.01,
            )

            assert result == 42
            mock_conn.fetchrow.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_last(self):
        """Получение последней суммаризации."""
        from src.database.repositories.summary_repository import SummaryRepository

        mock_conn = AsyncMock()
        mock_conn.fetchrow = AsyncMock(
            return_value={
                "id": 1,
                "period_start": datetime(2026, 1, 1, 10, 0),
                "period_end": datetime(2026, 1, 1, 11, 0),
                "content": '{"summary": "Последняя сводка"}',
            }
        )

        @asynccontextmanager
        async def mock_connection():
            yield mock_conn

        with patch("src.database.repositories.summary_repository.DatabasePoolManager") as MockPool:
            MockPool.connection = mock_connection

            result = await SummaryRepository.get_last(period_type="hour")

            assert result is not None
            assert result["id"] == 1
            assert isinstance(result["content"], dict)
            assert result["content"]["summary"] == "Последняя сводка"

    @pytest.mark.asyncio
    async def test_get_last_not_found(self):
        """Получение последней суммаризации - не найдено."""
        from src.database.repositories.summary_repository import SummaryRepository

        mock_conn = AsyncMock()
        mock_conn.fetchrow = AsyncMock(return_value=None)

        @asynccontextmanager
        async def mock_connection():
            yield mock_conn

        with patch("src.database.repositories.summary_repository.DatabasePoolManager") as MockPool:
            MockPool.connection = mock_connection

            result = await SummaryRepository.get_last(period_type="hour")

            assert result is None


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
