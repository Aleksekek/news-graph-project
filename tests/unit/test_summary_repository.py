"""
Тесты для SummaryRepository.
"""

from contextlib import asynccontextmanager
from datetime import datetime
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
