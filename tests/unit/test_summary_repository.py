"""
Тесты для SummaryRepository.
"""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestSummaryRepository:
    """Тесты SummaryRepository."""

    @pytest.mark.asyncio
    async def test_get_for_period_empty(self):
        """Получение суммаризаций за период - пустой результат."""
        from src.database.repositories.summary_repository import SummaryRepository

        # Правильный мок для async context manager
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])

        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_cm.__aexit__ = AsyncMock()

        async def mock_connection():
            return mock_cm

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

        # Мокаем запрос источников - возвращаем пустой список
        mock_conn = AsyncMock()
        mock_conn.fetch = AsyncMock(return_value=[])  # Нет источников

        mock_cm = MagicMock()
        mock_cm.__aenter__ = AsyncMock(return_value=mock_conn)
        mock_cm.__aexit__ = AsyncMock()

        async def mock_connection():
            return mock_cm

        with patch("src.database.repositories.summary_repository.DatabasePoolManager") as MockPool:
            MockPool.connection = mock_connection

            result = await SummaryRepository.get_smart_articles(
                start=datetime(2026, 1, 1), end=datetime(2026, 1, 2), total_limit=10
            )

            assert result == []
