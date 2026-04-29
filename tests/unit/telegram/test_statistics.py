"""
Тесты для статистики Telegram бота.
"""

from datetime import datetime, timedelta
from unittest.mock import AsyncMock, Mock, patch

import pytest

from src.infrastructure.telegram.statistics import (
    MAX_BAR_LENGTH,
    create_hourly_bar,
    format_hourly_stats,
    get_hourly_stats,
)


class TestStatistics:
    """Тесты для статистики."""

    def test_create_hourly_bar_max(self):
        """Создание полоски для максимального значения."""
        bar = create_hourly_bar(100, 100)
        assert bar == "█" * MAX_BAR_LENGTH

    def test_create_hourly_bar_half(self):
        """Создание полоски для половины максимума."""
        bar = create_hourly_bar(50, 100)
        expected = "█████" + "░" * 5
        assert bar == expected

    def test_create_hourly_bar_zero_max(self):
        """Создание полоски при нулевом максимуме."""
        bar = create_hourly_bar(0, 0)
        assert bar == "░" * MAX_BAR_LENGTH

    def test_create_hourly_bar_small_value(self):
        """Создание полоски для маленького значения."""
        bar = create_hourly_bar(1, 100)
        assert bar[0] == "█"
        assert bar[1:] == "░" * (MAX_BAR_LENGTH - 1)

    def test_create_hourly_bar_zero_count_non_zero_max(self):
        """Создание полоски для нулевого значения."""
        bar = create_hourly_bar(0, 100)
        assert bar == "░" * MAX_BAR_LENGTH

    def test_create_hourly_bar_very_small_max(self):
        """Создание полоски при очень маленьком максимуме."""
        bar = create_hourly_bar(3, 5)
        expected = "██████" + "░" * 4
        assert bar == expected

    def test_create_hourly_bar_exact_proportion(self):
        """Создание полоски с точной пропорцией."""
        bar = create_hourly_bar(2, 4)
        expected = "█████" + "░" * 5
        assert bar == expected

        bar = create_hourly_bar(8, 10)
        expected = "████████" + "░" * 2
        assert bar == expected

    def test_format_hourly_stats(self):
        """Форматирование почасовой статистики."""
        now = datetime(2026, 4, 28, 15, 0)
        stats = [
            (now - timedelta(hours=3), 10),
            (now - timedelta(hours=2), 20),
            (now - timedelta(hours=1), 15),
            (now, 25),
        ]

        result = format_hourly_stats(stats)

        # Проверяем заголовок
        assert "Активность по часам" in result
        assert "Максимум: 25" in result

        # Проверяем часы
        assert "12:00" in result
        assert "13:00" in result
        assert "14:00" in result
        assert "15:00" in result

        # Проверяем, что нет лишних символов
        assert "🔄" not in result
        assert "28.04" not in result

    def test_format_hourly_stats_empty(self):
        """Форматирование пустой статистики."""
        result = format_hourly_stats([])
        assert "Нет данных" in result

    def test_format_hourly_stats_single_hour(self):
        """Форматирование с одним часом."""
        stats = [(datetime(2026, 4, 28, 10, 0), 42)]
        result = format_hourly_stats(stats)

        assert "10:00" in result
        assert "42" in result
        assert "█" * MAX_BAR_LENGTH in result
        assert "28.04" not in result

    def test_format_hourly_stats_all_zeros(self):
        """Форматирование с нулевыми значениями."""
        now = datetime(2026, 4, 28, 15, 0)
        stats = [
            (now - timedelta(hours=2), 0),
            (now - timedelta(hours=1), 0),
            (now, 0),
        ]

        result = format_hourly_stats(stats)

        assert "Максимум: 0" in result
        assert "13:00" in result
        assert "14:00" in result
        assert "15:00" in result
        assert "░" * MAX_BAR_LENGTH in result

    def test_format_hourly_stats_large_numbers(self):
        """Форматирование с большими числами."""
        stats = [(datetime(2026, 4, 28, 10, 0), 12345)]
        result = format_hourly_stats(stats)

        assert "12 345" in result

    @pytest.mark.asyncio
    async def test_get_hourly_stats_success(self):
        """Получение почасовой статистики через репозиторий."""
        mock_repo = AsyncMock()
        expected_stats = [
            (datetime(2026, 4, 28, 10, 0), 10),
            (datetime(2026, 4, 28, 11, 0), 20),
        ]
        mock_repo.get_hourly_stats_24h = AsyncMock(return_value=expected_stats)

        result = await get_hourly_stats(mock_repo)

        assert result == expected_stats
        mock_repo.get_hourly_stats_24h.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_hourly_stats_empty(self):
        """Получение пустой статистики."""
        mock_repo = AsyncMock()
        mock_repo.get_hourly_stats_24h = AsyncMock(return_value=[])

        result = await get_hourly_stats(mock_repo)

        assert result == []


class TestHourlyBarEdgeCases:
    """Тесты граничных случаев."""

    def test_edge_case_very_large_numbers(self):
        """Очень большие числа."""
        bar = create_hourly_bar(1000000, 2000000)
        assert bar == "█████" + "░" * 5

    def test_edge_case_count_greater_than_max(self):
        """Количество больше максимума."""
        bar = create_hourly_bar(150, 100)
        assert bar == "█" * MAX_BAR_LENGTH

    def test_edge_case_negative_numbers(self):
        """Отрицательные числа."""
        bar = create_hourly_bar(-5, 100)
        assert bar == "░" * MAX_BAR_LENGTH


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
