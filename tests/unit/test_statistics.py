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
        # При MAX_BAR_LENGTH=10, половина = 5 блоков
        expected = "█████" + "░" * 5
        assert bar == expected

    def test_create_hourly_bar_zero_max(self):
        """Создание полоски при нулевом максимуме."""
        bar = create_hourly_bar(0, 0)
        assert bar == "░" * MAX_BAR_LENGTH

    def test_create_hourly_bar_small_value(self):
        """Создание полоски для маленького значения (должен быть минимум 1 блок)."""
        bar = create_hourly_bar(1, 100)
        # Даже для 1 статьи должна быть хотя бы одна черта
        assert bar[0] == "█"
        # Остальные - пустые
        assert bar[1:] == "░" * (MAX_BAR_LENGTH - 1)

    def test_create_hourly_bar_zero_count_non_zero_max(self):
        """Создание полоски для нулевого значения при ненулевом максимуме."""
        bar = create_hourly_bar(0, 100)
        # Ноль статей - ни одного блока
        assert bar == "░" * MAX_BAR_LENGTH

    def test_create_hourly_bar_very_small_max(self):
        """Создание полоски при очень маленьком максимуме."""
        bar = create_hourly_bar(3, 5)
        # Ожидаем 6 блоков из 10 (3/5 = 0.6 * 10 = 6)
        expected = "██████" + "░" * 4
        assert bar == expected

    def test_create_hourly_bar_exact_proportion(self):
        """Создание полоски с точной пропорцией."""
        # 2/4 = 0.5 -> 5 блоков
        bar = create_hourly_bar(2, 4)
        expected = "█████" + "░" * 5
        assert bar == expected

        # 8/10 = 0.8 -> 8 блоков
        bar = create_hourly_bar(8, 10)
        expected = "████████" + "░" * 2
        assert bar == expected

    def test_format_hourly_stats(self):
        """Форматирование почастовой статистики."""
        now = datetime(2026, 4, 28, 15, 0)
        stats = [
            (now - timedelta(hours=3), 10),
            (now - timedelta(hours=2), 20),
            (now - timedelta(hours=1), 15),
            (now, 25),
        ]

        result = format_hourly_stats(stats)

        # Проверяем заголовок
        assert "Активность по часам (последние 24 ч)" in result
        assert "Максимум: 25 публикаций" in result

        # Проверяем формат строки
        for dt, _ in stats:
            time_str = dt.strftime("%d.%m %H:00")
            assert time_str in result

    def test_format_hourly_stats_empty(self):
        """Форматирование пустой статистики."""
        result = format_hourly_stats([])
        assert "Нет данных" in result

    def test_format_hourly_stats_single_hour(self):
        """Форматирование с одним часом."""
        stats = [(datetime(2026, 4, 28, 10, 0), 42)]
        result = format_hourly_stats(stats)

        assert "28.04 10:00" in result
        assert "42" in result
        # Полоска должна быть максимальной
        assert "█" * MAX_BAR_LENGTH in result

    def test_format_hourly_stats_all_zeros(self):
        """Форматирование с нулевыми значениями."""
        now = datetime(2026, 4, 28, 15, 0)
        stats = [
            (now - timedelta(hours=2), 0),
            (now - timedelta(hours=1), 0),
            (now, 0),
        ]

        result = format_hourly_stats(stats)

        assert "Максимум: 0 публикаций" in result
        for dt, _ in stats:
            time_str = dt.strftime("%d.%m %H:00")
            assert time_str in result
            # Все полоски должны быть пустыми
            assert "░" * MAX_BAR_LENGTH in result

    def test_format_hourly_stats_large_numbers(self):
        """Форматирование с большими числами (проверка форматирования тысяч)."""
        stats = [(datetime(2026, 4, 28, 10, 0), 12345)]
        result = format_hourly_stats(stats)

        # Должно быть с пробелом как разделителем тысяч
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

    @pytest.mark.asyncio
    async def test_get_hourly_stats_with_zeros(self):
        """Получение статистики с нулевыми значениями."""
        mock_repo = AsyncMock()
        now = datetime(2026, 4, 28, 15, 0)
        expected_stats = []
        for i in range(24):
            expected_stats.append((now - timedelta(hours=23 - i), i * 0))  # все нули

        mock_repo.get_hourly_stats_24h = AsyncMock(return_value=expected_stats)

        result = await get_hourly_stats(mock_repo)
        assert all(count == 0 for _, count in result)


class TestHourlyBarEdgeCases:
    """Тесты граничных случаев для create_hourly_bar."""

    def test_edge_case_very_large_numbers(self):
        """Очень большие числа."""
        bar = create_hourly_bar(1000000, 2000000)
        # Должна быть половина (5 блоков)
        assert bar == "█████" + "░" * 5

    def test_edge_case_count_greater_than_max(self):
        """Количество больше максимума (не должно случиться, но обрабатываем)."""
        bar = create_hourly_bar(150, 100)
        # Будет вычислено как 150/100 * 10 = 15, но обрезается до 10
        assert bar == "█" * MAX_BAR_LENGTH

    def test_edge_case_negative_numbers(self):
        """Отрицательные числа."""
        bar = create_hourly_bar(-5, 100)
        # Отрицательное значение -> 0 блоков
        assert bar == "░" * MAX_BAR_LENGTH


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
