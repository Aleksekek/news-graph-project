"""
Тесты для форматтера суммаризаций.
"""

from datetime import datetime

import pytest

from src.processing.summarization.formatter import SummaryFormatter


class TestSummaryFormatter:
    """Тесты форматтера."""

    def setup_method(self):
        self.formatter = SummaryFormatter()

    def test_format_daily_digest_with_full_data(self):
        """Форматирование дайджеста с полными данными."""
        summary = {
            "period_start": datetime(2026, 4, 27, 0, 0),
            "content": {
                "summary": "Рынок вырос на фоне позитивных новостей",
                "topics": ["Экономика", "Нефть", "Рубль"],
                "trend": "Восходящий тренд",
                "important_events": ["Снижение ставки ФРС"],
            },
        }

        result = self.formatter.format_daily_digest(summary)

        assert "Рынок вырос" in result
        assert "Экономика" in result
        assert "Нефть" in result
        assert "Восходящий тренд" in result
        assert "27.04.2026" in result or "сегодня" in result

    def test_format_daily_digest_with_minimal_data(self):
        """Форматирование дайджеста с минимальными данными."""
        summary = {"content": {"summary": "Краткая сводка"}}

        result = self.formatter.format_daily_digest(summary)

        assert "Краткая сводка" in result
        assert "Темы дня" not in result

    def test_format_daily_digest_without_date(self):
        """Форматирование дайджеста без даты."""
        summary = {"content": {"summary": "Новости дня", "topics": ["Политика"]}}

        result = self.formatter.format_daily_digest(summary)

        assert "Новости дня" in result
        assert "сегодня" in result

    def test_format_daily_digest_with_invalid_content(self):
        """Форматирование с некорректным content."""
        summary = {"content": "plain text instead of dict"}

        result = self.formatter.format_daily_digest(summary)

        assert result == "Дайджест временно недоступен"

    def test_format_daily_digest_empty_topics(self):
        """Форматирование с пустыми темами."""
        summary = {
            "period_start": datetime(2026, 4, 27, 0, 0),
            "content": {"summary": "Сводка без тем", "topics": []},
        }

        result = self.formatter.format_daily_digest(summary)

        assert "Сводка без тем" in result
        assert "Темы дня" not in result
