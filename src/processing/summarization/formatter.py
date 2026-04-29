"""
Форматирование суммаризаций для вывода.
"""

from typing import Any

from src.utils.datetime_utils import format_for_display


class SummaryFormatter:
    """Форматирование суммаризаций для разных целей."""

    def format_daily_digest(self, summary: dict[str, Any]) -> str:
        """Форматирование дневного дайджеста."""
        content = summary.get("content", {})
        if not isinstance(content, dict):
            return "Дайджест временно недоступен"

        date = summary.get("period_start")
        date_str = format_for_display(date, include_time=False) if date else "сегодня"

        text = f"📅 *{date_str}*\n\n"
        text += f"📌 *Главное:* {content.get('summary', 'Нет данных')}\n\n"

        topics = content.get("topics", [])
        if topics:
            text += f"📊 *Темы дня:* {', '.join(topics[:3])}\n"

        trend = content.get("trend")
        if trend:
            text += f"\n📈 *Тренд:* {trend}\n"

        return text
