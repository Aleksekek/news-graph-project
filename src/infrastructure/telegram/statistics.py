"""
Логика статистики для Telegram бота.
"""

import logging
from datetime import datetime
from typing import List, Tuple

from src.database.repositories.article_repository import ArticleRepository
from src.utils.datetime_utils import format_for_display, now_msk

logger = logging.getLogger(__name__)

MAX_BAR_LENGTH = 10


def create_hourly_bar(count: int, max_count: int) -> str:
    """
    Создаёт текстовую полоску для гистограммы.

    Args:
        count: текущее значение
        max_count: максимальное значение

    Returns:
        Строка с полоской (например, "████░░░░░░")
    """
    if max_count == 0:
        return "░" * MAX_BAR_LENGTH

    bar_length = int((count / max_count) * MAX_BAR_LENGTH)
    bar_length = max(0, min(MAX_BAR_LENGTH, bar_length))
    if bar_length == 0 and count > 0:
        bar_length = 1
    bar = "█" * bar_length
    bar = bar.ljust(MAX_BAR_LENGTH, "░")
    return bar


def format_hourly_stats(stats: List[Tuple[datetime, int]]) -> str:
    """
    Форматирует почасовую статистику для отображения.
    Текущий час помечается звёздочкой.
    """
    if not stats:
        return "❌ Нет данных для статистики"

    max_count = max(count for _, count in stats)
    now = now_msk()
    current_hour = now.hour

    response = "🕐 *Активность по часам (последние 24 ч)*\n\n"
    response += f"📈 Максимум: {max_count} публикаций\n\n"

    for dt, count in stats:
        time_str = dt.strftime("%H:00")

        # Помечаем текущий час
        if dt.hour == current_hour:
            time_str = f"🔄 {time_str}"

        bar = create_hourly_bar(count, max_count)
        formatted_count = f"{count:,}".replace(",", " ")
        response += f"• {time_str} {bar} {formatted_count}\n"

    response += f"\n🔄 — текущий час (данные обновляются в реальном времени)"

    return response


async def get_hourly_stats(repo: ArticleRepository) -> List[Tuple[datetime, int]]:
    """
    Получает почасовую статистику за последние 24 часа.

    Args:
        repo: репозиторий статей

    Returns:
        Список кортежей (datetime начала часа в MSK, количество статей)
    """
    return await repo.get_hourly_stats_24h()
