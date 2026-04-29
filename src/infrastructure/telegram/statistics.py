"""
Логика статистики для Telegram бота.
"""

import logging
from datetime import datetime
from typing import List, Tuple

from src.database.repositories.article_repository import ArticleRepository
from src.utils.datetime_utils import now_msk

logger = logging.getLogger(__name__)

MAX_BAR_LENGTH = 10


def create_hourly_bar(count: int, max_count: int) -> str:
    """Создаёт текстовую полоску для гистограммы."""
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
    """Форматирует почасовую статистику для отображения."""
    if not stats:
        return "❌ Нет данных за последние 24 часа"

    max_count = max(count for _, count in stats)

    response = "🕐 *Активность по часам (последние 24 ч)*\n\n"
    response += f"📈 Максимум: {max_count} публикаций\n\n"

    for dt, count in stats:
        time_str = dt.strftime("%H:00")
        bar = create_hourly_bar(count, max_count)
        formatted_count = f"{count:,}".replace(",", " ")
        response += f"• {time_str} {bar} {formatted_count}\n"

    return response


async def get_hourly_stats(repo: ArticleRepository) -> List[Tuple[datetime, int]]:
    """Получает почасовую статистику за последние 24 часа."""
    try:
        result = await repo.get_hourly_stats_24h()
        return result if result else []
    except Exception as e:
        logger.error(f"Ошибка в get_hourly_stats: {e}")
        return []
