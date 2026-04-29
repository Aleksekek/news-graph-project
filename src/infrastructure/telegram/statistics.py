"""
Логика статистики для Telegram бота.
"""

import logging
from datetime import datetime

from src.database.repositories.article_repository import ArticleRepository

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


def format_hourly_stats(stats: list[tuple[datetime, int]]) -> str:
    """
    Форматирует почасовую статистику для отображения.
    """
    logger.info(f"format_hourly_stats: получено {len(stats)} часов")

    if not stats:
        logger.warning("format_hourly_stats: stats is empty")
        return "❌ Нет данных для статистики"

    max_count = max(count for _, count in stats)
    logger.info(f"format_hourly_stats: max_count={max_count}")

    response = "🕐 *Активность по часам (последние 24 ч)*\n\n"
    response += f"📈 Максимум: {max_count} публикаций\n\n"

    for dt, count in stats:
        time_str = dt.strftime("%H:00")
        bar = create_hourly_bar(count, max_count)
        formatted_count = f"{count:,}".replace(",", " ")
        response += f"• {time_str} {bar} {formatted_count}\n"

    return response


async def get_hourly_stats(repo: ArticleRepository) -> list[tuple[datetime, int]]:
    """
    Получает почасовую статистику за последние 24 часа.

    Args:
        repo: репозиторий статей

    Returns:
        Список кортежей (datetime начала часа в MSK, количество статей)
    """
    logger.info("get_hourly_stats: вызываю repo.get_hourly_stats_24h()")
    result = await repo.get_hourly_stats_24h()
    logger.info(f"get_hourly_stats: получил {len(result)} записей")

    # Логируем первые 5 записей для проверки
    if result:
        logger.info(f"get_hourly_stats: первые 5 записей: {result[:5]}")
        # Проверяем, есть ли ненулевые значения
        non_zero = [(dt, c) for dt, c in result if c > 0]
        logger.info(f"get_hourly_stats: ненулевых записей: {len(non_zero)}")
        if non_zero:
            logger.info(f"get_hourly_stats: первая ненулевая: {non_zero[0]}")
    else:
        logger.warning("get_hourly_stats: результат пустой!")

    return result
