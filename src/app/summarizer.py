"""
Планировщик для генерации суммаризаций.
Запускает LLM для создания часовых и дневных сводок.
"""

import asyncio
import sys
from datetime import datetime, timedelta
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.processing.summarization.service import SummarizationService
from src.utils.logging import setup_logging

logger = setup_logging()


async def run_hourly_summarization():
    """Запуск суммаризации за прошедший час."""
    service = SummarizationService()

    # Суммаризируем предыдущий час
    last_hour = datetime.now().replace(minute=0, second=0, microsecond=0) - timedelta(
        hours=1
    )

    logger.info(f"🕐 Запуск часовой суммаризации для {last_hour}")

    try:
        summary_id = await service.generate_hourly_summary(last_hour)

        if summary_id:
            logger.info(f"✅ Часовая суммаризация завершена: id={summary_id}")
        else:
            logger.warning("⚠️ Часовая суммаризация не была создана (мало данных)")
    except Exception as e:
        logger.error(f"❌ Ошибка часовой суммаризации: {e}", exc_info=True)


async def run_daily_summary():
    """Утренняя сводка за вчерашний день."""
    service = SummarizationService()

    yesterday = datetime.now().replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(days=1)

    logger.info(f"📅 Запуск дневной суммаризации для {yesterday.date()}")

    try:
        summary_id = await service.generate_daily_summary(yesterday)

        if summary_id:
            logger.info(f"✅ Дневная суммаризация завершена: id={summary_id}")
        else:
            logger.warning("⚠️ Дневная суммаризация не была создана")
    except Exception as e:
        logger.error(f"❌ Ошибка дневной суммаризации: {e}", exc_info=True)


async def main():
    """Основная функция запуска планировщика."""
    logger.info("🚀 Запуск планировщика суммаризаций...")

    scheduler = AsyncIOScheduler(timezone="Europe/Moscow")

    # Каждый час в 5 минут после начала часа (01:05, 02:05...)
    scheduler.add_job(
        run_hourly_summarization,
        trigger=CronTrigger(minute=5),
        id="hourly_summarization",
        name="Часовая суммаризация",
        replace_existing=True,
    )

    # Каждый день в 09:05 — утренняя сводка за вчера
    scheduler.add_job(
        run_daily_summary,
        trigger=CronTrigger(hour=9, minute=5),
        id="daily_summary",
        name="Дневная суммаризация",
        replace_existing=True,
    )

    scheduler.start()
    logger.info("⏰ Планировщик запущен. Ожидание задач...")

    # Опционально: запустить сразу для проверки
    # await run_hourly_summarization()

    try:
        while True:
            await asyncio.sleep(60)
    except KeyboardInterrupt:
        logger.info("👋 Остановка планировщика")
        scheduler.shutdown()
        sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
