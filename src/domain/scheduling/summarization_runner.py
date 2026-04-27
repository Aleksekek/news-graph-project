#!/usr/bin/env python
"""Планировщик для запуска суммаризаций"""

import asyncio
import logging
import sys
from datetime import datetime, timedelta

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from src.domain.processing.summarization_service import SummarizationService

# Настройка логирования
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s", level=logging.INFO
)
logger = logging.getLogger(__name__)


async def run_hourly_summarization():
    """Запуск суммаризации за прошедший час"""
    service = SummarizationService()

    # Суммаризируем предыдущий час
    last_hour = datetime.now() - timedelta(hours=1)
    last_hour = last_hour.replace(minute=0, second=0, microsecond=0)

    logger.info(f"🕐 Запуск часовой суммаризации для {last_hour}")
    summary_id = await service.generate_hourly_summary(last_hour)

    if summary_id:
        logger.info(f"✅ Часовая суммаризация завершена: id={summary_id}")
    else:
        logger.warning("⚠️ Часовая суммаризация не была создана")


async def run_morning_summary():
    """Утренняя сводка за вчерашний день"""
    service = SummarizationService()
    yesterday = datetime.now().replace(
        hour=0, minute=0, second=0, microsecond=0
    ) - timedelta(days=1)

    logger.info(f"📅 Запуск дневной суммаризации для {yesterday.date()}")
    summary_id = await service.generate_daily_summary(yesterday)

    if summary_id:
        logger.info(f"✅ Дневная суммаризация завершена: id={summary_id}")
    else:
        logger.warning("⚠️ Дневная суммаризация не была создана")


async def main():
    """Основная функция запуска планировщика"""
    logger.info("🚀 Запуск планировщика суммаризаций...")

    scheduler = AsyncIOScheduler(timezone="Europe/Moscow")

    # Каждый час в 5 минут после начала часа (01:05, 02:05...)
    scheduler.add_job(
        run_hourly_summarization,
        trigger=CronTrigger(minute=5),
        id="hourly_summarization",
        name="Часовая суммаризация",
    )

    # Каждый день в 09:05 — утренняя сводка за вчера
    scheduler.add_job(
        run_morning_summary,
        trigger=CronTrigger(hour=9, minute=5),
        id="daily_summary",
        name="Дневная суммаризация",
    )

    scheduler.start()
    logger.info("⏰ Планировщик запущен. Ожидание задач...")

    # Первый запуск сразу (для теста)
    asyncio.create_task(run_hourly_summarization())

    # Бесконечный цикл
    try:
        while True:
            await asyncio.sleep(60)
    except KeyboardInterrupt:
        logger.info("👋 Остановка планировщика")
        scheduler.shutdown()
        sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
