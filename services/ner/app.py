"""
Точка входа NER-сервиса.
Запускает NER-обработку каждые 30 минут и сразу при старте.
"""

import asyncio
import sys
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.app.ner_processor import NERProcessor
from src.utils.logging import setup_logging

logger = setup_logging()

BATCH_SIZE = 100


async def run_ner_batch() -> None:
    """Запускает один батч NER-обработки."""
    processor = NERProcessor()
    try:
        await processor.process_batch(batch_size=BATCH_SIZE)
    except Exception as e:
        logger.error(f"❌ Критическая ошибка NER-батча: {e}", exc_info=True)


async def main() -> None:
    logger.info("🚀 Запуск NER-сервиса...")

    scheduler = AsyncIOScheduler(timezone="Europe/Moscow")
    scheduler.add_job(
        run_ner_batch,
        trigger=CronTrigger(minute="*/30"),
        id="ner_batch",
        name="NER обработка",
        replace_existing=True,
    )
    scheduler.start()
    logger.info("⏰ Планировщик запущен (каждые 30 минут)")

    # Обрабатываем накопившееся сразу при старте
    await run_ner_batch()

    try:
        while True:
            await asyncio.sleep(60)
    except KeyboardInterrupt:
        logger.info("👋 Остановка NER-сервиса")
        scheduler.shutdown()
        sys.exit(0)


if __name__ == "__main__":
    asyncio.run(main())
