"""
Точка входа NER-сервиса.
Запускает NER-обработку каждые 30 минут и сразу при старте.

Еженедельная LLM-очистка сущностей (вс 03:00 МСК) запускается ТОЛЬКО при
NER_ENGINE=natasha. В режиме llm канонизация происходит at-extract-time —
weekly cleanup был бы лишней тратой токенов.
"""

import asyncio
import sys
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.app.entity_cleanup import count_unaliased_entities, run_full_cleanup
from src.app.ner_processor import NERProcessor
from src.config.settings import settings
from src.utils.logging import setup_logging

logger = setup_logging()

# Минимум неалиасированных сущностей для запуска LLM-очистки.
# Защита от холостого запуска сразу после предыдущей очистки.
MIN_UNALIASED_FOR_CLEANUP = 100


async def run_ner_batch() -> None:
    """Запускает один батч NER-обработки. Размер и параллелизм — из settings."""
    processor = NERProcessor()
    try:
        await processor.process_batch()
    except Exception as e:
        logger.error(f"❌ Критическая ошибка NER-батча: {e}", exc_info=True)


async def run_entity_cleanup() -> None:
    """Еженедельная LLM-очистка сущностей: нормализует алиасы и удаляет мусор."""
    try:
        unaliased = await count_unaliased_entities()
        if unaliased < MIN_UNALIASED_FOR_CLEANUP:
            logger.info(
                f"[cleanup] Пропуск: неалиасированных сущностей {unaliased} "
                f"< {MIN_UNALIASED_FOR_CLEANUP}"
            )
            return

        logger.info(f"[cleanup] Старт еженедельной очистки ({unaliased} неалиасированных сущностей)")
        stats = await run_full_cleanup()
        logger.info(
            f"[cleanup] Завершено: алиасов={stats['aliases']}, "
            f"исправлений={stats['type_fixes']}, слито={stats['merged']}, "
            f"токенов={stats['tokens']}, ошибок={stats['errors']}"
        )
    except Exception as e:
        logger.error(f"❌ Ошибка LLM-очистки сущностей: {e}", exc_info=True)


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

    # Weekly entity_cleanup нужен только для Natasha — в LLM режиме канонизация
    # уже происходит на этапе extract, и weekly cleanup был бы дублирующим расходом.
    if settings.NER_ENGINE == "natasha":
        scheduler.add_job(
            run_entity_cleanup,
            trigger=CronTrigger(day_of_week="sun", hour=3, minute=0),
            id="entity_cleanup",
            name="LLM-очистка сущностей",
            replace_existing=True,
        )
        logger.info("⏰ Планировщик: NER каждые 30 мин + cleanup вс 03:00 МСК (Natasha)")
    else:
        logger.info(
            f"⏰ Планировщик: NER каждые 30 мин (engine={settings.NER_ENGINE}, "
            f"weekly cleanup НЕ зарегистрирован — не нужен для LLM)"
        )

    scheduler.start()

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
