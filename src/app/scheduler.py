"""
Планировщик задач для парсинга.
Запускает парсеры по расписанию из schedule_config.yaml.
"""

import asyncio
import sys
from pathlib import Path

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

# Добавляем корень в путь (для запуска скрипта)
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.app.parse_source import ParseSourceUseCase
from src.config.schedules import schedule_config
from src.utils.logging import setup_logging

logger = setup_logging()


async def run_parse_task(source: str, **kwargs):
    """
    Задача парсинга для планировщика.

    Args:
        source: Имя источника
        **kwargs: Параметры для парсера (limit, categories, sections, tickers и т.д.)
    """
    logger.info(f"🔄 Запуск парсинга {source} с параметрами: {kwargs}")

    use_case = ParseSourceUseCase()

    try:
        # Преобразуем строковые параметры списков из YAML в list[str]
        for key in ("categories", "tickers", "sections"):
            if isinstance(kwargs.get(key), str):
                kwargs[key] = [v.strip() for v in kwargs[key].split(",") if v.strip()]

        # Выполняем парсинг
        stats = await use_case.execute(source_name=source, **kwargs)

        logger.info(
            f"✅ Парсинг {source} завершён: "
            f"сохранено {stats.saved}, "
            f"пропущено {stats.skipped}, "
            f"ошибок {stats.errors}"
        )

    except Exception as e:
        logger.error(f"❌ Ошибка парсинга {source}: {e}", exc_info=True)


def setup_scheduler() -> AsyncIOScheduler:
    """Настройка и запуск планировщика."""
    scheduler = AsyncIOScheduler(timezone="Europe/Moscow")

    known_sources = ["lenta", "tinvest", "interfax", "tass", "rbc"]

    for task_id, task in schedule_config.tasks.items():
        if not task.enabled:
            logger.info(f"Задача {task_id} отключена")
            continue

        source = next((s for s in known_sources if task_id.startswith(s)), None)
        if source is None:
            logger.warning(f"Неизвестный источник для задачи {task_id}")
            continue

        trigger = CronTrigger.from_crontab(task.cron)

        scheduler.add_job(
            run_parse_task,
            trigger,
            id=task_id,
            name=task.name,
            kwargs={"source": source, **task.kwargs},
            replace_existing=True,
        )

        logger.info(f"📅 Задача '{task.name}' добавлена: " f"cron='{task.cron}', source={source}")

    return scheduler


async def main():
    """Основная функция запуска планировщика."""
    logger.info("🚀 Запуск планировщика парсинга...")

    scheduler = setup_scheduler()
    scheduler.start()

    logger.info("⏰ Планировщик запущен. Ожидание задач...")

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
