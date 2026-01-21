import asyncio
import sys

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from src.application.use_cases.parse_source import ParseSourceUseCase
from src.config.schedules import schedule_config
from src.utils.logging import setup_logging

logger = setup_logging()


async def run_parser_task(source: str, **kwargs):
    """Запуск задачи парсинга — используем унифицированный execute."""
    logger.info(f"Starting scheduled parsing for {source} with args {kwargs}")

    use_case = ParseSourceUseCase()

    try:  # Преобразуем tickers в список, если это строка (для тинвеста)
        if (
            source == "lenta"
            and "categories" in kwargs
            and isinstance(kwargs["categories"], str)
        ):
            # Разбираем строку в список
            categories_str = kwargs["categories"]
            kwargs["categories"] = [
                t.strip() for t in categories_str.split(",") if t.strip()
            ]
            logger.info(f"Parsed categories list: {kwargs['categories']}")
        elif (
            source == "tinvest"
            and "tickers" in kwargs
            and isinstance(kwargs["tickers"], str)
        ):
            # Разбираем строку в список
            tickers_str = kwargs["tickers"]
            kwargs["tickers"] = [t.strip() for t in tickers_str.split(",") if t.strip()]
            logger.info(f"Parsed tickers list: {kwargs['tickers']}")

        # Выполняем парсинг через унифицированный execute
        stats = await use_case.execute(source_name=source, **kwargs)

        logger.info(
            f"Parsed {stats.total_rows} items for {source}: saved {stats.saved}, skipped {stats.skipped}, errors {stats.errors}"
        )

    except Exception as e:
        logger.error(f"Error parsing {source}: {e}")


async def main():
    """Основная функция запуска планировщика."""
    logger.info("Starting scheduled parser...")
    scheduler = AsyncIOScheduler(timezone="Europe/Moscow")

    for task_id, task in schedule_config.tasks.items():
        if task.enabled:
            trigger = CronTrigger.from_crontab(task.cron)
            task_name = task.name if task.name else task_id
            logger.info(f"Adding task: {task_name} with cron '{task.cron}'")

            # Определяем источник на основе имени задачи
            source = (
                "lenta"
                if "lenta" in task_id
                else ("tinvest" if "tinvest" in task_id else None)
            )
            if source:
                scheduler.add_job(
                    run_parser_task,
                    trigger,
                    name=task_name,
                    kwargs={"source": source, **task.kwargs},
                )

    scheduler.start()
    logger.info("Scheduler running...")

    # Бесконечный цикл с паузой
    while True:
        await asyncio.sleep(300)


if __name__ == "__main__":
    asyncio.run(main())
