import asyncio
import sys

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger

from src.application.use_cases.parse_source import ParseSourceUseCase
from src.config.schedules import schedule_config
from src.utils.logging import setup_logging

logger = setup_logging()


async def run_parser_task(source: str, **kwargs):
    """Запуск задачи парсинга."""
    logger.info(f"Starting scheduled parsing for {source} with args {kwargs}")

    # Создаем use case (подобно run_parser.py)
    use_case = ParseSourceUseCase()

    try:
        if source == "tinvest" and "tickers" in kwargs:
            # Специально для tinvest с тикерами
            tickers = (
                kwargs.pop("tickers").split(",")
                if isinstance(kwargs.get("tickers"), str)
                else kwargs.get("tickers", [])
            )
            stats = await use_case.execute_with_tickers(
                source_name=source,
                tickers=tickers,
                limit=kwargs.get("num_posts", 100),
                start_date=None,
                end_date=None,
            )
        else:
            # Для lenta или других
            stats = await use_case.execute(
                source_name=source,
                limit=kwargs.get("limit", 100),
                start_date=None,
                end_date=None,
                parser_params={},
            )

        # Лог результата
        logger.info(
            f"Parsed {stats.total_rows} items for {source}: saved {stats.saved}, skipped {stats.skipped}, errors {stats.errors}"
        )

    except Exception as e:
        logger.error(f"Error parsing {source}: {e}")


async def main():
    """Основная функция запуска планировщика."""
    logger.info("Starting scheduled parser...")
    scheduler = AsyncIOScheduler(timezone="Europe/Moscow")

    # Добавляем задачи из конфига
    for task_id, task in schedule_config.tasks.items():
        if task.enabled:
            trigger = CronTrigger.from_crontab(task.cron)
            task_name = task.name if task.name else task_id
            logger.info(f"Adding task: {task_name} with cron '{task.cron}'")

            # Для разных источников
            if "lenta" in task_id.lower():
                source = "lenta"
            elif "tinvest" in task_id.lower():
                source = "tinvest"
            else:
                logger.warning(f"Unknown task {task_id}")
                continue

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
        await asyncio.sleep(300)  # Проверять каждые 5 мин


if __name__ == "__main__":
    asyncio.run(main())
