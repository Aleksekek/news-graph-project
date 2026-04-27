"""
Use case для парсинга источника.
Оркестрирует парсер, конвертер и репозиторий.
"""

from datetime import datetime
from typing import Optional

from src.core.models import ProcessingStats
from src.database.repositories.article_repository import ArticleRepository
from src.parsers.converter_factory import ConverterFactory
from src.parsers.factory import ParserFactory
from src.utils.datetime_utils import format_for_db
from src.utils.logging import get_logger, log_async_execution_time

logger = get_logger("use_cases.parse_source")


class ParseSourceUseCase:
    """Use case для парсинга источника."""

    def __init__(self):
        self.repo = ArticleRepository()

    @log_async_execution_time()
    async def execute(
        self,
        source_name: str,
        limit: int = 100,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        **filters,
    ) -> ProcessingStats:
        """
        Выполнение парсинга источника.

        Args:
            source_name: Имя источника ("lenta" или "tinvest")
            limit: Максимальное количество элементов
            start_date: Начало периода (для архива)
            end_date: Конец периода (для архива)
            **filters: Специфичные фильтры (categories, tickers и т.д.)

        Returns:
            Статистика обработки
        """
        logger.info(f"Запуск парсинга: {source_name}, лимит={limit}")

        # 1. Создаём парсер и конвертер
        parser = ParserFactory.create(source_name, filters)
        converter = ConverterFactory.create(source_name)

        # 2. Выполняем парсинг
        async with parser:
            if start_date and end_date:
                # Приводим даты к MSK naive
                start = format_for_db(start_date)
                end = format_for_db(end_date)
                result = await parser.parse_period(start, end, limit, **filters)
            else:
                result = await parser.parse(limit, **filters)

        if not result.items:
            logger.warning(f"Нет элементов для {source_name}")
            return ProcessingStats(total_rows=0)

        logger.info(f"Распарсено {len(result.items)} элементов")

        # 3. Конвертируем в модели БД
        articles = []
        for item in result.items:
            try:
                article = converter.convert(item)
                articles.append(article)
            except Exception as e:
                logger.error(f"Ошибка конвертации: {e}")
                continue

        # 4. Сохраняем в БД
        stats = await self.repo.save_batch(articles)

        logger.info(
            f"Готово: сохранено {stats.saved}, "
            f"пропущено {stats.skipped}, "
            f"ошибок {stats.errors}"
        )

        return stats
