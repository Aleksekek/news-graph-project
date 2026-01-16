"""
Исправленный use case без контекстного менеджера для репозитория.
"""

import asyncio
from datetime import datetime
from typing import Any, Dict, List, Optional

from src.core.exceptions import DatabaseError, ParserError
from src.core.models import ArticleForDB, ParsedItem, ProcessingStats
from src.domain.parsing.factory import ParserFactory
from src.domain.parsing.processors.factory import ProcessorFactory
from src.domain.storage.database import ArticleRepository
from src.utils.logging import get_logger, log_async_execution_time

logger = get_logger("use_cases.parse_source")


class ParseSourceUseCase:
    """
    Use case для парсинга источника без контекстного менеджера.
    """

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)

    @log_async_execution_time()
    async def execute(
        self,
        source_name: str,
        limit: int = 100,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None,
        parser_params: Optional[Dict[str, Any]] = None,
        config_overrides: Optional[Dict[str, Any]] = None,
    ) -> ProcessingStats:
        """
        Выполнение парсинга источника с передачей параметров парсеру.

        Args:
            source_name: Имя источника (lenta, tinvest)
            limit: Лимит элементов
            start_date: Начальная дата для архивного парсинга
            end_date: Конечная дата для архивного парсинга
            parser_params: Параметры для передачи в методы парсера (например, tickers для TInvest)
            config_overrides: Переопределения конфигурации парсера

        Returns:
            Статистика обработки
        """
        self.logger.info(
            f"Запуск парсинга: {source_name}, лимит: {limit}, "
            f"параметры: {parser_params}"
        )

        try:
            # 1. Создаем парсер с конфигурацией
            parser = ParserFactory.create(source_name, config_overrides or {})

            # 2. Создаем процессор
            processor = ProcessorFactory.create(source_name)

            # 3. Создаем репозиторий
            repository = ArticleRepository()

            # 4. Выполняем парсинг с параметрами
            async with parser:
                if start_date and end_date:
                    # Архивный парсинг с параметрами
                    parsed_items = await parser.parse_period(
                        start_date=start_date,
                        end_date=end_date,
                        limit=limit,
                        **(parser_params or {}),
                    )
                else:
                    # Парсинг последних элементов с параметрами
                    parsed_items = await parser.parse_recent(
                        limit=limit, **(parser_params or {})
                    )

            self.logger.info(f"Распарсено элементов: {len(parsed_items)}")

            if not parsed_items:
                self.logger.warning("Нет элементов для сохранения")
                return ProcessingStats(total_rows=0)

            # 5. Преобразуем в модели БД
            articles_db = []
            for item in parsed_items:
                try:
                    article_db = processor.to_article_db(item)
                    if article_db:
                        # Убираем meta_info из модели если поле отсутствует в БД
                        article_db.meta_info = None
                        articles_db.append(article_db)
                except Exception as e:
                    self.logger.error(f"Ошибка преобразования: {e}")
                    continue

            self.logger.info(f"Преобразовано в модели БД: {len(articles_db)}")

            # 6. Сохраняем в БД (БЕЗ with!)
            stats = repository.save_articles_batch(articles_db)

            self.logger.info(
                f"Сохранено: {stats.saved}, пропущено: {stats.skipped}, "
                f"ошибок: {stats.errors}"
            )

            # 7. Очищаем ресурсы
            repository.cleanup()

            return stats

        except Exception as e:
            self.logger.error(f"Ошибка парсинга: {e}")
            raise ParserError(f"Ошибка парсинга источника {source_name}: {e}")

    async def execute_with_tickers(
        self, source_name: str, tickers: List[str], limit: int = 100, **kwargs
    ) -> ProcessingStats:
        """
        Специализированный метод для парсинга с тикерами.

        Args:
            source_name: Имя источника (должен быть tinvest)
            tickers: Список тикеров для парсинга
            limit: Лимит постов на тикер
            **kwargs: Дополнительные параметры

        Returns:
            Статистика обработки
        """
        if source_name != "tinvest":
            self.logger.warning(
                f"Параметр tickers игнорируется для источника {source_name}"
            )

        parser_params = {"tickers": tickers}

        # Для TInvest можно умножить лимит на количество тикеров
        if source_name == "tinvest" and tickers:
            # Если указаны тикеры, парсим по limit постов на каждый тикер
            effective_limit = limit * len(tickers)
        else:
            effective_limit = limit

        return await self.execute(
            source_name=source_name,
            limit=effective_limit,
            parser_params=parser_params,
            **kwargs,
        )
