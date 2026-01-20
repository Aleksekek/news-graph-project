"""
Исправленный use case с унифицированным интерфейсом и асинхронным сохранением.
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
    Use case для парсинга источника с унифицированным интерфейсом.
    Поддерживает передачу специфичных параметров через **kwargs.
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
        **kwargs,  # Все специфичные параметры для парсера
    ) -> ProcessingStats:
        """
        Унифицированный метод парсинга источника.

        Args:
            source_name: Имя источника (lenta, tinvest)
            limit: Лимит элементов
            start_date: Начальная дата для архивного парсинга
            end_date: Конечная дата для архивного парсинга
            **kwargs: Параметры для конкретного парсера:
                - Для TInvest: tickers=["SBER", "VTBR"]
                - Для Lenta: categories=["Политика", "Экономика"]

        Returns:
            Статистика обработки
        """
        self.logger.info(
            f"Запуск парсинга: {source_name}, лимит: {limit}, " f"параметры: {kwargs}"
        )

        try:
            # 1. Подготавливаем конфиг для парсера
            parser_config = {}

            # Для TInvest передаем тикеры в конфиг
            if source_name == "tinvest" and "tickers" in kwargs:
                parser_config["tickers"] = kwargs["tickers"]
                # Убираем из kwargs чтобы не передавать дважды
                tickers = kwargs.pop("tickers")

            # Для Lenta передаем категории в конфиг
            if source_name == "lenta" and "categories" in kwargs:
                parser_config["categories"] = kwargs["categories"]
                # Убираем из kwargs чтобы не передавать дважды
                categories = kwargs.pop("categories")

            # 2. Создаем парсер с конфигурацией
            parser = ParserFactory.create(source_name, parser_config)

            # 3. Создаем процессор и репозиторий
            processor = ProcessorFactory.create(source_name)

            # 4. Выполняем парсинг
            async with parser:
                if start_date and end_date:
                    # Архивный парсинг
                    parsed_items = await parser.parse_period(
                        start_date=start_date,
                        end_date=end_date,
                        limit=limit,
                        **kwargs,  # Остальные параметры
                    )
                else:
                    # Обычный парсинг
                    parsed_items = await parser.parse(
                        limit=limit,
                        **kwargs,  # Все параметры (включая tickers/categories если не убраны из конфига)
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
                        articles_db.append(article_db)
                except Exception as e:
                    self.logger.error(f"Ошибка преобразования: {e}")
                    continue

            self.logger.info(f"Преобразовано в модели БД: {len(articles_db)}")

            # 6. Сохраняем в БД (асинхронно)
            repository = ArticleRepository()
            stats = await repository.save_articles_batch(articles_db)

            self.logger.info(
                f"Сохранено: {stats.saved}, пропущено: {stats.skipped}, "
                f"ошибок: {stats.errors}"
            )

            if stats.saved == 0 and stats.total_rows > 0:
                self.logger.warning(
                    "⚠️  Новых статей не найдено (возможно все уже в БД)"
                )

            return stats

        except Exception as e:
            self.logger.error(f"Ошибка парсинга: {e}")
            raise ParserError(f"Ошибка парсинга источника {source_name}: {e}")
