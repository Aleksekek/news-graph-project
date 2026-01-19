"""
Скрипт для запуска парсера с поддержкой параметров.
"""

import argparse
import asyncio
import sys
from datetime import datetime, timedelta
from typing import List, Optional

# Добавляем src в путь
sys.path.insert(0, "src")

from src.application.use_cases.parse_source import ParseSourceUseCase
from src.utils.logging import setup_logging

logger = setup_logging()


def parse_args():
    """Парсинг аргументов командной строки."""
    parser = argparse.ArgumentParser(description="Запуск парсера новостей")

    parser.add_argument(
        "--source",
        required=True,
        choices=["lenta", "tinvest"],
        help="Источник для парсинга",
    )

    parser.add_argument(
        "--limit", type=int, default=100, help="Лимит элементов для парсинга"
    )

    parser.add_argument(
        "--archive", action="store_true", help="Включить архивный парсинг"
    )

    parser.add_argument(
        "--start-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d"),
        help="Начальная дата для архива (YYYY-MM-DD)",
    )

    parser.add_argument(
        "--end-date",
        type=lambda s: datetime.strptime(s, "%Y-%m-%d"),
        help="Конечная дата для архива (YYYY-MM-DD)",
    )

    # Параметры для TInvest
    parser.add_argument(
        "--tickers", type=str, help="Тикеры для Тинькофф Пульса через запятую"
    )

    # Параметры для Lenta
    parser.add_argument(
        "--categories", type=str, help="Категории для Lenta.ru через запятую"
    )

    parser.add_argument(
        "--check-duplicates",
        action="store_true",
        help="Проверять дубликаты перед парсингом",
    )

    return parser.parse_args()


async def main():
    """Основная функция."""
    args = parse_args()

    logger.info(f"🚀 Запуск парсера: {args.source}")
    logger.info(f"Аргументы: {vars(args)}")

    # Подготавливаем параметры парсера
    parser_params = {}

    if args.source == "tinvest" and args.tickers:
        tickers_list = [t.strip() for t in args.tickers.split(",")]
        parser_params["tickers"] = tickers_list

    if args.source == "lenta" and args.categories:
        categories_list = [c.strip() for c in args.categories.split(",")]
        parser_params["categories"] = categories_list

    # Определяем даты для архива
    start_date = None
    end_date = None

    if args.archive:
        if args.start_date and args.end_date:
            start_date = args.start_date
            end_date = args.end_date
        else:
            # По умолчанию последние 7 дней
            end_date = datetime.now()
            start_date = end_date - timedelta(days=7)

    # Создаем use case
    use_case = ParseSourceUseCase()

    try:
        # Выполняем парсинг с параметрами
        if args.source == "tinvest" and args.tickers:
            # Используем специализированный метод для тикеров
            tickers_list = [t.strip() for t in args.tickers.split(",")]
            stats = await use_case.execute_with_tickers(
                source_name=args.source,
                tickers=tickers_list,
                limit=args.limit,
                start_date=start_date,
                end_date=end_date,
            )
        else:
            # Обычный парсинг
            stats = await use_case.execute(
                source_name=args.source,
                limit=args.limit,
                start_date=start_date,
                end_date=end_date,
                parser_params=parser_params,
            )

        # Выводим результаты
        logger.info("📊 Результаты парсинга:")
        logger.info(f"  Всего обработано: {stats.total_rows}")
        logger.info(f"  Сохранено: {stats.saved}")
        logger.info(f"  Пропущено (дубликаты): {stats.skipped}")
        logger.info(f"  Ошибок: {stats.errors}")

        if stats.saved == 0 and stats.total_rows > 0:
            logger.warning("⚠️  Новых статей не найдено (возможно все уже в БД)")

        return 0

    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
