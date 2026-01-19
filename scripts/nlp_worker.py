"""
Скрипт для запуска NLP воркера.
Совместимость со старой структурой.
"""

import argparse
import asyncio
import sys

# Добавляем src в путь
sys.path.insert(0, "src")

from src.domain.processing.nlp_worker import SimpleNLPWorker
from src.utils.logging import setup_logging

logger = setup_logging()


def parse_args():
    """Парсинг аргументов командной строки."""
    parser = argparse.ArgumentParser(description="Запуск NLP воркера")

    parser.add_argument(
        "--batch-size", type=int, default=20, help="Размер батча для обработки"
    )

    parser.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Интервал между проверками новых статей (секунды)",
    )

    parser.add_argument(
        "--once", action="store_true", help="Обработать один батч и выйти"
    )

    return parser.parse_args()


async def main():
    """Основная функция."""
    args = parse_args()

    logger.info(f"🚀 Запуск NLP воркера")
    logger.info(f"Размер батча: {args.batch_size}")
    logger.info(f"Режим: {'один батч' if args.once else 'непрерывный'}")

    # Создаем воркер
    worker = SimpleNLPWorker(batch_size=args.batch_size)

    try:
        if args.once:
            # Обработка одного батча
            processed = await worker.process_batch()
            logger.info(f"Обработано статей: {processed}")
        else:
            # Непрерывная обработка
            await worker.process_continuously(interval=args.interval)

        return 0

    except KeyboardInterrupt:
        logger.info("Остановка воркера по запросу пользователя")
        return 0
    except Exception as e:
        logger.error(f"❌ Критическая ошибка: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
