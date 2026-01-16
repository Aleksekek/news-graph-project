"""Точка входа в приложение"""

import asyncio
import sys

from src.config.settings import settings
from src.utils.logging import setup_logging

logger = setup_logging()


def main():
    """Основная функция"""
    logger.info(f"🚀 Запуск News Graph Project")
    logger.info(f"📊 Конфигурация: {settings.dict()}")

    # Здесь будет основной код приложения
    print("Приложение запущено!")

    # Для асинхронного кода
    # asyncio.run(async_main())

    return 0


if __name__ == "__main__":
    sys.exit(main())
