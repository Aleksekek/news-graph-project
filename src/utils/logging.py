"""
Настройка логирования для проекта.
"""

import functools
import logging
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

from src.config.settings import settings


class ColoredFormatter(logging.Formatter):
    """Цветной форматтер для консоли."""

    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[41m",  # Red background
        "RESET": "\033[0m",
    }

    def format(self, record: logging.LogRecord) -> str:
        levelname = record.levelname
        color = self.COLORS.get(levelname, self.COLORS["RESET"])

        message = super().format(record)

        if sys.stderr.isatty():
            return f"{color}{message}{self.COLORS['RESET']}"
        return message


def get_logger(name: str) -> logging.Logger:
    """
    Получение настроенного логгера.

    Args:
        name: Имя логгера (обычно __name__)

    Returns:
        Logger instance
    """
    logger = logging.getLogger(name)

    # Если логгер уже настроен, возвращаем
    if logger.handlers:
        return logger

    # Уровень логирования
    level = getattr(logging, settings.LOG_LEVEL.upper(), logging.INFO)
    logger.setLevel(level)

    # Создаём директорию для логов
    log_path = Path(settings.LOG_DIR)
    log_path.mkdir(exist_ok=True)

    # Формат
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    # Консольный handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(ColoredFormatter(log_format, date_format))
    logger.addHandler(console_handler)

    # Файловый handler
    file_handler = logging.FileHandler(log_path / "app.log", encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(log_format, date_format))
    logger.addHandler(file_handler)

    # Уменьшаем шум от библиотек
    for lib in ["urllib3", "asyncio", "apscheduler", "httpx", "httpcore"]:
        logging.getLogger(lib).setLevel(logging.WARNING)

    return logger


def setup_logging() -> logging.Logger:
    """Настройка корневого логгера."""
    return get_logger("news_graph")


def log_async_execution_time(logger_name: str | None = None):
    """
    Декоратор для логирования времени выполнения асинхронной функции.

    Args:
        logger_name: Имя логгера (если None, используем __name__)

    Returns:
        Декоратор
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            name = logger_name or func.__module__
            logger = get_logger(name)

            start_time = time.time()
            try:
                result = await func(*args, **kwargs)
                elapsed = time.time() - start_time
                logger.debug(f"⏱️ {func.__name__} выполнена за {elapsed:.3f} сек")
                return result
            except Exception as e:
                elapsed = time.time() - start_time
                logger.error(f"❌ {func.__name__} упала через {elapsed:.3f} сек: {e}")
                raise

        return wrapper

    return decorator


def log_execution_time(logger_name: str | None = None):
    """
    Декоратор для логирования времени выполнения синхронной функции.

    Args:
        logger_name: Имя логгера (если None, используем __name__)

    Returns:
        Декоратор
    """

    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            name = logger_name or func.__module__
            logger = get_logger(name)

            start_time = time.time()
            try:
                result = func(*args, **kwargs)
                elapsed = time.time() - start_time
                logger.debug(f"⏱️ {func.__name__} выполнена за {elapsed:.3f} сек")
                return result
            except Exception as e:
                elapsed = time.time() - start_time
                logger.error(f"❌ {func.__name__} упала через {elapsed:.3f} сек: {e}")
                raise

        return wrapper

    return decorator
