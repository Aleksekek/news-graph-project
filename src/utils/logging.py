"""
Настройка логирования для проекта
"""

import json
import logging
import sys
import time
from datetime import datetime
from functools import wraps
from pathlib import Path
from typing import Optional

from src.config.settings import settings


class JSONFormatter(logging.Formatter):
    """Форматтер для вывода логов в JSON (удобно для ELK/CloudWatch)."""

    def format(self, record: logging.LogRecord) -> str:
        log_data = {
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "line": record.lineno,
        }

        # Добавляем exception если есть
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)

        # Добавляем дополнительные поля
        if hasattr(record, "extra"):
            log_data.update(record.extra)

        return json.dumps(log_data, ensure_ascii=False)


class ColoredFormatter(logging.Formatter):
    """Цветной форматтер для консоли."""

    COLORS = {
        "DEBUG": "\033[36m",  # Cyan
        "INFO": "\033[32m",  # Green
        "WARNING": "\033[33m",  # Yellow
        "ERROR": "\033[31m",  # Red
        "CRITICAL": "\033[41m",  # Red background
        "RESET": "\033[0m",  # Reset
    }

    def format(self, record: logging.LogRecord) -> str:
        levelname = record.levelname
        color = self.COLORS.get(levelname, self.COLORS["RESET"])

        # Форматируем сообщение
        message = super().format(record)

        # Добавляем цвет только если вывод в консоль
        if sys.stderr.isatty():
            return f"{color}{message}{self.COLORS['RESET']}"
        return message


def setup_logging(
    log_dir: Optional[str] = None,
    log_level: Optional[str] = None,
    json_format: bool = False,
) -> logging.Logger:
    """
    Настройка логирования для проекта.

    Args:
        log_dir: Директория для логов (по умолчанию из настроек)
        log_level: Уровень логирования (по умолчанию из настроек)
        json_format: Использовать JSON формат

    Returns:
        Корневой логгер
    """
    # Используем настройки если не указаны явно
    if log_dir is None:
        log_dir = settings.LOG_DIR
    if log_level is None:
        log_level = settings.LOG_LEVEL

    # Создаем директорию для логов
    log_path = Path(log_dir)
    log_path.mkdir(exist_ok=True)

    # Уровень логирования
    level = getattr(logging, log_level.upper(), logging.INFO)

    # Базовый формат
    log_format = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    date_format = "%Y-%m-%d %H:%M:%S"

    # Выбираем форматтер
    if json_format:
        formatter = JSONFormatter()
    else:
        formatter = ColoredFormatter(log_format, date_format)

    # Настройка корневого логгера
    root_logger = logging.getLogger()
    root_logger.setLevel(level)

    # Удаляем существующие handlers
    for handler in root_logger.handlers[:]:
        root_logger.removeHandler(handler)

    # Консольный handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    root_logger.addHandler(console_handler)

    # Файловый handler для основного лога
    file_handler = logging.FileHandler(log_path / "app.log", encoding="utf-8")
    file_handler.setFormatter(logging.Formatter(log_format, date_format))
    root_logger.addHandler(file_handler)

    # Отдельные файлы для компонентов
    components = ["parser", "database", "scheduler", "bot"]

    for component in components:
        handler = logging.FileHandler(log_path / f"{component}.log", encoding="utf-8")
        handler.setFormatter(logging.Formatter(log_format, date_format))

        logger = logging.getLogger(component)
        logger.addHandler(handler)
        logger.setLevel(level)

    # Уменьшаем логирование для некоторых библиотек
    noisy_libraries = [
        "urllib3",
        "telegram",
        "asyncio",
        "apscheduler",
        "httpx",
        "httpcore",
    ]

    for lib in noisy_libraries:
        logging.getLogger(lib).setLevel(logging.WARNING)

    # Логируем факт настройки
    root_logger.info(
        f"✅ Логирование настроено. Уровень: {log_level}, Директория: {log_dir}"
    )

    return root_logger


def get_logger(name: str, extra: Optional[dict] = None) -> logging.Logger:
    """
    Получение логгера с дополнительными полями.

    Args:
        name: Имя логгера
        extra: Дополнительные поля для логирования

    Returns:
        Настроенный логгер
    """
    logger = logging.getLogger(name)

    if extra:
        # Создаем адаптер для добавления extra полей
        class ExtraLoggerAdapter(logging.LoggerAdapter):
            def process(self, msg, kwargs):
                kwargs["extra"] = self.extra
                return msg, kwargs

        logger = ExtraLoggerAdapter(logger, extra)

    return logger


# Автоматическая настройка при импорте
root_logger = setup_logging()


# Декоратор для логирования времени выполнения функций
def log_execution_time(logger_name: str = __name__):
    """
    Декоратор для логирования времени выполнения функции.

    Args:
        logger_name: Имя логгера

    Returns:
        Декоратор
    """
    logger = logging.getLogger(logger_name)

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()

            try:
                result = func(*args, **kwargs)
                execution_time = time.time() - start_time

                logger.debug(
                    f"⏱️  {func.__name__} выполнена за {execution_time:.3f} сек"
                )

                return result

            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(
                    f"❌ {func.__name__} упала с ошибкой через {execution_time:.3f} сек: {e}"
                )
                raise

        return wrapper

    return decorator


def log_async_execution_time(logger_name: str = __name__):
    """
    Декоратор для логирования времени выполнения асинхронной функции.

    Args:
        logger_name: Имя логгера

    Returns:
        Декоратор
    """
    logger = logging.getLogger(logger_name)

    def decorator(func):
        @wraps(func)
        async def wrapper(*args, **kwargs):
            start_time = time.time()

            try:
                result = await func(*args, **kwargs)
                execution_time = time.time() - start_time

                logger.debug(
                    f"⏱️  {func.__name__} выполнена за {execution_time:.3f} сек"
                )

                return result

            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(
                    f"❌ {func.__name__} упала с ошибкой через {execution_time:.3f} сек: {e}"
                )
                raise

        return wrapper

    return decorator
