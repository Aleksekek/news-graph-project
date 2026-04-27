"""
Утилиты для повторных попыток (retry logic).
"""

import asyncio
import logging
import time
from functools import wraps
from typing import Any, Callable, Optional, Tuple, Type

from src.core.exceptions import RetryExhaustedError


def async_retry(
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    logger: Optional[logging.Logger] = None,
):
    """
    Декоратор для повторных попыток выполнения асинхронных функций.

    Args:
        exceptions: Кортеж исключений, при которых делать повтор
        max_attempts: Максимальное количество попыток
        delay: Начальная задержка между попытками (секунды)
        backoff: Множитель для экспоненциальной задержки
        logger: Логгер для записи ошибок
    """
    if logger is None:
        import logging

        logger = logging.getLogger(__name__)

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        async def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            current_delay = delay

            for attempt in range(1, max_attempts + 1):
                try:
                    return await func(*args, **kwargs)

                except exceptions as e:
                    last_exception = e

                    if attempt == max_attempts:
                        logger.error(
                            f"❌ Все {max_attempts} попытки для {func.__name__} провалились: {e}"
                        )
                        break

                    logger.warning(
                        f"⚠️ Попытка {attempt}/{max_attempts} для {func.__name__} провалилась: {e}. "
                        f"Повтор через {current_delay:.1f} сек"
                    )

                    await asyncio.sleep(current_delay)
                    current_delay *= backoff

            raise RetryExhaustedError(
                f"Все {max_attempts} попытки для {func.__name__} провалились"
            ) from last_exception

        return wrapper

    return decorator


def retry(
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    logger: Optional[logging.Logger] = None,
):
    """
    Декоратор для повторных попыток выполнения синхронных функций.
    """
    if logger is None:
        import logging

        logger = logging.getLogger(__name__)

    def decorator(func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            current_delay = delay

            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)

                except exceptions as e:
                    last_exception = e

                    if attempt == max_attempts:
                        logger.error(
                            f"❌ Все {max_attempts} попытки для {func.__name__} провалились: {e}"
                        )
                        break

                    logger.warning(
                        f"⚠️ Попытка {attempt}/{max_attempts} для {func.__name__} провалилась: {e}. "
                        f"Повтор через {current_delay:.1f} сек"
                    )

                    time.sleep(current_delay)
                    current_delay *= backoff

            raise RetryExhaustedError(
                f"Все {max_attempts} попытки для {func.__name__} провалились"
            ) from last_exception

        return wrapper

    return decorator
