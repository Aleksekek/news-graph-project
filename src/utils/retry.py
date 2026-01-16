"""
Утилиты для повторных попыток (retry logic)
"""

import asyncio
import inspect
import logging
import time
from functools import wraps
from typing import Any, Callable, Optional, Tuple, Type

from src.core.exceptions import RetryExhaustedError

logger = logging.getLogger(__name__)


def retry(
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    logger: Optional[logging.Logger] = None,
):
    """
    Декоратор для повторных попыток выполнения синхронных функций.

    Args:
        exceptions: Кортеж исключений, при которых делать повтор
        max_attempts: Максимальное количество попыток
        delay: Начальная задержка между попытками (секунды)
        backoff: Множитель для экспоненциальной задержки
        logger: Логгер для записи ошибок

    Returns:
        Декоратор
    """
    if logger is None:
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

    Returns:
        Декоратор
    """
    if logger is None:
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


class CircuitBreaker:
    """
    Реализация паттерна Circuit Breaker для устойчивости к сбоям.
    """

    def __init__(
        self,
        failure_threshold: int = 5,
        recovery_timeout: float = 60.0,
        expected_exceptions: Tuple[Type[Exception], ...] = (Exception,),
    ):
        """
        Args:
            failure_threshold: Количество ошибок до разрыва цепи
            recovery_timeout: Время восстановления (секунды)
            expected_exceptions: Исключения, считающиеся ошибками
        """
        self.failure_threshold = failure_threshold
        self.recovery_timeout = recovery_timeout
        self.expected_exceptions = expected_exceptions

        self.failures = 0
        self.last_failure_time = None
        self.state = "CLOSED"  # CLOSED, OPEN, HALF_OPEN

    def __call__(self, func: Callable) -> Callable:
        @wraps(func)
        def wrapper(*args, **kwargs):
            if self.state == "OPEN":
                # Проверяем, не прошло ли время восстановления
                if self.last_failure_time:
                    time_since_failure = time.time() - self.last_failure_time
                    if time_since_failure > self.recovery_timeout:
                        self.state = "HALF_OPEN"
                    else:
                        raise CircuitBreakerOpenError(
                            f"Circuit breaker is OPEN. Try again in "
                            f"{self.recovery_timeout - time_since_failure:.0f} seconds"
                        )

            try:
                result = func(*args, **kwargs)

                # Успешный вызов в HALF_OPEN состоянии сбрасывает счетчик
                if self.state == "HALF_OPEN":
                    self.reset()

                return result

            except self.expected_exceptions as e:
                self.record_failure()
                raise e

        return wrapper

    def record_failure(self):
        """Запись ошибки и обновление состояния."""
        self.failures += 1
        self.last_failure_time = time.time()

        if self.failures >= self.failure_threshold:
            self.state = "OPEN"
            logger.warning(f"⚠️ Circuit breaker OPENED after {self.failures} failures")

    def reset(self):
        """Сброс состояния circuit breaker."""
        self.failures = 0
        self.last_failure_time = None
        self.state = "CLOSED"
        logger.info("✅ Circuit breaker RESET to CLOSED state")


class CircuitBreakerOpenError(Exception):
    """Исключение при открытом circuit breaker."""

    pass


# Готовые конфигурации для часто используемых случаев
retry_network = retry(
    exceptions=(ConnectionError, TimeoutError), max_attempts=3, delay=2.0, backoff=2.0
)

retry_database = retry(
    exceptions=(Exception,),  # Широкий спектр для БД
    max_attempts=5,
    delay=1.0,
    backoff=1.5,
)

async_retry_network = async_retry(
    exceptions=(ConnectionError, TimeoutError, asyncio.TimeoutError),
    max_attempts=3,
    delay=2.0,
    backoff=2.0,
)


class RetryContext:
    """Контекст для хранения состояния между повторными попытками"""

    def __init__(self):
        self.shared_state = {}

    def get(self, key, default=None):
        return self.shared_state.get(key, default)

    def set(self, key, value):
        self.shared_state[key] = value


def retry_with_context(
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    max_attempts: int = 3,
    delay: float = 1.0,
    backoff: float = 2.0,
    logger: Optional[logging.Logger] = None,
):
    """Декоратор для повторных попыток с контекстом."""
    if logger is None:
        logger = logging.getLogger(__name__)

    def decorator(func: Callable):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            last_exception = None
            current_delay = delay
            context = RetryContext()

            for attempt in range(1, max_attempts + 1):
                try:
                    # Передаем контекст если метод его принимает
                    if "retry_context" in inspect.signature(func).parameters:
                        return func(self, *args, **kwargs, retry_context=context)
                    else:
                        return func(self, *args, **kwargs)

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
