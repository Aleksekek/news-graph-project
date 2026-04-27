"""
Тесты для логирования.
"""

import pytest

from src.utils.logging import get_logger, log_async_execution_time, log_execution_time


class TestLogging:
    """Тесты логирования."""

    def test_get_logger_returns_logger(self):
        """get_logger возвращает логгер."""
        logger = get_logger("test_module")

        assert logger.name == "test_module"
        assert logger.level is not None

    def test_get_logger_same_name_returns_same(self):
        """Один и тот же имя возвращает один и тот же логгер."""
        logger1 = get_logger("test")
        logger2 = get_logger("test")

        assert logger1 is logger2

    @pytest.mark.asyncio
    async def test_log_async_execution_time_decorator(self):
        """Декоратор логирования времени асинхронной функции."""

        @log_async_execution_time()
        async def async_func():
            return "result"

        result = await async_func()
        assert result == "result"

    def test_log_execution_time_decorator(self):
        """Декоратор логирования времени синхронной функции."""

        @log_execution_time()
        def sync_func():
            return "result"

        result = sync_func()
        assert result == "result"
