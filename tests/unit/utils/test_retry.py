"""
Тесты для декоратора retry.
"""

from unittest.mock import AsyncMock, patch

import pytest

from src.core.exceptions import RetryExhaustedError
from src.utils.retry import async_retry, retry


class TestAsyncRetry:
    """Тесты async_retry декоратора."""

    @pytest.mark.asyncio
    async def test_retry_success_first_try(self):
        """Успех с первой попытки."""
        mock_func = AsyncMock(return_value="success")

        @async_retry(max_attempts=3)
        async def test_func():
            return await mock_func()

        result = await test_func()

        assert result == "success"
        assert mock_func.call_count == 1

    @pytest.mark.asyncio
    async def test_retry_success_after_failure(self):
        """Успех после нескольких неудач."""
        call_count = 0

        async def flaky_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ConnectionError("Temporary error")
            return "success"

        @async_retry(exceptions=(ConnectionError,), max_attempts=5, delay=0.01)
        async def test_func():
            return await flaky_func()

        result = await test_func()

        assert result == "success"
        assert call_count == 3

    @pytest.mark.asyncio
    async def test_retry_exhausted(self):
        """Исчерпаны попытки."""
        mock_func = AsyncMock(side_effect=ValueError("Permanent error"))

        @async_retry(exceptions=(ValueError,), max_attempts=3, delay=0.01)
        async def test_func():
            return await mock_func()

        with pytest.raises(RetryExhaustedError):
            await test_func()

        assert mock_func.call_count == 3


class TestSyncRetry:
    """Тесты синхронного retry декоратора."""

    def test_sync_retry_success_first_try(self):
        """Успех с первой попытки."""

        call_count = 0

        @retry(max_attempts=3)
        def test_func():
            nonlocal call_count
            call_count += 1
            return "success"

        result = test_func()

        assert result == "success"
        assert call_count == 1

    def test_sync_retry_success_after_failure(self):
        """Успех после нескольких неудач."""

        call_count = 0

        @retry(exceptions=(ValueError,), max_attempts=5, delay=0.01)
        def test_func():
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                raise ValueError("Temporary error")
            return "success"

        result = test_func()

        assert result == "success"
        assert call_count == 3

    def test_sync_retry_exhausted(self):
        """Исчерпаны попытки."""

        @retry(exceptions=(ValueError,), max_attempts=2, delay=0.01)
        def test_func():
            raise ValueError("Permanent error")

        with pytest.raises(RetryExhaustedError):
            test_func()
