"""
Тесты для DatabasePoolManager.
"""

from unittest.mock import AsyncMock, Mock, patch

import pytest


class TestDatabasePoolManager:
    """Тесты пула соединений."""

    @pytest.mark.asyncio
    async def test_get_pool_singleton(self):
        """Пул должен быть синглтоном."""
        from src.database.pool import DatabasePoolManager

        # Сбрасываем состояние перед тестом
        DatabasePoolManager._pool = None
        DatabasePoolManager._semaphore = None

        with patch("src.database.pool.asyncpg") as mock_asyncpg:
            mock_pool = AsyncMock()
            mock_asyncpg.create_pool = AsyncMock(return_value=mock_pool)

            pool1 = await DatabasePoolManager.get_pool()
            pool2 = await DatabasePoolManager.get_pool()

            assert pool1 == pool2
            assert mock_asyncpg.create_pool.call_count == 1

        # Очищаем после теста
        DatabasePoolManager._pool = None
        DatabasePoolManager._semaphore = None

    @pytest.mark.asyncio
    async def test_connection_context_manager(self):
        """Контекстный менеджер должен работать."""
        from src.database.pool import DatabasePoolManager

        # Сбрасываем состояние
        DatabasePoolManager._pool = None
        DatabasePoolManager._semaphore = None

        with patch("src.database.pool.asyncpg") as mock_asyncpg:
            mock_pool = AsyncMock()
            mock_conn = AsyncMock()
            mock_pool.acquire = AsyncMock(return_value=mock_conn)
            mock_asyncpg.create_pool = AsyncMock(return_value=mock_pool)

            async with DatabasePoolManager.connection() as conn:
                # conn может быть mock_conn или результат acquire
                # Проверяем что соединение было получено
                mock_pool.acquire.assert_called_once()

        # Очищаем
        DatabasePoolManager._pool = None
        DatabasePoolManager._semaphore = None

    @pytest.mark.asyncio
    async def test_close_pool(self):
        """Закрытие пула."""
        from src.database.pool import DatabasePoolManager

        # Сбрасываем состояние
        DatabasePoolManager._pool = None
        DatabasePoolManager._semaphore = None

        mock_pool = AsyncMock()
        mock_pool._closed = False

        with patch("src.database.pool.asyncpg") as mock_asyncpg:
            mock_asyncpg.create_pool = AsyncMock(return_value=mock_pool)
            await DatabasePoolManager.get_pool()

            await DatabasePoolManager.close()

            # Проверяем что close был вызван
            mock_pool.close.assert_called_once()

        # Очищаем
        DatabasePoolManager._pool = None
        DatabasePoolManager._semaphore = None
