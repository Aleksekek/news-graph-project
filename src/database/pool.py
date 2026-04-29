"""
Менеджер пула соединений с PostgreSQL.
Вынесен отдельно для использования в любых репозиториях.
"""

import asyncio
import contextlib
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager

import asyncpg
from asyncpg.pool import Pool

from src.config.settings import settings
from src.utils.logging import get_logger

logger = get_logger("database.pool")


class DatabasePoolManager:
    """
    Менеджер пула соединений.
    Технический класс, не содержит бизнес-логики.
    """

    _pool: Pool | None = None
    _semaphore: asyncio.Semaphore | None = None
    _heartbeat_tasks: set[asyncio.Task] = set()

    @classmethod
    async def get_pool(cls) -> Pool:
        """
        Получение или создание пула соединений.
        Использует keepalive если поддерживается.
        """
        if cls._pool is None:
            cls._semaphore = asyncio.Semaphore(3)

            # Базовые параметры пула
            pool_kwargs = {
                "user": settings.DB_USER,
                "password": settings.DB_PASSWORD,
                "database": settings.DB_NAME,
                "host": settings.DB_HOST,
                "port": settings.DB_PORT,
                "min_size": 1,
                "max_size": 3,
                "command_timeout": 300,
                "server_settings": {
                    "statement_timeout": "300000",  # 5 минут
                    "idle_in_transaction_session_timeout": "300000",
                },
            }

            try:
                # Пробуем с keepalive (если asyncpg поддерживает)
                cls._pool = await asyncpg.create_pool(
                    **pool_kwargs,
                    keepalive_interval=30,
                    keepalive_timeout=10,
                    max_inactive_connection_lifetime=0,
                )
                logger.info("✅ Пул создан с keepalive")
            except TypeError:
                try:
                    # Fallback: без keepalive, но с max_inactive
                    cls._pool = await asyncpg.create_pool(
                        **pool_kwargs,
                        max_inactive_connection_lifetime=0,
                    )
                    logger.info("✅ Пул создан с max_inactive_connection_lifetime")
                    cls._start_heartbeat()
                except TypeError:
                    # Простой пул
                    cls._pool = await asyncpg.create_pool(**pool_kwargs)
                    logger.info("✅ Пул создан с базовыми параметрами")
                    cls._start_heartbeat()

        return cls._pool

    @classmethod
    def _start_heartbeat(cls):
        """Запуск heartbeat для поддержания соединений."""

        async def heartbeat():
            while True:
                await asyncio.sleep(30)
                if cls._pool and not cls._pool._closed:
                    try:
                        async with cls.connection() as conn:
                            await conn.fetchval("SELECT 1")
                        logger.debug("💓 Heartbeat: соединение живо")
                    except Exception as e:
                        logger.warning(f"💔 Heartbeat ошибка: {e}")
                else:
                    break

        task = asyncio.create_task(heartbeat())
        cls._heartbeat_tasks.add(task)
        task.add_done_callback(cls._heartbeat_tasks.discard)

    @classmethod
    @asynccontextmanager
    async def connection(cls) -> AsyncGenerator[asyncpg.Connection, None]:
        """
        Контекстный менеджер для получения соединения.
        Использовать во всех репозиториях.

        Пример:
            async with DatabasePoolManager.connection() as conn:
                result = await conn.fetch("SELECT ...")
        """
        pool = await cls.get_pool()

        async with cls._semaphore:
            conn = await pool.acquire()
            try:
                yield conn
            finally:
                await pool.release(conn)

    @classmethod
    async def reinitialize(cls):
        """Пересоздание пула после ошибки."""
        logger.warning("🔄 Пересоздание пула соединений...")
        await asyncio.sleep(1.0)

        if cls._pool:
            with contextlib.suppress(Exception):
                await cls._pool.close()

        cls._pool = None
        cls._semaphore = None

        for task in cls._heartbeat_tasks:
            task.cancel()
        cls._heartbeat_tasks.clear()

        await cls.get_pool()
        logger.info("✅ Пул пересоздан")

    @classmethod
    async def close(cls):
        """Закрытие пула (при завершении работы)."""
        logger.info("🔌 Закрытие пула соединений...")

        for task in cls._heartbeat_tasks:
            task.cancel()
        cls._heartbeat_tasks.clear()

        if cls._pool and not cls._pool._closed:
            await cls._pool.close()

        cls._pool = None
        cls._semaphore = None
