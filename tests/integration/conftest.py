"""
Conftest для integration-тестов.

DatabasePoolManager хранит пул как class-level singleton, но pytest-asyncio
по умолчанию даёт каждому тесту НОВЫЙ event loop. Пул и heartbeat-task'и
привязаны к старому (уже мёртвому) loop'у — следующий тест ловит
"cannot perform operation: another operation is in progress".

Этот autouse-фикстур закрывает пул до и после каждого теста — каждый
integration-тест начинается с чистого состояния.
"""

import contextlib

import pytest


async def _reset_pool() -> None:
    from src.database.pool import DatabasePoolManager

    if DatabasePoolManager._pool is None:
        return
    with contextlib.suppress(Exception):
        await DatabasePoolManager.close()
    # На всякий случай, если close() кинул и не довёл состояние до конца
    DatabasePoolManager._pool = None
    DatabasePoolManager._semaphore = None


@pytest.fixture(autouse=True)
async def _isolate_db_pool():
    """Сбрасывает DatabasePoolManager между тестами для изоляции event loop'ов."""
    await _reset_pool()
    yield
    await _reset_pool()
