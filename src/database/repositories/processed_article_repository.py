"""
Репозиторий для работы с processed_articles.
"""

from datetime import datetime
from typing import Any

import asyncpg

from src.database.pool import DatabasePoolManager
from src.utils.datetime_utils import msk_naive_to_aware
from src.utils.logging import get_logger
from src.utils.retry import async_retry

logger = get_logger("database.processed_article_repository")


class ProcessedArticleRepository:
    """CRUD для processed_articles."""

    @async_retry(exceptions=(asyncpg.exceptions.PostgresError,), max_attempts=3, delay=1.0)
    async def create(
        self,
        raw_article_id: int,
        title: str,
        text: str,
        published_at: datetime,
    ) -> int:
        """
        Создаёт запись в processed_articles.
        При повторном вызове для того же raw_article_id обновляет processed_at и возвращает
        существующий id (идемпотентно).
        """
        async with DatabasePoolManager.connection() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO processed_articles (raw_article_id, title, text, published_at,
                    clean_text_tsvector)
                VALUES ($1, $2, $3, $4,
                    to_tsvector('russian', $2 || ' ' || $3))
                ON CONFLICT (raw_article_id) DO UPDATE
                    SET processed_at = NOW()
                RETURNING id
                """,
                raw_article_id,
                title[:10000],
                text[:100000],
                # naive MSK → aware MSK: сериализация перестаёт зависеть от
                # локальной TZ процесса (см. article_repository.save_batch)
                msk_naive_to_aware(published_at),
            )
            return row["id"]

    @async_retry(exceptions=(asyncpg.exceptions.PostgresError,), max_attempts=3, delay=1.0)
    async def update_processing_flags(self, processed_id: int, flags: dict[str, Any]) -> None:
        """Обновляет флаги этапов обработки."""
        import json

        async with DatabasePoolManager.connection() as conn:
            await conn.execute(
                "UPDATE processed_articles SET processing_flags = $1 WHERE id = $2",
                json.dumps(flags),
                processed_id,
            )

    @async_retry(exceptions=(asyncpg.exceptions.PostgresError,), max_attempts=3, delay=1.0)
    async def get_by_raw_id(self, raw_article_id: int) -> dict[str, Any] | None:
        """Возвращает processed_article по raw_article_id."""
        async with DatabasePoolManager.connection() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM processed_articles WHERE raw_article_id = $1",
                raw_article_id,
            )
            return dict(row) if row else None
