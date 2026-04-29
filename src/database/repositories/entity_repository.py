"""
Репозиторий для работы с таблицей entities.
"""

import asyncpg

from src.core.models import ExtractedEntity
from src.database.pool import DatabasePoolManager
from src.utils.logging import get_logger
from src.utils.retry import async_retry

logger = get_logger("database.entity_repository")


class EntityRepository:
    """CRUD для entities."""

    @async_retry(exceptions=(asyncpg.exceptions.PostgresError,), max_attempts=3, delay=1.0)
    async def upsert(self, entity: ExtractedEntity) -> tuple[int, bool]:
        """
        Вставляет новую сущность или возвращает id существующей.
        Возвращает (entity_id, is_new).
        """
        async with DatabasePoolManager.connection() as conn:
            # ON CONFLICT DO UPDATE нужен, чтобы RETURNING всегда возвращал id.
            # Фактически данные не меняем — original_name оставляем как есть при первой встрече.
            row = await conn.fetchrow(
                """
                INSERT INTO entities (normalized_name, type, original_name)
                VALUES ($1, $2, $3)
                ON CONFLICT (normalized_name, type) DO UPDATE
                    SET normalized_name = entities.normalized_name
                RETURNING id, (xmax = 0) AS is_new
                """,
                entity.normalized_name[:500],
                entity.entity_type,
                entity.original_name[:500],
            )
            return row["id"], row["is_new"]
