"""
Репозиторий для работы с article_entities (связи статья ↔ сущность).
"""


import asyncpg

from src.core.models import ExtractedEntity
from src.database.pool import DatabasePoolManager
from src.utils.logging import get_logger
from src.utils.retry import async_retry

logger = get_logger("database.article_entity_repository")


class ArticleEntityRepository:
    """CRUD для article_entities."""

    @async_retry(exceptions=(asyncpg.exceptions.PostgresError,), max_attempts=3, delay=1.0)
    async def save_batch(
        self,
        processed_article_id: int,
        entity_links: list[tuple[int, ExtractedEntity]],
    ) -> int:
        """
        Пакетно сохраняет связи статья-сущность.
        entity_links: список (entity_id, ExtractedEntity).
        Возвращает количество вставленных записей.
        """
        if not entity_links:
            return 0

        batch_data = [
            (
                processed_article_id,
                entity_id,
                entity.count,
                entity.importance_score,
                entity.context_snippet,
            )
            for entity_id, entity in entity_links
        ]

        async with DatabasePoolManager.connection() as conn:
            await conn.executemany(
                """
                INSERT INTO article_entities
                    (processed_article_id, entity_id, count, importance_score, context_snippet)
                VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (processed_article_id, entity_id) DO NOTHING
                """,
                batch_data,
            )
        return len(batch_data)
