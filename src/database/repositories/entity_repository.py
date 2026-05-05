"""
Репозиторий для работы с таблицей entities.
"""

import asyncpg

from src.core.models import ExtractedEntity
from src.database.pool import DatabasePoolManager
from src.utils.logging import get_logger
from src.utils.retry import async_retry

logger = get_logger("database.entity_repository")


def _normalize_canonical_name(name: str) -> str:
    """
    Нормализация canonical перед записью в БД:
      - "ё/Ё" → "е/Е" (LLM на одной статье даёт "Артём", на другой "Артем" — сводим)
      - первая буква в верхнем регистре, если строчная
        ("налог на доходы..." → "Налог на доходы...";
         аббревиатуры "ВТБ", "КПРФ", "НДС" остаются как есть)
    Существующий UNIQUE(normalized_name, type) после этой нормализации
    сам сводит дубли через ON CONFLICT — без изменения схемы.
    """
    if not name:
        return name
    name = name.replace("ё", "е").replace("Ё", "Е")
    if name[0].islower():
        name = name[0].upper() + name[1:]
    return name


class EntityRepository:
    """CRUD для entities."""

    @async_retry(exceptions=(asyncpg.exceptions.PostgresError,), max_attempts=3, delay=1.0)
    async def upsert(self, entity: ExtractedEntity) -> tuple[int | None, bool]:
        """
        Вставляет новую сущность или возвращает id существующей.
        Перед вставкой проверяет entity_aliases: если имя — алиас, подставляет каноническое.
        Если алиас имеет canonical_type='discard', возвращает (None, False) — сущность пропускается.
        Возвращает (entity_id, is_new).
        """
        async with DatabasePoolManager.connection() as conn:
            # Проверяем алиасы: type-specific match приоритетнее type-agnostic
            alias = await conn.fetchrow(
                """
                SELECT canonical_name, canonical_type
                FROM entity_aliases
                WHERE lower(alias_name) = lower($1)
                  AND (alias_type = $2 OR alias_type IS NULL)
                ORDER BY alias_type NULLS LAST
                LIMIT 1
                """,
                entity.normalized_name,
                entity.entity_type,
            )

            resolved_name = alias["canonical_name"] if alias else entity.normalized_name[:500]
            resolved_type = alias["canonical_type"] if alias else entity.entity_type

            # Нормализация canonical: ё→е и заглавная первая буква. Это одна точка,
            # после которой ON CONFLICT (normalized_name, type) сворачивает дубли
            # "Артем"/"Артём", "налог"/"Налог", "цифровой рубль"/"Цифровой рубль".
            resolved_name = _normalize_canonical_name(resolved_name)

            if resolved_type == "discard":
                return None, False

            # ON CONFLICT DO UPDATE нужен, чтобы RETURNING всегда возвращал id.
            row = await conn.fetchrow(
                """
                INSERT INTO entities (normalized_name, type, original_name)
                VALUES ($1, $2, $3)
                ON CONFLICT (normalized_name, type) DO UPDATE
                    SET normalized_name = entities.normalized_name
                RETURNING id, (xmax = 0) AS is_new
                """,
                resolved_name,
                resolved_type,
                entity.original_name[:500],
            )
            return row["id"], row["is_new"]
