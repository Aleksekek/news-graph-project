"""
Скрипт очистки: объединяет существующие дублирующиеся сущности по таблице entity_aliases.

Для каждого алиаса в entity_aliases:
  1. Находит сущность-алиас в entities (если есть)
  2. Находит или создаёт каноническую сущность
  3. Переводит все article_entities с алиаса на каноническую сущность
  4. Удаляет сущность-алиас

Безопасно перезапускать: идемпотентен (уже слитые записи пропускаются).

Запуск (из корня проекта):
    python scripts/merge_entity_aliases.py
    python scripts/merge_entity_aliases.py --dry-run    # показать что будет слито
"""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.pool import DatabasePoolManager
from src.utils.logging import setup_logging

logger = setup_logging()


async def _find_alias_entities(conn, alias_name: str, alias_type: str | None) -> list[dict]:
    """Возвращает список записей entities, соответствующих алиасу."""
    if alias_type:
        rows = await conn.fetch(
            "SELECT id, type FROM entities WHERE normalized_name = $1 AND type = $2",
            alias_name,
            alias_type,
        )
    else:
        rows = await conn.fetch(
            "SELECT id, type FROM entities WHERE normalized_name = $1",
            alias_name,
        )
    return [dict(r) for r in rows]


async def _merge_one(
    conn,
    alias_id: int,
    canonical_name: str,
    canonical_type: str,
    dry_run: bool,
) -> int:
    """
    Объединяет одну сущность-алиас с канонической.
    Возвращает кол-во перенесённых связей.
    """
    canonical = await conn.fetchrow(
        "SELECT id FROM entities WHERE normalized_name = $1 AND type = $2",
        canonical_name,
        canonical_type,
    )

    if canonical is None:
        if dry_run:
            link_count = await conn.fetchval(
                "SELECT COUNT(*) FROM article_entities WHERE entity_id = $1", alias_id
            )
            logger.info(
                f"  [dry-run] Создать {canonical_name!r} ({canonical_type}), "
                f"перенести {link_count} связей с id={alias_id}"
            )
            return link_count
        canonical = await conn.fetchrow(
            """
            INSERT INTO entities (normalized_name, type, original_name)
            VALUES ($1, $2, $1)
            ON CONFLICT (normalized_name, type) DO UPDATE
                SET normalized_name = entities.normalized_name
            RETURNING id
            """,
            canonical_name,
            canonical_type,
        )

    canonical_id = canonical["id"]
    if canonical_id == alias_id:
        return 0

    link_count = await conn.fetchval(
        "SELECT COUNT(*) FROM article_entities WHERE entity_id = $1", alias_id
    )

    if dry_run:
        logger.info(
            f"  [dry-run] Слить id={alias_id} → id={canonical_id} "
            f"{canonical_name!r} ({canonical_type}), связей: {link_count}"
        )
        return link_count

    # Переносим связи (ON CONFLICT DO NOTHING если у канонической уже есть такая статья)
    await conn.execute(
        """
        INSERT INTO article_entities
               (processed_article_id, entity_id, count, importance_score, context_snippet)
        SELECT  processed_article_id, $2,         count, importance_score, context_snippet
        FROM    article_entities
        WHERE   entity_id = $1
        ON CONFLICT (processed_article_id, entity_id) DO NOTHING
        """,
        alias_id,
        canonical_id,
    )

    await conn.execute("DELETE FROM article_entities WHERE entity_id = $1", alias_id)
    await conn.execute("DELETE FROM entities WHERE id = $1", alias_id)

    logger.debug(
        f"Слито id={alias_id} → id={canonical_id} ({canonical_name!r}), связей: {link_count}"
    )
    return link_count


async def merge_aliases(dry_run: bool = False) -> None:
    async with DatabasePoolManager.connection() as conn:
        aliases = await conn.fetch(
            "SELECT alias_name, alias_type, canonical_name, canonical_type FROM entity_aliases"
        )
        logger.info(f"Загружено {len(aliases)} алиасов")

        total_merged = 0
        total_links_moved = 0

        for alias in aliases:
            alias_name = alias["alias_name"]
            alias_type = alias["alias_type"]
            canonical_name = alias["canonical_name"]
            canonical_type = alias["canonical_type"]

            alias_entities = await _find_alias_entities(conn, alias_name, alias_type)

            for ae in alias_entities:
                moved = await _merge_one(
                    conn,
                    ae["id"],
                    canonical_name,
                    canonical_type,
                    dry_run,
                )
                if moved > 0 or not dry_run:
                    total_merged += 1
                total_links_moved += moved

        action = "[dry-run] Будет" if dry_run else "Готово:"
        logger.info(
            f"{action} объединено сущностей={total_merged}, "
            f"перенесено связей article_entities={total_links_moved}"
        )


def main() -> None:
    parser = argparse.ArgumentParser(description="Объединение сущностей по алиасам")
    parser.add_argument("--dry-run", action="store_true", help="Только отчёт, без изменений")
    args = parser.parse_args()

    asyncio.run(merge_aliases(dry_run=args.dry_run))


if __name__ == "__main__":
    main()
