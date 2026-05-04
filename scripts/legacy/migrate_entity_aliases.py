"""
Миграция: создаёт таблицу entity_aliases и заполняет стартовыми данными.

Безопасно перезапускать: существующие алиасы обновляются (не пропускаются),
поэтому смена canonical_name (например, Банк России → ЦБ РФ) применяется.

Запуск (из корня проекта):
    python scripts/legacy/migrate_entity_aliases.py
    python scripts/legacy/migrate_entity_aliases.py --seed-only   # только сидинг (таблица уже есть)
    python scripts/legacy/migrate_entity_aliases.py --dry-run     # показать что будет применено
"""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from entity_aliases_data import SEED_ALIASES

from src.database.pool import DatabasePoolManager
from src.utils.logging import setup_logging

logger = setup_logging()

CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS entity_aliases (
    id             SERIAL PRIMARY KEY,
    alias_name     TEXT NOT NULL,
    alias_type     TEXT,
    canonical_name TEXT NOT NULL,
    canonical_type TEXT NOT NULL,
    created_at     TIMESTAMPTZ DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_entity_aliases_unique
    ON entity_aliases (alias_name, COALESCE(alias_type, ''));

CREATE INDEX IF NOT EXISTS idx_entity_aliases_lookup
    ON entity_aliases (lower(alias_name));
"""


# UPDATE-then-INSERT: обновляет canonical_name/type если алиас уже есть
_UPSERT_SQL = """
WITH upd AS (
    UPDATE entity_aliases
       SET canonical_name = $3,
           canonical_type = $4
     WHERE alias_name = $1
       AND alias_type IS NOT DISTINCT FROM $2
     RETURNING id
)
INSERT INTO entity_aliases (alias_name, alias_type, canonical_name, canonical_type)
SELECT $1, $2, $3, $4
WHERE NOT EXISTS (SELECT 1 FROM upd);
"""


async def run_migration(seed_only: bool = False, dry_run: bool = False) -> None:
    if not seed_only:
        logger.info("Создание таблицы entity_aliases...")
        if dry_run:
            print(CREATE_TABLE_SQL)
        else:
            async with DatabasePoolManager.connection() as conn:
                await conn.execute(CREATE_TABLE_SQL)
            logger.info("Таблица создана (или уже существовала)")

    logger.info(f"Применение {len(SEED_ALIASES)} алиасов (upsert)...")

    if dry_run:
        for alias_name, alias_type, canonical_name, canonical_type in SEED_ALIASES:
            print(
                f"  {alias_name!r} ({alias_type or 'any'}) → {canonical_name!r} ({canonical_type})"
            )
        return

    async with DatabasePoolManager.connection() as conn:
        await conn.executemany(
            _UPSERT_SQL,
            [(a, t, c, ct) for a, t, c, ct in SEED_ALIASES],
        )

    logger.info(f"Готово: {len(SEED_ALIASES)} алиасов применено")


def main() -> None:
    parser = argparse.ArgumentParser(description="Миграция entity_aliases")
    parser.add_argument("--seed-only", action="store_true", help="Только сидинг, без CREATE TABLE")
    parser.add_argument("--dry-run", action="store_true", help="Показать что будет применено")
    args = parser.parse_args()

    asyncio.run(run_migration(seed_only=args.seed_only, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
