"""
Миграция: создаёт таблицу entity_aliases и заполняет стартовыми данными.

Запуск (из корня проекта):
    python scripts/migrate_entity_aliases.py
    python scripts/migrate_entity_aliases.py --seed-only   # только сидинг (таблица уже есть)
    python scripts/migrate_entity_aliases.py --dry-run     # показать SQL без выполнения
"""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.database.pool import DatabasePoolManager
from src.utils.logging import setup_logging

logger = setup_logging()

# DDL для новой таблицы
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

# Стартовые алиасы: (alias_name, alias_type, canonical_name, canonical_type)
# alias_type=None => применяется к любому типу сущности с таким именем
SEED_ALIASES: list[tuple[str, str | None, str, str]] = [
    # Россия / РФ
    ("РФ",                      "location",     "Россия",                           "location"),
    ("Российская Федерация",    "location",     "Россия",                           "location"),
    ("России",                  "location",     "Россия",                           "location"),
    ("России",                  "organization", "Россия",                           "organization"),

    # Банк России / ЦБ
    ("ЦБ",                      "organization", "Банк России",                      "organization"),
    ("ЦБ РФ",                   "organization", "Банк России",                      "organization"),
    ("ЦБ России",               "organization", "Банк России",                      "organization"),
    ("Центробанк",              "organization", "Банк России",                      "organization"),
    ("Центральный банк",        "organization", "Банк России",                      "organization"),

    # Государственная дума
    ("Госдума",                 "organization", "Государственная дума",             "organization"),
    ("ГД",                      "organization", "Государственная дума",             "organization"),
    ("Государственной думы",    "organization", "Государственная дума",             "organization"),

    # Совет Федерации
    ("СФ",                      "organization", "Совет Федерации",                  "organization"),

    # Правительство
    ("Правительство РФ",        "organization", "Правительство России",             "organization"),
    ("Правительства РФ",        "organization", "Правительство России",             "organization"),

    # Министерство финансов
    ("Минфин",                  "organization", "Министерство финансов",            "organization"),
    ("Минфин России",           "organization", "Министерство финансов",            "organization"),
    ("Минфина",                 "organization", "Министерство финансов",            "organization"),

    # Министерство обороны
    ("Минобороны",              "organization", "Министерство обороны",             "organization"),
    ("Минобороны России",       "organization", "Министерство обороны",             "organization"),

    # Федеральная налоговая служба
    ("ФНС",                     "organization", "Федеральная налоговая служба",     "organization"),

    # ФСБ
    ("Федеральная служба безопасности", "organization", "ФСБ",                     "organization"),

    # МВД
    ("Министерство внутренних дел", "organization", "МВД",                         "organization"),

    # Центральная избирательная комиссия
    ("ЦИК",                     "organization", "Центральная избирательная комиссия", "organization"),

    # Сбербанк → Сбер
    ("Сбербанк",                "organization", "Сбер",                             "organization"),
    ("Сбербанка",               "organization", "Сбер",                             "organization"),

    # США
    ("США",                     "location",     "США",                              "location"),
    ("Соединённые Штаты",       "location",     "США",                              "location"),
    ("Соединенные Штаты",       "location",     "США",                              "location"),

    # Украина
    ("Украины",                 "location",     "Украина",                          "location"),

    # Европейский союз
    ("ЕС",                      "organization", "Европейский союз",                 "organization"),
    ("Евросоюз",                "organization", "Европейский союз",                 "organization"),

    # Москва
    ("Москвы",                  "location",     "Москва",                           "location"),
]


async def run_migration(seed_only: bool = False, dry_run: bool = False) -> None:
    if not seed_only:
        logger.info("Создание таблицы entity_aliases...")
        if dry_run:
            print(CREATE_TABLE_SQL)
        else:
            async with DatabasePoolManager.connection() as conn:
                await conn.execute(CREATE_TABLE_SQL)
            logger.info("Таблица создана (или уже существовала)")

    logger.info(f"Добавление {len(SEED_ALIASES)} алиасов...")

    if dry_run:
        for alias_name, alias_type, canonical_name, canonical_type in SEED_ALIASES:
            print(f"  {alias_name!r} ({alias_type}) → {canonical_name!r} ({canonical_type})")
        return

    inserted = 0
    skipped = 0

    async with DatabasePoolManager.connection() as conn:
        for alias_name, alias_type, canonical_name, canonical_type in SEED_ALIASES:
            result = await conn.execute(
                """
                INSERT INTO entity_aliases (alias_name, alias_type, canonical_name, canonical_type)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT DO NOTHING
                """,
                alias_name,
                alias_type,
                canonical_name,
                canonical_type,
            )
            if result == "INSERT 0 1":
                inserted += 1
            else:
                skipped += 1

    logger.info(f"Готово: добавлено={inserted}, пропущено (уже есть)={skipped}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Миграция entity_aliases")
    parser.add_argument("--seed-only", action="store_true", help="Только сидинг, без CREATE TABLE")
    parser.add_argument("--dry-run", action="store_true", help="Показать SQL без выполнения")
    args = parser.parse_args()

    asyncio.run(run_migration(seed_only=args.seed_only, dry_run=args.dry_run))


if __name__ == "__main__":
    main()
