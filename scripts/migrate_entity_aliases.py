"""
Миграция: создаёт таблицу entity_aliases и заполняет стартовыми данными.

Безопасно перезапускать: существующие алиасы обновляются (не пропускаются),
поэтому смена canonical_name (например, Банк России → ЦБ РФ) применяется.

Запуск (из корня проекта):
    python scripts/migrate_entity_aliases.py
    python scripts/migrate_entity_aliases.py --seed-only   # только сидинг (таблица уже есть)
    python scripts/migrate_entity_aliases.py --dry-run     # показать что будет применено
"""

import argparse
import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

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

# (alias_name, alias_type, canonical_name, canonical_type)
# alias_type=None => применяется к любому типу сущности с таким именем
SEED_ALIASES: list[tuple[str, str | None, str, str]] = [
    # ===== РОССИЯ =====
    ("РФ",                          "location",     "Россия",                               "location"),
    ("Российская Федерация",        "location",     "Россия",                               "location"),
    ("России",                      "location",     "Россия",                               "location"),
    ("России",                      "organization", "Россия",                               "organization"),

    # ===== ЦБ РФ =====
    ("Банк России",                 "organization", "ЦБ РФ",                                "organization"),
    ("ЦБ",                          "organization", "ЦБ РФ",                                "organization"),
    ("ЦБ России",                   "organization", "ЦБ РФ",                                "organization"),
    ("Центробанк",                  "organization", "ЦБ РФ",                                "organization"),
    ("Центральный банк",            "organization", "ЦБ РФ",                                "organization"),
    ("Центральный банк России",     "organization", "ЦБ РФ",                                "organization"),

    # ===== ПЕРСОНЫ: короткое ↔ полное имя =====
    ("Путин",                       "person",       "Владимир Путин",                       "person"),
    ("Трамп",                       "person",       "Дональд Трамп",                        "person"),
    ("Зеленский",                   "person",       "Владимир Зеленский",                   "person"),
    ("Песков",                      "person",       "Дмитрий Песков",                       "person"),
    ("Набиуллина",                  "person",       "Эльвира Набиуллина",                   "person"),
    ("Мишустин",                    "person",       "Михаил Мишустин",                      "person"),
    ("Силуанов",                    "person",       "Антон Силуанов",                       "person"),
    ("Лавров",                      "person",       "Сергей Лавров",                        "person"),
    ("Медведев",                    "person",       "Дмитрий Медведев",                     "person"),
    ("Байден",                      "person",       "Джо Байден",                           "person"),
    ("Макрон",                      "person",       "Эмманюэль Макрон",                     "person"),
    ("Шольц",                       "person",       "Олаф Шольц",                           "person"),
    ("Костин",                      "person",       "Андрей Костин",                        "person"),

    # ===== ГОСУДАРСТВЕННАЯ ДУМА =====
    ("Госдума",                     "organization", "Государственная дума",                 "organization"),
    ("ГД",                          "organization", "Государственная дума",                 "organization"),
    ("Государственной думы",        "organization", "Государственная дума",                 "organization"),

    # ===== СОВЕТ ФЕДЕРАЦИИ =====
    ("СФ",                          "organization", "Совет Федерации",                      "organization"),

    # ===== ПРАВИТЕЛЬСТВО =====
    ("Правительство РФ",            "organization", "Правительство России",                 "organization"),
    ("Правительства РФ",            "organization", "Правительство России",                 "organization"),

    # ===== МИНИСТЕРСТВА =====
    ("Минфин",                      "organization", "Министерство финансов",                "organization"),
    ("Минфин России",               "organization", "Министерство финансов",                "organization"),
    ("Минфина",                     "organization", "Министерство финансов",                "organization"),
    ("Минобороны",                  "organization", "Министерство обороны",                 "organization"),
    ("Минобороны России",           "organization", "Министерство обороны",                 "organization"),
    ("Минэкономразвития",           "organization", "Министерство экономического развития", "organization"),
    ("МЭР",                         "organization", "Министерство экономического развития", "organization"),

    # ===== ФСБ / МВД / ФНС / ЦИК =====
    ("Федеральная служба безопасности", "organization", "ФСБ",                              "organization"),
    ("Министерство внутренних дел", "organization", "МВД",                                  "organization"),
    ("ФНС",                         "organization", "Федеральная налоговая служба",         "organization"),
    ("ЦИК",                         "organization", "Центральная избирательная комиссия",   "organization"),

    # ===== ВСУ / Армия Украины =====
    ("ВСУ",                         "organization", "Вооружённые силы Украины",             "organization"),
    ("Украина (ВСУ)",               "organization", "Вооружённые силы Украины",             "organization"),
    ("Вооруженные силы Украина",    "organization", "Вооружённые силы Украины",             "organization"),
    ("Вооруженные силы Украина (ВСУ)", "organization", "Вооружённые силы Украины",          "organization"),
    ("Вооружённые силы Украина",    "organization", "Вооружённые силы Украины",             "organization"),

    # ===== ЕВРОПЕЙСКИЙ СОЮЗ =====
    ("ЕС",                          "organization", "Европейский союз",                     "organization"),
    ("Евросоюз",                    "organization", "Европейский союз",                     "organization"),
    ("Европейский союз (ЕС)",       "organization", "Европейский союз",                     "organization"),

    # ===== МОСКОВСКАЯ БИРЖА =====
    ("Мосбиржа",                    "organization", "Московская биржа",                     "organization"),
    ("Мосбиржи",                    "organization", "Московская биржа",                     "organization"),
    ("MOEX",                        "organization", "Московская биржа",                     "organization"),

    # ===== ТИКЕРЫ TINVEST → НАЗВАНИЯ КОМПАНИЙ =====
    # Сбер (+ исправление ошибочного типа location → organization)
    ("SBER",                        "organization", "Сбер",                                 "organization"),
    ("SBER",                        "location",     "Сбер",                                 "organization"),
    ("Сбербанк",                    "organization", "Сбер",                                 "organization"),
    ("Сбербанка",                   "organization", "Сбер",                                 "organization"),
    ("Сбер",                        "location",     "Сбер",                                 "organization"),

    ("VTBR",                        "organization", "ВТБ",                                  "organization"),
    ("GAZP",                        "organization", "Газпром",                              "organization"),
    ("LKOH",                        "organization", "Лукойл",                               "organization"),
    ("GMKN",                        "organization", "Норникель",                            "organization"),
    ("Норильский никель",           "organization", "Норникель",                            "organization"),
    ("NVTK",                        "organization", "Новатэк",                              "organization"),
    ("TATN",                        "organization", "Татнефть",                             "organization"),
    ("ROSN",                        "organization", "Роснефть",                             "organization"),
    ("ALRS",                        "organization", "АЛРОСА",                               "organization"),
    ("YDEX",                        "organization", "Яндекс",                               "organization"),
    ("Yandex",                      "organization", "Яндекс",                               "organization"),
    ("OZON",                        "organization", "Ozon",                                 "organization"),
    ("Ozon",                        "organization", "Ozon",                                 "organization"),
    ("AFKS",                        "organization", "АФК Система",                          "organization"),
    ("АФК «Система»",               "organization", "АФК Система",                          "organization"),
    ("PHOR",                        "organization", "ФосАгро",                              "organization"),
    ("MGNT",                        "organization", "Магнит",                               "organization"),

    # ===== ГЕОЛОКАЦИИ =====
    ("США",                         "location",     "США",                                  "location"),
    ("Соединённые Штаты",           "location",     "США",                                  "location"),
    ("Соединенные Штаты",           "location",     "США",                                  "location"),
    ("Украины",                     "location",     "Украина",                              "location"),
    ("Москвы",                      "location",     "Москва",                               "location"),
    ("Китая",                       "location",     "Китай",                                "location"),
]


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
            print(f"  {alias_name!r} ({alias_type or 'any'}) → {canonical_name!r} ({canonical_type})")
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
