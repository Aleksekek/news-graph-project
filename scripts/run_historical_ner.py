"""
Скрипт для исторической NER-обработки всех накопившихся статей.

Читает батчами все raw_articles со статусом 'raw' и прогоняет через NER.
Безопасно перезапускать: обработанные статьи пропускаются (status != 'raw').

Запуск:
    python scripts/run_historical_ner.py [--batch-size N] [--dry-run]

Примеры:
    python scripts/run_historical_ner.py                  # батч 200, реальная обработка
    python scripts/run_historical_ner.py --batch-size 50  # меньший батч, меньше нагрузки на RAM
    python scripts/run_historical_ner.py --dry-run        # только посчитать, не обрабатывать
"""

import argparse
import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.app.ner_processor import NERProcessor
from src.core.models import NERStats
from src.database.pool import DatabasePoolManager
from src.utils.logging import setup_logging

logger = setup_logging()


async def count_pending() -> int:
    """Считает количество статей, ожидающих обработки."""
    async with DatabasePoolManager.connection() as conn:
        return await conn.fetchval(
            "SELECT COUNT(*) FROM raw_articles WHERE status = 'raw'"
        )


async def run(batch_size: int, dry_run: bool) -> None:
    total_pending = await count_pending()
    logger.info(f"📊 Статей ожидает обработки: {total_pending}")

    if total_pending == 0:
        logger.info("✅ Все статьи уже обработаны")
        return

    if dry_run:
        logger.info(f"🔍 Dry-run режим. Без --dry-run будет обработано ~{total_pending} статей "
                    f"батчами по {batch_size}.")
        return

    processor = NERProcessor()
    cumulative = NERStats()
    batch_num = 0
    start_time = time.perf_counter()

    logger.info(f"🚀 Начинаю обработку батчами по {batch_size} статей...")
    logger.info("   Ctrl+C для безопасной остановки (текущий батч будет завершён)\n")

    try:
        while True:
            batch_num += 1
            batch_start = time.perf_counter()

            stats = await processor.process_batch(batch_size=batch_size)
            cumulative = cumulative.add(stats)

            if stats.total_articles == 0:
                break

            elapsed_batch = time.perf_counter() - batch_start
            elapsed_total = time.perf_counter() - start_time
            remaining = await count_pending()

            logger.info(
                f"  Батч {batch_num}: обработано={stats.processed} "
                f"ошибок={stats.failed} "
                f"сущностей={stats.total_entities} (новых={stats.new_entities}) "
                f"| {elapsed_batch:.1f}с | осталось ~{remaining} статей"
            )

    except KeyboardInterrupt:
        logger.info("\n⚠️  Остановлено пользователем")

    elapsed_total = time.perf_counter() - start_time
    remaining = await count_pending()

    logger.info("\n" + "═" * 60)
    logger.info("📈 Итоги:")
    logger.info(f"   Батчей: {batch_num}")
    logger.info(f"   Статей обработано: {cumulative.processed}")
    logger.info(f"   Статей с ошибками: {cumulative.failed}")
    logger.info(f"   Сущностей найдено: {cumulative.total_entities}")
    logger.info(f"   Новых сущностей:   {cumulative.new_entities}")
    logger.info(f"   Время: {elapsed_total:.1f}с ({elapsed_total/60:.1f} мин)")
    if cumulative.processed > 0:
        logger.info(f"   Среднее: {elapsed_total/cumulative.processed*1000:.0f} мс/статья")
    logger.info(f"   Осталось необработанных: {remaining}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Историческая NER-обработка статей")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=200,
        help="Размер батча (по умолчанию 200)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Только посчитать статьи, не обрабатывать",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    asyncio.run(run(batch_size=args.batch_size, dry_run=args.dry_run))
