"""
Скрипт для исторической NER-обработки всех накопившихся статей.

Читает батчами все raw_articles со статусом 'raw' и прогоняет через NER.
Безопасно перезапускать: обработанные статьи пропускаются (status != 'raw').

Движок и параллелизм управляются через настройки в .env:
    NER_ENGINE=natasha|llm        - какой движок использовать
    NER_BATCH_SIZE=100            - default batch size (можно перебить через --batch-size)
    NER_BATCH_CONCURRENCY=5       - параллельных обработок внутри батча

Запуск:
    python scripts/run_historical_ner.py
    python scripts/run_historical_ner.py --batch-size 50
    python scripts/run_historical_ner.py --dry-run

Для прода LLM-движка имей в виду расход токенов: ~$0.0001/статья на v4-flash.
"""

import argparse
import asyncio
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.app.ner_processor import NERProcessor
from src.config.settings import settings
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


def _print_engine_banner(batch_size: int, total_pending: int) -> None:
    """Отчёт о настройках перед стартом — критично перед массовым LLM-прогоном."""
    engine = settings.NER_ENGINE
    concurrency = settings.NER_BATCH_CONCURRENCY
    logger.info("═" * 60)
    logger.info(f"🛠️  NER engine:        {engine}")
    logger.info(f"📦 Batch size:        {batch_size}")
    logger.info(f"⚡ Concurrency:       {concurrency}")
    logger.info(f"📊 Pending articles:  {total_pending}")
    if engine == "llm":
        # Грубая оценка: v4-flash ≈ $0.0001/article
        est_cost = total_pending * 0.0001
        logger.info(f"💸 Расход (примерно): ~${est_cost:.2f} (v4-flash, грубая оценка)")
    logger.info("═" * 60)


async def run(batch_size: int, dry_run: bool) -> None:
    total_pending = await count_pending()

    _print_engine_banner(batch_size, total_pending)

    if total_pending == 0:
        logger.info("✅ Все статьи уже обработаны")
        return

    if dry_run:
        logger.info(f"🔍 Dry-run: без --dry-run обработалось бы ~{total_pending} статей.")
        return

    processor = NERProcessor()
    cumulative = NERStats()
    batch_num = 0
    start_time = time.perf_counter()

    logger.info("🚀 Стартую. Ctrl+C для безопасной остановки (текущий батч завершится).\n")

    try:
        while True:
            batch_num += 1
            batch_start = time.perf_counter()

            stats = await processor.process_batch(batch_size=batch_size)
            cumulative = cumulative.add(stats)

            if stats.total_articles == 0:
                break

            elapsed_batch = time.perf_counter() - batch_start
            remaining = await count_pending()

            logger.info(
                f"  Батч {batch_num}: обработано={stats.processed} "
                f"ошибок={stats.failed} "
                f"сущностей={stats.total_entities} (новых={stats.new_entities}) "
                f"| {elapsed_batch:.1f}с | осталось ~{remaining}"
            )

    except KeyboardInterrupt:
        logger.info("\n⚠️  Остановлено пользователем")

    elapsed_total = time.perf_counter() - start_time
    remaining = await count_pending()

    logger.info("\n" + "═" * 60)
    logger.info("📈 Итоги:")
    logger.info(f"   Engine:            {settings.NER_ENGINE}")
    logger.info(f"   Батчей:            {batch_num}")
    logger.info(f"   Обработано:        {cumulative.processed}")
    logger.info(f"   Ошибок:            {cumulative.failed}")
    logger.info(f"   Сущностей:         {cumulative.total_entities} (новых={cumulative.new_entities})")
    logger.info(f"   Время:             {elapsed_total:.1f}с ({elapsed_total/60:.1f} мин)")
    if cumulative.processed > 0:
        logger.info(f"   Среднее:           {elapsed_total/cumulative.processed*1000:.0f} мс/статья")
    logger.info(f"   Осталось:          {remaining}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Историческая NER-обработка")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=settings.NER_BATCH_SIZE,
        help=f"Размер батча (default: settings.NER_BATCH_SIZE={settings.NER_BATCH_SIZE})",
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
