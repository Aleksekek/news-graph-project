"""
Архивный парсер: запускает parse_period для одного источника по диапазону дат.

Идёт день-за-днём, чтобы лимит применялся per-day (а не на весь диапазон).
Безопасно перезапускать: дубли по URL отсекаются на уровне save_batch
(ON CONFLICT DO NOTHING).

TInvest пропущен — у него специфичная архивная логика и она пока не делалась.

Запуск:
    python scripts/run_historical_parser.py --source rbc --from 2026-01-01 --to 2026-05-04
    python scripts/run_historical_parser.py --source tass --from 2026-01-01 --limit-per-day 1000
    python scripts/run_historical_parser.py --source interfax --from 2026-04-01 --to 2026-04-30 --dry-run
"""

import argparse
import asyncio
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.app.parse_source import ParseSourceUseCase
from src.utils.logging import setup_logging

logger = setup_logging()

ALLOWED_SOURCES = ("lenta", "interfax", "tass", "rbc")  # tinvest пока не поддерживается


def _parse_date(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d")
    except ValueError as e:
        raise argparse.ArgumentTypeError(
            f"Дата должна быть в формате YYYY-MM-DD, получено {value!r}: {e}"
        ) from e


async def run(
    source: str,
    from_date: datetime,
    to_date: datetime,
    limit_per_day: int,
    dry_run: bool,
) -> None:
    n_days = (to_date.date() - from_date.date()).days + 1
    logger.info(
        f"🚀 Архивный парсинг {source}: {from_date.date()} → {to_date.date()} "
        f"({n_days} дней, лимит {limit_per_day}/день)"
    )

    if dry_run:
        logger.info(
            f"🔍 Dry-run: было бы запущено {n_days} вызовов parse_period "
            f"(потенциально до {n_days * limit_per_day} статей)"
        )
        return

    use_case = ParseSourceUseCase()
    current = from_date
    total_saved = 0
    total_skipped = 0
    total_errors = 0
    failed_days: list[str] = []
    start_time = time.perf_counter()

    try:
        day_idx = 0
        while current <= to_date:
            day_idx += 1
            day_start = current.replace(hour=0, minute=0, second=0, microsecond=0)
            day_end = day_start + timedelta(hours=23, minutes=59, seconds=59)
            day_t0 = time.perf_counter()

            try:
                stats = await use_case.execute(
                    source_name=source,
                    start_date=day_start,
                    end_date=day_end,
                    limit=limit_per_day,
                )
                total_saved += stats.saved
                total_skipped += stats.skipped
                total_errors += stats.errors

                elapsed = time.perf_counter() - day_t0
                logger.info(
                    f"  [{day_idx:>3}/{n_days}] {current.date()}: "
                    f"saved={stats.saved}, skipped={stats.skipped}, errors={stats.errors} "
                    f"| {elapsed:.1f}с"
                )
            except Exception as e:
                logger.error(f"  [{day_idx:>3}/{n_days}] {current.date()}: ОШИБКА {e}")
                failed_days.append(str(current.date()))

            current += timedelta(days=1)

    except KeyboardInterrupt:
        logger.info("\n⚠️  Остановлено пользователем")

    elapsed_total = time.perf_counter() - start_time
    logger.info("\n" + "═" * 60)
    logger.info(f"📈 Итоги ({source}):")
    logger.info(f"   Сохранено новых:      {total_saved}")
    logger.info(f"   Пропущено (дубли):    {total_skipped}")
    logger.info(f"   Ошибок при сохранении:{total_errors}")
    logger.info(f"   Дней с ошибкой:       {len(failed_days)}")
    if failed_days:
        logger.info(f"     {failed_days[:10]}{' ...' if len(failed_days) > 10 else ''}")
    logger.info(f"   Время:                {elapsed_total:.1f}с ({elapsed_total/60:.1f} мин)")


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--source", "-s",
        required=True,
        choices=ALLOWED_SOURCES,
        help="Источник для архивного парсинга",
    )
    p.add_argument(
        "--from", "-f",
        dest="from_date",
        type=_parse_date,
        default=_parse_date("2026-01-01"),
        help="Начало периода YYYY-MM-DD (default: 2026-01-01)",
    )
    p.add_argument(
        "--to", "-t",
        dest="to_date",
        type=_parse_date,
        default=None,
        help="Конец периода YYYY-MM-DD (default: вчера)",
    )
    p.add_argument(
        "--limit-per-day", "-l",
        type=int,
        default=500,
        help="Максимум статей на день (default: 500)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="Только показать что бы делалось, без запуска",
    )
    return p.parse_args()


if __name__ == "__main__":
    args = parse_args()
    to_date = args.to_date or (datetime.now() - timedelta(days=1))
    if args.from_date > to_date:
        logger.error(f"--from ({args.from_date.date()}) > --to ({to_date.date()})")
        sys.exit(1)
    asyncio.run(
        run(
            source=args.source,
            from_date=args.from_date,
            to_date=to_date,
            limit_per_day=args.limit_per_day,
            dry_run=args.dry_run,
        )
    )
