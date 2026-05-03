"""
Демо-скрипт для визуальной проверки парсеров.
Запускает парсер, выводит статьи в читаемом виде — можно сверить с сайтом.

Запуск:
    python scripts/demo_parsers.py                        # все три парсера по 5 статей
    python scripts/demo_parsers.py --source interfax      # только Интерфакс
    python scripts/demo_parsers.py --source tass --limit 10
    python scripts/demo_parsers.py --source rbc --full    # показать полный текст
    python scripts/demo_parsers.py --source rbc --archive # архивный парсинг (1 января 2026)

Примеры:
    python scripts/demo_parsers.py --source interfax --limit 3 --full
    python scripts/demo_parsers.py --source tass --limit 5
    python scripts/demo_parsers.py --source rbc --archive --limit 5
"""

import argparse
import asyncio
import io
import sys
import textwrap
from datetime import datetime
from pathlib import Path

# Принудительно UTF-8 для Windows-консоли
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

sys.path.insert(0, str(Path(__file__).parent.parent))

import logging

logging.disable(logging.DEBUG)  # скрываем дебаг-шум, оставляем WARNING+

from src.parsers.factory import ParserFactory

SOURCES = ["interfax", "tass", "rbc"]
PREVIEW_CHARS = 400  # символов текста в превью


def _sep(char: str = "─", width: int = 80) -> str:
    return char * width


def _print_item(i: int, item, full: bool = False) -> None:
    print(_sep())
    print(f"#{i}  {item.title}")
    print(f"    URL  : {item.url}")

    date_str = item.published_at.strftime("%d.%m.%Y %H:%M") if item.published_at else "—"
    print(f"    Дата : {date_str}  |  Длина текста: {len(item.content)} симв.")

    print()
    if full:
        content = item.content
    else:
        content = item.content[:PREVIEW_CHARS]
        if len(item.content) > PREVIEW_CHARS:
            content += f"\n... [ещё {len(item.content) - PREVIEW_CHARS} симв.]"

    for line in content.splitlines():
        if line.strip():
            print("    " + textwrap.fill(line, width=76, subsequent_indent="    "))
    print()


def _print_source_header(source: str, mode: str, count: int) -> None:
    print()
    print(_sep("═"))
    print(f"  ПАРСЕР: {source.upper()}  |  режим: {mode}  |  лимит: {count}")
    print(_sep("═"))


async def demo_parse(source: str, limit: int, full: bool, archive: bool) -> None:
    parser = ParserFactory.create(source)

    if archive:
        mode = "archive (1 янв 2026)"
        start = datetime(2026, 1, 1, 0, 0, 0)
        end = datetime(2026, 1, 1, 23, 59, 59)
    else:
        mode = "parse (свежие)"

    _print_source_header(source, mode, limit)

    async with parser:
        if archive:
            result = await parser.parse_period(start_date=start, end_date=end, limit=limit)
        else:
            result = await parser.parse(limit=limit)

    items = result.items
    if not items:
        print("  ⚠  Статьи не найдены.")
        return

    print(f"  Получено: {len(items)} статей\n")

    for i, item in enumerate(items, 1):
        _print_item(i, item, full=full)

    print(_sep("═"))
    lengths = [len(it.content) for it in items]
    avg = sum(lengths) // len(lengths)
    min_l = min(lengths)
    max_l = max(lengths)
    print(
        f"  Итого: {len(items)} статей  |  длина текста — мин: {min_l}, сред: {avg}, макс: {max_l}"
    )
    print(_sep("═"))


async def main(args: argparse.Namespace) -> None:
    sources = [args.source] if args.source else SOURCES

    for source in sources:
        try:
            await demo_parse(
                source=source,
                limit=args.limit,
                full=args.full,
                archive=args.archive,
            )
        except Exception as e:
            print(f"\n  [ОШИБКА] {source}: {e}\n")
            if args.verbose:
                import traceback

                traceback.print_exc()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Визуальная проверка парсеров Interfax / TASS / RBC"
    )
    parser.add_argument(
        "--source",
        "-s",
        choices=SOURCES,
        default=None,
        help="Источник (по умолчанию — все три)",
    )
    parser.add_argument(
        "--limit",
        "-n",
        type=int,
        default=5,
        help="Сколько статей получить (по умолчанию 5)",
    )
    parser.add_argument(
        "--full",
        "-f",
        action="store_true",
        help="Показать полный текст (по умолчанию — первые 400 симв.)",
    )
    parser.add_argument(
        "--archive",
        "-a",
        action="store_true",
        help="Архивный парсинг за 1 января 2026 вместо свежей ленты",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Подробный трейсбек при ошибках",
    )

    args = parser.parse_args()
    asyncio.run(main(args))
