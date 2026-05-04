"""
Диагностика временных зон в БД и поведения TIMESTAMPTZ.

Зачем: понять, как именно naive datetime превращается в physical UTC при записи
в TIMESTAMPTZ. Скрипт read-only (round-trip делает в TEMP таблице).

Ключевая гипотеза, которую проверяем:
  asyncpg сериализует naive datetime через ЛОКАЛЬНУЮ TZ Python-процесса,
  а не через session TIMEZONE PostgreSQL. Если так — наша конвенция "naive MSK
  в БД" безопасна только когда местная TZ процесса = MSK (Windows-разработка),
  и ЛОМАЕТСЯ в Docker (по умолчанию TZ=UTC).

Что показывает:
  1. Python local TZ и naive vs aware naive.timestamp() — поведение Python.
  2. server-default TIMEZONE  — то, что отдаёт PG без override.
  3. session TIMEZONE pool'a   — что устанавливает наш фикс в pool.py.
  4. Round-trip × 3 в каждой сессии:
        a) naive 12:00:00
        b) aware 12:00:00 MSK
        c) aware 12:00:00 UTC
     Сравнение epoch → видно, считает ли asyncpg local TZ или session TZ.
  5. Последние 3 записи raw_articles + processed_articles (epoch + UTC + MSK).
  6. Сводный диагноз: какая логика реально работает и что чинить.

Запуск:
    python scripts/check_db_timezone.py
"""

import asyncio
import io
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Win-консоль по умолчанию cp1251; принудительно UTF-8 для рамок-юникода
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
else:
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

import asyncpg

sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config.settings import settings


MSK = timezone(timedelta(hours=3))

# Контрольная точка: полдень 15 января 2026 по МСК.
# Если интерпретировать как MSK → physical 09:00 UTC → epoch 1768467600.
# Если интерпретировать как UTC → physical 12:00 UTC → epoch 1768478400.
PROBE = datetime(2026, 1, 15, 12, 0, 0)
EPOCH_IF_MSK = 1768467600
EPOCH_IF_UTC = 1768478400


async def open_conn(server_settings: dict | None = None) -> asyncpg.Connection:
    return await asyncpg.connect(
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        database=settings.DB_NAME,
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        server_settings=server_settings or {},
    )


def hr(char: str = "─", width: int = 78) -> str:
    return char * width


def classify(epoch: int) -> str:
    if epoch == EPOCH_IF_MSK:
        return "MSK-интерпретация"
    if epoch == EPOCH_IF_UTC:
        return "UTC-интерпретация"
    return f"неожиданно ({epoch})"


def show_python_env() -> None:
    print(hr())
    print("[0] Python local timezone (важно: asyncpg может опираться на неё)")
    print(hr())
    # time.tzname — пара (стандартное, летнее). time.timezone — секунды от UTC (со знаком inverted).
    print(f"  time.tzname              = {time.tzname}")
    print(f"  time.timezone (sec west) = {time.timezone}  (= UTC{-time.timezone//3600:+d}h)")
    print(f"  os.environ.get('TZ')     = {os.environ.get('TZ')!r}")
    # Поведение naive .timestamp() — это ключевое: оно интерпретирует naive как local.
    naive_epoch = int(PROBE.timestamp())
    print(f"  PROBE = {PROBE.isoformat()} (naive)")
    print(f"  PROBE.timestamp() = {naive_epoch}  → {classify(naive_epoch)}")
    print(f"  → Python трактует naive как: {classify(naive_epoch)}")
    print()


async def round_trip(conn: asyncpg.Connection, value: datetime, label: str) -> int:
    async with conn.transaction():
        await conn.execute("CREATE TEMP TABLE _tz_probe (ts TIMESTAMPTZ) ON COMMIT DROP")
        await conn.execute("INSERT INTO _tz_probe VALUES ($1)", value)
        row = await conn.fetchrow(
            """
            SELECT
                ts AS display,
                EXTRACT(EPOCH FROM ts)::bigint AS epoch,
                to_char(ts AT TIME ZONE 'UTC',           'YYYY-MM-DD HH24:MI:SS') AS as_utc,
                to_char(ts AT TIME ZONE 'Europe/Moscow', 'YYYY-MM-DD HH24:MI:SS') AS as_msk
            FROM _tz_probe
            """
        )
    epoch = row["epoch"]
    print(f"    {label}")
    print(f"      Insert:  {value!r}")
    print(f"      Display: {row['display']}")
    print(f"      EPOCH:   {epoch}  → {classify(epoch)}")
    print(f"      UTC:     {row['as_utc']}    MSK: {row['as_msk']}")
    return epoch


async def show_recent(conn: asyncpg.Connection, table: str, limit: int = 3) -> None:
    rows = await conn.fetch(
        f"""
        SELECT
            id,
            published_at AS display,
            EXTRACT(EPOCH FROM published_at)::bigint AS epoch,
            to_char(published_at AT TIME ZONE 'UTC',           'YYYY-MM-DD HH24:MI:SS') AS as_utc,
            to_char(published_at AT TIME ZONE 'Europe/Moscow', 'YYYY-MM-DD HH24:MI:SS') AS as_msk
        FROM {table}
        WHERE published_at IS NOT NULL
        ORDER BY id DESC
        LIMIT $1
        """,
        limit,
    )
    if not rows:
        print(f"  {table}: пусто")
        return
    print(f"  {table} — последние {len(rows)} записей:")
    for r in rows:
        print(f"    id={r['id']:<8} epoch={r['epoch']:<12} "
              f"display={r['display']}  UTC={r['as_utc']}  MSK={r['as_msk']}")


async def session_block(label: str, server_settings: dict | None) -> tuple[str, dict[str, int]]:
    print(hr())
    print(f"{label}")
    print(hr())
    conn = await open_conn(server_settings)
    try:
        tz = await conn.fetchval("SELECT current_setting('TimeZone')")
        now_db = await conn.fetchval("SELECT now()")
        print(f"  current_setting('TimeZone') = {tz!r}")
        print(f"  now()                       = {now_db}")
        print()
        print("  Round-trip 3 формата (один и тот же момент 12:00 МСК = 09:00 UTC):")
        results = {
            "naive": await round_trip(conn, PROBE, "naive 12:00:00"),
            "aware_msk": await round_trip(conn, PROBE.replace(tzinfo=MSK), "aware 12:00:00 +03 (MSK)"),
            "aware_utc": await round_trip(conn, PROBE.replace(tzinfo=timezone.utc), "aware 12:00:00 +00 (UTC)"),
        }
    finally:
        await conn.close()
    print()
    return tz, results


async def main() -> None:
    print()
    print(hr("═"))
    print("  Диагностика TIMEZONE для БД news_graph")
    print(hr("═"))
    print(f"  Хост: {settings.DB_HOST}:{settings.DB_PORT}  БД: {settings.DB_NAME}")
    print()

    show_python_env()

    default_tz, default_results = await session_block(
        "[1] Сессия БЕЗ server_settings (server default TZ)",
        None,
    )
    msk_tz, msk_results = await session_block(
        "[2] Сессия С timezone=Europe/Moscow (текущий pool.py)",
        {"timezone": "Europe/Moscow"},
    )

    print(hr())
    print("[3] Реальные данные (последние записи) — physical EPOCH одинаков в любой сессии")
    print(hr())
    conn = await open_conn({"timezone": "Europe/Moscow"})
    try:
        await show_recent(conn, "raw_articles")
        print()
        await show_recent(conn, "processed_articles")
    finally:
        await conn.close()
    print()

    # Сводный диагноз
    print(hr("═"))
    print("  Диагноз")
    print(hr("═"))
    print(f"  Server default TZ  : {default_tz!r}")
    print(f"  Pool override TZ   : {msk_tz!r}")
    print()
    print("  Эталоны: aware 12:00 MSK → epoch должен быть EPOCH_IF_MSK = 1768467600")
    print("           aware 12:00 UTC → epoch должен быть EPOCH_IF_UTC = 1768478400")
    print()

    # Проверка 1: aware datetime — total контроль, должен быть всегда корректным
    aware_msk_ok = (
        default_results["aware_msk"] == EPOCH_IF_MSK
        and msk_results["aware_msk"] == EPOCH_IF_MSK
    )
    aware_utc_ok = (
        default_results["aware_utc"] == EPOCH_IF_UTC
        and msk_results["aware_utc"] == EPOCH_IF_UTC
    )
    print(f"  aware MSK → корректно в обеих сессиях: {aware_msk_ok}")
    print(f"  aware UTC → корректно в обеих сессиях: {aware_utc_ok}")
    print()

    # Проверка 2: naive — что от чего зависит
    naive_default = default_results["naive"]
    naive_msk_sess = msk_results["naive"]
    if naive_default == naive_msk_sess:
        print(f"  naive → ОДИНАКОВЫЙ epoch в обеих сессиях: {naive_default} ({classify(naive_default)})")
        print("  → session TIMEZONE сервера НЕ влияет на сериализацию naive datetime.")
        print("  → Решение принимает Python (asyncpg + .timestamp() / local TZ).")
        print()
        if naive_default == EPOCH_IF_MSK:
            print("  ВЫВОД: на этой машине Python local TZ = MSK → naive интерпретируется как MSK.")
            print("    Это окружение разработчика. В Docker (где TZ=UTC) тот же naive будет")
            print("    интерпретирован как UTC → данные уезжают на 3ч в будущее физически.")
            print()
            print("  ПРАВИЛЬНЫЙ ФИКС: перед INSERT превращать naive MSK в aware MSK,")
            print("    тогда поведение перестаёт зависеть от среды (Windows / Docker / любой TZ).")
            print("    Pool override session TZ можно убрать — он бесполезен.")
        elif naive_default == EPOCH_IF_UTC:
            print("  ВЫВОД: на этой машине Python local TZ = UTC → naive интерпретируется как UTC.")
            print("    То же будет в Docker. Это значит: код, отдающий naive MSK в БД, БРОСАЕТ")
            print("    данные на 3ч в будущее.")
            print()
            print("  ПРАВИЛЬНЫЙ ФИКС: aware MSK перед INSERT (см. рекомендацию выше).")
    else:
        print(f"  naive default-session → epoch {naive_default} ({classify(naive_default)})")
        print(f"  naive moscow-session  → epoch {naive_msk_sess} ({classify(naive_msk_sess)})")
        print("  → session TIMEZONE сервера ВЛИЯЕТ на сериализацию.")
        print("    (это редкий случай, обычно asyncpg на стороне Python решает)")
    print()
    print("  Подсказка: эталонный путь — aware datetime в БД. Naive — лотерея среды.")
    print(hr("═"))


if __name__ == "__main__":
    asyncio.run(main())
