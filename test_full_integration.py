"""
Полный интеграционный тест парсеров: последние посты + архивный парсинг с сохранением в БД.
Имитирует работу в проде для локального тестирования.
"""

import asyncio
import sys
from datetime import datetime, timedelta

sys.path.insert(0, "src")

from src.application.use_cases.parse_source import ParseSourceUseCase
from src.core.constants import LENTA_CATEGORIES
from src.domain.storage.database import ArticleRepository, DatabasePoolManager
from src.utils.logging import setup_logging

logger = setup_logging()


async def get_db_stats_before():
    """Получение статистики по БД до теста."""
    repo = ArticleRepository()
    try:
        stats = await repo.get_processing_stats()
        print(f"📊 Статистика БД до теста: {stats}")
        return stats
    except Exception as e:
        print(f"❌ Ошибка получения статистики: {e}")
        return {}


async def test_recent_posts(source_name: str, limit: int, **kwargs):
    """Тест загрузки последних k постов."""
    print(f"\n📰 Тест последних {limit} постов для {source_name}...")

    use_case = ParseSourceUseCase()
    try:
        stats = await use_case.execute(source_name=source_name, limit=limit, **kwargs)
        print(
            f"✅ {source_name} - последних постов: сохранено {stats.saved}, пропущено {stats.skipped}"
        )
        return stats
    except Exception as e:
        print(f"❌ Ошибка загрузки последних постов {source_name}: {e}")
        import traceback

        traceback.print_exc()
        return None


async def test_archive_parsing(
    source_name: str, days_back_start: int, days_back_end: int, limit: int, **kwargs
):
    """Тест архивного парсинга с n-го дня по j-й день назад."""
    print(
        f"\n📚 Тест архивного парсинга {source_name}: с {days_back_start} дня назад по {days_back_end} дня назад..."
    )

    use_case = ParseSourceUseCase()
    try:
        # Вычисляем даты
        today = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
        start_date = today - timedelta(days=days_back_start)
        end_date = today - timedelta(days=days_back_end)

        print(f"   Период: {start_date.date()} - {end_date.date()}")
        input()

        stats = await use_case.execute(
            source_name=source_name,
            limit=limit,
            start_date=start_date,
            end_date=end_date,
            **kwargs,
        )
        print(
            f"✅ {source_name} - архив: сохранено {stats.saved}, пропущено {stats.skipped}"
        )
        return stats
    except Exception as e:
        print(f"❌ Ошибка архивного парсинга {source_name}: {e}")
        import traceback

        traceback.print_exc()
        return None


async def main():
    """Основная функция тестов."""
    print("🚀 Запуск полного интеграционного теста парсеров")
    print("=" * 60)

    try:
        # Получим статистику по БД до начала
        stats_before = await get_db_stats_before()

        tests = [
            # Последние посты
            # ("Последние 5 постов Lenta (все категории)",
            # lambda: test_recent_posts("lenta", 5, categories=["Политика", "Экономика"])),
            #
            # ("Последние 5 постов TInvest (акции)",
            # lambda: test_recent_posts("tinvest", 5, tickers=["SBER", "VTBR", "GAZP"])),
            # Архивный парсинг (неделя назад)
            (
                "Архив Lenta с 7-го по 2-й день назад",
                lambda: test_archive_parsing(
                    "lenta", 5, 1, 1000000, categories=LENTA_CATEGORIES
                ),
            ),
            # ("Архив TInvest с 5-го по 1-й день назад",
            # lambda: test_archive_parsing("tinvest", 5, 1, 10, tickers=["SBER"])),
        ]

        results = []
        total_saved = 0

        for test_name, test_func in tests:
            try:
                stats = await test_func()
                if stats:
                    total_saved += stats.saved
                    success = stats.saved > 0 or stats.total_rows > 0
                else:
                    success = False
                results.append((test_name, success))

                # Небольшая пауза между тестами
                await asyncio.sleep(2)

            except Exception as e:
                print(f"❌ Критическая ошибка в тесте {test_name}: {e}")
                results.append((test_name, False))

        # Статистика после тестов
        print("\n" + "=" * 60)
        print("📋 Итоги тестирования:")

        all_passed = True
        for test_name, success in results:
            status = "✅ PASS" if success else "❌ FAIL"
            print(f"  {test_name}: {status}")
            if not success:
                all_passed = False

        print(f"\n📊 Всего сохранено статей: {total_saved}")

        # Финальная проверка статистики БД
        try:
            stats_after = await ArticleRepository().get_processing_stats()
            print(f"Статистика БД после: {stats_after}")
            new_articles = stats_after.get("total", 0) - stats_before.get("total", 0)
            print(f"Новых статей в БД: {new_articles}")
        except Exception as e:
            print(f"❌ Ошибка получения финальной статистики: {e}")

        print("\n" + "=" * 60)
        return 0 if all_passed else 1

    finally:
        # Закрытие пула в конце
        print("🔌 Закрытие глобального пула соединений...")
        try:
            await DatabasePoolManager.close_global_pool()
        except Exception:
            pass


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
