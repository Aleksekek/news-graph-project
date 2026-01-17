#!/usr/bin/env python3
"""
Обновленный тестовый скрипт для асинхронной работы.
"""

import asyncio
import sys
from datetime import datetime, timedelta

sys.path.insert(0, "src")

from src.application.use_cases.parse_source import ParseSourceUseCase
from src.domain.storage.database import ArticleRepository, DatabasePoolManager
from src.utils.logging import setup_logging

logger = setup_logging()


async def test_database_connection():
    """Тест асинхронного подключения к БД."""
    print("🔌 Тест подключения к БД...")

    try:
        repo = ArticleRepository()

        # Асинхронный тест
        if await repo.test_connection():
            print("✅ Подключение к БД успешно")
            return True
        else:
            print("❌ Не удалось подключиться к БД")
            return False

    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        return False


async def test_lenta_parser():
    """Тест парсера Lenta.ru с унифицированным интерфейсом."""
    print("\n📰 Тест парсера Lenta.ru...")

    try:
        use_case = ParseSourceUseCase()

        # Парсим с параметрами через **kwargs
        stats = await use_case.execute(
            source_name="lenta",
            limit=2,
            categories=["Политика", "Экономика", "Мир"],  # Передаем через kwargs
        )

        print(f"✅ Парсинг Lenta.ru завершен")
        print(f"   Сохранено: {stats.saved}, Пропущено: {stats.skipped}")

        return stats.saved > 0 or stats.total_rows > 0

    except Exception as e:
        print(f"❌ Ошибка парсера Lenta.ru: {e}")
        import traceback

        traceback.print_exc()
        return False


async def test_tinvest_parser():
    """Тест парсера Тинькофф Пульса с унифицированным интерфейсом."""
    print("\n💹 Тест парсера Тинькофф Пульса...")

    try:
        use_case = ParseSourceUseCase()

        # Парсим с параметрами через **kwargs
        stats = await use_case.execute(
            source_name="tinvest",
            limit=2,
            tickers=["SBER", "VTBR"],  # Передаем через kwargs
        )

        print(f"✅ Парсинг Тинькофф Пульса завершен")
        print(f"   Сохранено: {stats.saved}, Пропущено: {stats.skipped}")

        return stats.saved > 0 or stats.total_rows > 0

    except Exception as e:
        print(f"❌ Ошибка парсера Тинькофф Пульса: {e}")
        import traceback

        traceback.print_exc()
        return False


async def test_unified_interface():
    """Тест унифицированного интерфейса."""
    print("\n🎯 Тест унифицированного интерфейса...")

    try:
        from src.domain.parsing.factory import ParserFactory

        # Тест TInvest с тикерами в конструкторе
        print("  Создаем TInvest парсер с тикерами в конструкторе...")
        parser1 = ParserFactory.create("tinvest", {"tickers": ["SBER", "GAZP"]})

        # Тест TInvest с передачей тикеров в метод parse
        print("  Создаем TInvest парсер без тикеров...")
        parser2 = ParserFactory.create("tinvest")

        async with parser1:
            items1 = await parser1.parse(limit=1)
            print(f"    Парсер1 (тикеры в конструкторе): {len(items1)} постов")

        async with parser2:
            items2 = await parser2.parse(limit=1, tickers=["VTBR"])
            print(f"    Парсер2 (тикеры в методе): {len(items2)} постов")

        # Тест Lenta с категориями
        print("  Создаем Lenta парсер с категориями...")
        parser3 = ParserFactory.create("lenta", {"categories": ["Политика"]})

        async with parser3:
            items3 = await parser3.parse(limit=1)
            print(f"    Парсер3 (категории в конструкторе): {len(items3)} статей")

        return True

    except Exception as e:
        print(f"❌ Ошибка теста интерфейса: {e}")
        import traceback

        traceback.print_exc()
        return False


async def main():
    """Основная тестовая функция."""
    print("🚀 Запуск интеграционных тестов (асинхронная версия)")
    print("=" * 50)

    try:
        tests = [
            ("Подключение к БД", test_database_connection),
            ("Парсер Lenta.ru", test_lenta_parser),
            #("Парсер Тинькофф Пульса", test_tinvest_parser),
            #("Унифицированный интерфейс", test_unified_interface),
        ]

        results = []

        for test_name, test_func in tests:
            try:
                success = await test_func()
                results.append((test_name, success))

                await asyncio.sleep(1)

            except Exception as e:
                print(f"❌ Критическая ошибка в тесте {test_name}: {e}")
                results.append((test_name, False))

        print("\n" + "=" * 50)
        print("📋 Результаты тестов:")

        all_passed = True
        for test_name, success in results:
            status = "✅ PASS" if success else "❌ FAIL"
            print(f"  {test_name}: {status}")
            if not success:
                all_passed = False

        print("\n" + "=" * 50)

        return 0 if all_passed else 1

    finally:
        # Закрываем пул ТОЛЬКО в конце всех тестов
        print("🔌 Закрытие глобального пула соединений...")
        await DatabasePoolManager.close_global_pool()


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
