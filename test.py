#!/usr/bin/env python3
"""
Обновленный тестовый скрипт.
"""

import asyncio
import sys
from datetime import datetime, timedelta

sys.path.insert(0, "src")

from src.application.use_cases.parse_source import ParseSourceUseCase
from src.domain.storage.database import ArticleRepository
from src.utils.logging import setup_logging

logger = setup_logging()


async def test_database_connection():
    """Тест подключения к БД."""
    print("🔌 Тест подключения к БД...")

    try:
        repo = ArticleRepository()

        # Простой запрос для проверки
        if repo.test_connection():
            print("✅ Подключение к БД успешно")
            return True
        else:
            print("❌ Не удалось подключиться к БД")
            return False

    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        return False


async def test_lenta_parser():
    """Тест парсера Lenta.ru."""
    print("\n📰 Тест парсера Lenta.ru...")

    try:
        use_case = ParseSourceUseCase()

        # Парсим только 2 статьи для теста
        stats = await use_case.execute(source_name="lenta", limit=2)

        print(f"✅ Парсинг Lenta.ru завершен")
        print(f"   Сохранено: {stats.saved}, Пропущено: {stats.skipped}")

        return stats.saved > 0 or stats.total_rows > 0

    except Exception as e:
        print(f"❌ Ошибка парсера Lenta.ru: {e}")
        import traceback

        traceback.print_exc()
        return False


async def test_tinvest_parser():
    """Тест парсера Тинькофф Пульса."""
    print("\n💹 Тест парсера Тинькофф Пульса...")

    try:
        use_case = ParseSourceUseCase()

        # Парсим только 2 поста для теста
        stats = await use_case.execute_with_tickers(
            source_name="tinvest", tickers=["SBER"], limit=2
        )

        print(f"✅ Парсинг Тинькофф Пульса завершен")
        print(f"   Сохранено: {stats.saved}, Пропущено: {stats.skipped}")

        return stats.saved > 0 or stats.total_rows > 0

    except Exception as e:
        print(f"❌ Ошибка парсера Тинькофф Пульса: {e}")
        import traceback

        traceback.print_exc()
        return False


async def test_nlp_worker():
    """Тест NLP воркера."""
    print("\n🧠 Тест NLP воркера...")

    try:
        from src.domain.processing.nlp_worker import SimpleNLPWorker

        worker = SimpleNLPWorker(batch_size=2)
        processed = await worker.process_batch()

        print(f"✅ NLP воркер обработал: {processed} статей")
        return processed >= 0  # Может быть 0 если нет статей для обработки

    except Exception as e:
        print(f"❌ Ошибка NLP воркера: {e}")
        import traceback

        traceback.print_exc()
        return False


async def test_statistics():
    """Тест получения статистики."""
    print("\n📊 Тест получения статистики...")

    try:
        repo = ArticleRepository()
        stats = repo.get_processing_stats()

        print(f"✅ Статистика получена:")
        print(f"   Всего статей: {stats.get('total', 0)}")
        print(f"   Сырых: {stats.get('raw', 0)}")
        print(f"   Обработанных: {stats.get('processed', 0)}")

        # Очищаем ресурсы
        repo.cleanup()

        return True

    except Exception as e:
        print(f"❌ Ошибка получения статистики: {e}")
        return False


async def main():
    """Основная тестовая функция."""
    print("🚀 Запуск интеграционных тестов")
    print("=" * 50)

    tests = [
        ("Подключение к БД", test_database_connection),
        ("Парсер Lenta.ru", test_lenta_parser),
        ("Парсер Тинькофф Пульса", test_tinvest_parser),
        ("NLP воркер", test_nlp_worker),
        ("Статистика", test_statistics),
    ]

    results = []

    for test_name, test_func in tests:
        try:
            success = await test_func()
            results.append((test_name, success))

            # Небольшая пауза между тестами
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
    if all_passed:
        print("🎉 Все тесты пройдены успешно!")
        return 0
    else:
        print("⚠️  Некоторые тесты не пройдены")
        return 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)
