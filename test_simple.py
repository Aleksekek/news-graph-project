#!/usr/bin/env python3
"""
Упрощенный тестовый скрипт для проверки работы.
"""

import asyncio
import sys
import time

sys.path.insert(0, "src")

from src.domain.storage.database import ArticleRepository
from src.utils.logging import setup_logging

logger = setup_logging()


def test_database_connection():
    """Простой тест подключения к БД."""
    print("🔌 Тест подключения к БД...")
    
    try:
        repo = ArticleRepository()
        
        if repo.test_connection():
            print("✅ Подключение к БД успешно")
            return True
        else:
            print("❌ Не удалось подключиться к БД")
            return False
            
    except Exception as e:
        print(f"❌ Ошибка подключения к БД: {e}")
        return False


def test_statistics():
    """Тест получения статистики."""
    print("\n📊 Тест получения статистики...")
    
    try:
        repo = ArticleRepository()
        stats = repo.get_processing_stats()
        
        print(f"✅ Статистика получена:")
        print(f"   Всего статей: {stats.get('total', 0)}")
        print(f"   Сырых: {stats.get('raw', 0)}")
        print(f"   Обработанных: {stats.get('processed', 0)}")
        
        return True
        
    except Exception as e:
        print(f"❌ Ошибка получения статистики: {e}")
        return False


async def test_lenta_parser():
    """Тест парсера Lenta.ru с упрощенным сохранением."""
    print("\n📰 Тест парсера Lenta.ru (только парсинг)...")
    
    try:
        from src.domain.parsing.factory import ParserFactory
        
        # Только парсинг, без сохранения
        async with ParserFactory.create("lenta") as parser:
            items = await parser.parse_recent(limit=2)
            
            print(f"✅ Парсинг Lenta.ru завершен")
            print(f"   Получено статей: {len(items)}")
            
            if items:
                print(f"   Пример заголовка: {items[0].title[:50]}...")
                print(f"   Пример URL: {items[0].url}")
            
            return len(items) > 0
            
    except Exception as e:
        print(f"❌ Ошибка парсера Lenta.ru: {e}")
        import traceback
        traceback.print_exc()
        return False


async def test_tinvest_parser():
    """Тест парсера Тинькофф Пульса."""
    print("\n💹 Тест парсера Тинькофф Пульса (только парсинг)...")
    
    try:
        from src.domain.parsing.factory import ParserFactory
        
        # Конфигурация с тикером
        config_overrides = {"tickers": ["SBER"]}
        
        async with ParserFactory.create("tinvest", config_overrides) as parser:
            items = await parser.parse_recent(limit=2)
            
            print(f"✅ Парсинг Тинькофф Пульса завершен")
            print(f"   Получено постов: {len(items)}")
            
            if items:
                print(f"   Пример текста: {items[0].content[:50]}...")
                print(f"   Автор: {items[0].author}")
            
            return len(items) > 0
            
    except Exception as e:
        print(f"❌ Ошибка парсера Тинькофф Пульса: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    """Основная тестовая функция."""
    print("🚀 Упрощенный тест системы")
    print("=" * 50)
    
    # Сначала синхронные тесты
    tests = [
        ("Подключение к БД", lambda: test_database_connection()),
        ("Статистика", lambda: test_statistics()),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            success = test_func()
            results.append((test_name, success))
            
            # Пауза между тестами
            time.sleep(1)
            
        except Exception as e:
            print(f"❌ Критическая ошибка в тесте {test_name}: {e}")
            results.append((test_name, False))
    
    # Затем асинхронные тесты
    async_tests = [
        ("Парсер Lenta.ru", test_lenta_parser),
        ("Парсер Тинькофф Пульса", test_tinvest_parser),
    ]
    
    for test_name, test_func in async_tests:
        try:
            success = await test_func()
            results.append((test_name, success))
            
            # Пауза между тестами
            await asyncio.sleep(2)
            
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