"""
Тест стабильности подключений к БД с имитацией реальных нагрузок.
Запуск: python test_db_connections.py
"""

import asyncio
import time
from datetime import datetime, timedelta
from typing import List

from src.application.use_cases.parse_source import ParseSourceUseCase
from src.domain.storage.database import ArticleRepository, create_article_repository
from src.utils.logging import get_logger

logger = get_logger("test_db_connections")


class DBConnectionTestSuite:
    """Набор тестов для подключений к БД."""

    def __init__(self):
        self.use_case = ParseSourceUseCase()

    async def test_basic_connection_cycle(self):
        """Базовый цикл: открыть, сохранить, закрыть."""
        logger.info("🔄 Тест базового цикла подключения")
        try:
            # Простое сохранение
            stats = await self.use_case.execute(
                "lenta", limit=1, categories=["Политика"]
            )
            logger.info(
                f"📊 Базовый цикл: {stats.saved} сохранено, {stats.skipped} пропущено"
            )
            return True
        except Exception as e:
            logger.error(f"❌ Базовый цикл упал: {e}")
            return False

    async def test_multiple_cycles_with_delay(self):
        """Несколько циклов с паузой между ними."""
        logger.info("🔄 Тест нескольких циклов с паузой")
        results = []
        for i in range(3):
            try:
                stats = await self.use_case.execute(
                    "tinvest", limit=1, tickers=["SBER"]
                )
                logger.info(f"📊 Цикл {i+1}: {stats.saved} сохранено")
                results.append(True)
            except Exception as e:
                logger.error(f"❌ Цикл {i+1} упал: {e}")
                results.append(False)
            # Пауза 5 сек между циклами — проверка idle времени
            await asyncio.sleep(5)
        return all(results)

    async def test_concurrent_operations(self):
        """Конкурентные операции."""
        logger.info("🔄 Тест конкурентных операций")

        async def save_task(name: str):
            try:
                stats = await self.use_case.execute("lenta", limit=1)
                logger.info(f"📊 {name}: {stats.saved} сохранено")
                return True
            except Exception as e:
                logger.error(f"❌ {name} упал: {e}")
                return False

        tasks = [save_task(f"Task {i}") for i in range(3)]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return all(results)

    async def test_long_operation_with_heartbeat(self):
        """Длинная операция для проверки heartbeat."""
        logger.info("🔄 Тест длинной операции (парсинг ленты с лимитом 5)")
        start_time = time.time()
        try:
            stats = await self.use_case.execute(
                "lenta", limit=5, categories=["Политика", "Экономика"]
            )
            elapsed = time.time() - start_time
            logger.info(".2f")
            return elapsed < 60  # Не должно превышать 60 сек
        except Exception as e:
            logger.error(f"❌ Длинная операция упала: {e}")
            return False

    async def test_pool_reinit_after_error(self):
        """Имитация ошибки и реинициализации пула."""
        logger.info("🔄 Тест реинициализации пула после ошибки")
        repo = create_article_repository()
        try:
            # Сохранить нормальные статьи
            stats = await repo.save_articles_batch([])
            logger.info("📊 Пустой батч: OK")

            # Имитировать ошибку: вручную вызвать reinitialize
            from src.domain.storage.database import DatabasePoolManager

            await DatabasePoolManager.reinitialize_pool_on_error()
            logger.info("📊 Пул переинициализирован")

            # Попытаться сохранить еще
            stats = await self.use_case.execute("tinvest", limit=1)
            logger.info(f"📊 После реинита: {stats.saved} сохранено")
            return True
        except Exception as e:
            logger.error(f"❌ Реинициализация упала: {e}")
            return False

    async def run_all_tests(self):
        """Запустить все тесты."""
        logger.info("🚀 Запуск тестов стабильности БД подключений")
        tests = [
            ("Базовый цикл", self.test_basic_connection_cycle),
            ("Множественные циклы с паузой", self.test_multiple_cycles_with_delay),
            ("Конкурентные операции", self.test_concurrent_operations),
            ("Длинная операция", self.test_long_operation_with_heartbeat),
            ("Реинициализация пула", self.test_pool_reinit_after_error),
        ]

        results = {}
        for name, test_func in tests:
            logger.info(f"--- Начинаем тест: {name} ---")
            result = await test_func()
            results[name] = result
            logger.info(f"Результат: {'✅ PASS' if result else '❌ FAIL'}")
            # Небольшая пауза между тестами
            await asyncio.sleep(2)

        logger.info("📋 Результаты тестов:")
        passed = 0
        for name, result in results.items():
            status = "✅ PASS" if result else "❌ FAIL"
            logger.info(f"  {name}: {status}")
            if result:
                passed += 1

        logger.info(f"Общий результат: {passed}/{len(tests)} пройдено")
        return passed == len(tests)


async def main():
    """Основная функция тестов."""
    test_suite = DBConnectionTestSuite()
    success = await test_suite.run_all_tests()

    from src.domain.storage.database import DatabasePoolManager

    await DatabasePoolManager.close_global_pool()  # Обязательное закрытие
    logger.info("🔌 Пул закрыт")

    exit_code = 0 if success else 1
    logger.info(f"Exit code: {exit_code}")
    return exit_code


if __name__ == "__main__":
    exit_code = asyncio.run(main())
