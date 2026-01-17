"""
Асинхронный репозиторий для работы с Supabase.
Использует пул соединений и ограничение параллелизма.
"""

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Any, AsyncGenerator, Dict, List, Optional, Set, Union

import asyncpg
from asyncpg.exceptions import UniqueViolationError
from asyncpg.pool import Pool

from src.config.settings import settings
from src.core.exceptions import DatabaseError
from src.core.models import ArticleForDB, ProcessingStats
from src.utils.logging import get_logger
from src.utils.retry import async_retry

logger = get_logger("storage.database")


class DatabasePoolManager:
    """Менеджер пула соединений с ограничением параллелизма"""

    _pool: Optional[Pool] = None
    _semaphore: Optional[asyncio.Semaphore] = None

    @classmethod
    async def get_pool(cls) -> Pool:
        """Получение или создание пула соединений с ограничением"""
        if cls._pool is None:
            # Ограничиваем 3 одновременных соединения для Supabase
            cls._semaphore = asyncio.Semaphore(3)

            cls._pool = await asyncpg.create_pool(
                user=settings.DB_USER,
                password=settings.DB_PASSWORD,
                database=settings.DB_NAME,
                host=settings.DB_HOST,
                port=settings.DB_PORT,
                min_size=1,
                max_size=3,
                command_timeout=60,
            )
            logger.info("✅ Пул соединений с БД создан (max_size=3)")
        return cls._pool

    @classmethod
    @asynccontextmanager
    async def connection(cls) -> AsyncGenerator[asyncpg.Connection, None]:
        """
        Контекстный менеджер для получения соединения.
        Автоматически управляет acquired/released.

        Пример:
            async with DatabasePoolManager.connection() as conn:
                await conn.execute("SELECT 1")
        """
        pool = await cls.get_pool()

        # Ограничиваем одновременные соединения
        async with cls._semaphore:
            connection = await pool.acquire()
            try:
                yield connection
            finally:
                await pool.release(connection)

    @classmethod
    async def close_global_pool(cls):
        """Глобальное закрытие пула соединений (только при завершении приложения)"""
        if cls._pool and not cls._pool._closed:
            await cls._pool.close()
            cls._pool = None
            cls._semaphore = None
            logger.info("🔌 Глобальный пул соединений закрыт")


class ArticleRepository:
    """Асинхронный репозиторий для работы со статьями"""
    
    def __init__(self):
        self.logger = get_logger(f"{self.__class__.__name__}")
    
    @asynccontextmanager
    async def _transaction(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """
        Контекстный менеджер для выполнения операций в транзакции.
        
        Пример:
            async with self._transaction() as conn:
                await conn.execute("INSERT INTO ...")
        """
        async with DatabasePoolManager.connection() as conn:
            async with conn.transaction():
                yield conn

    @async_retry(
        exceptions=(asyncpg.exceptions.PostgresError,), max_attempts=3, delay=2.0
    )
    async def save_articles_batch(
        self, articles: List[ArticleForDB]
    ) -> ProcessingStats:
        """
        Пакетное сохранение статей в одной транзакции.

        Args:
            articles: Список статей для сохранения

        Returns:
            Статистика обработки
        """
        if not articles:
            return ProcessingStats()

        stats = ProcessingStats(total_rows=len(articles))

        async with self._transaction() as conn:
            for i, article in enumerate(articles):
                try:
                    # Быстрая проверка дубликата
                    is_duplicate = await self._check_duplicate_fast(conn, article.url)
                    if is_duplicate:
                        stats.skipped += 1
                        continue

                    # Сохраняем статью
                    article_id = await self._save_article(conn, article)

                    if article_id:
                        stats.saved += 1
                        # Логируем прогресс каждые 10 статей
                        if stats.saved % 10 == 0:
                            self.logger.debug(
                                f"Прогресс: {stats.saved}/{len(articles)} сохранено"
                            )
                    else:
                        stats.skipped += 1

                except UniqueViolationError:
                    # Дубликат обнаружен на уровне БД
                    stats.skipped += 1
                    self.logger.debug(f"Дубликат (уровень БД): {article.url}")
                except Exception as e:
                    stats.errors += 1
                    self.logger.error(f"❌ Ошибка сохранения статьи {i}: {e}")
                    # Продолжаем обработку остальных статей
                    continue

        self.logger.info(
            f"📦 Итог батча: {stats.saved} сохранено, "
            f"{stats.skipped} пропущено, {stats.errors} ошибок"
        )

        return stats

    async def _check_duplicate_fast(self, conn: asyncpg.Connection, url: str) -> bool:
        """
        Быстрая проверка дубликата по URL с использованием индекса.

        Args:
            conn: Соединение с БД
            url: URL для проверки

        Returns:
            True если дубликат найден
        """
        try:
            result = await conn.fetchval(
                "SELECT 1 FROM raw_articles WHERE url = $1 LIMIT 1", url
            )
            return result is not None
        except Exception as e:
            self.logger.warning(f"Ошибка проверки дубликата: {e}")
            # В случае ошибки предполагаем, что это не дубликат
            # Лучше сохранить дубликат, чем потерять данные
            return False

    async def _save_article(
        self, conn: asyncpg.Connection, article: ArticleForDB
    ) -> Optional[int]:
        """
        Сохранение одной статьи.

        Args:
            conn: Соединение с БД
            article: Статья для сохранения

        Returns:
            ID сохраненной статьи или None
        """
        try:
            # Подготавливаем параметры
            params = await self._prepare_article_params(article)

            # SQL запрос
            sql = """
            INSERT INTO raw_articles (
                source_id, original_id, url, canonical_url,
                raw_title, raw_text, raw_html, media_content,
                published_at, retrieved_at, author, language,
                headers, status
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), $10, $11, $12, $13)
            RETURNING id;
            """

            # Выполняем запрос
            result = await conn.fetchrow(sql, *params)

            if result:
                article_id = result["id"]
                self.logger.debug(f"✅ Статья сохранена: {article_id}")
                return article_id

            return None

        except UniqueViolationError:
            # Конфликт уникальности - статья уже существует
            return None
        except Exception as e:
            self.logger.error(f"Ошибка сохранения статьи: {e}")
            raise

    async def _prepare_article_params(self, article: ArticleForDB) -> List[Any]:
        """
        Подготовка параметров для SQL запроса.

        Args:
            article: Статья

        Returns:
            Список параметров
        """
        # Ограничение длины полей для БД
        raw_title = (article.raw_title or "Без заголовка")[:500]
        raw_text = (article.raw_text or "")[:10000]
        raw_html = (article.raw_html or "")[:50000] if article.raw_html else None
        author = (article.author or "")[:255] if article.author else None

        # Обработка JSON полей
        media_content = self._prepare_json_field(article.media_content)
        headers = self._prepare_json_field(article.headers)

        return [
            article.source_id,
            article.original_id,
            article.url,
            article.canonical_url,
            raw_title,
            raw_text,
            raw_html,
            media_content,
            article.published_at,
            author,
            article.language or "ru",
            headers,
            article.status or "raw",
        ]

    def _prepare_json_field(self, value: Any) -> Optional[Union[dict, list]]:
        """
        Подготовка JSON поля для сохранения в БД.

        Args:
            value: Значение поля

        Returns:
            Подготовленное значение или None
        """
        if value is None:
            return None

        if isinstance(value, (dict, list)):
            return value

        if isinstance(value, str):
            try:
                parsed = json.loads(value) if value.strip() else None
                if isinstance(parsed, (dict, list)):
                    return parsed
            except json.JSONDecodeError:
                self.logger.warning(f"Некорректный JSON: {value[:100]}...")

        return None

    # ==================== ПОЛУЧЕНИЕ ДАННЫХ ====================

    @async_retry(
        exceptions=(asyncpg.exceptions.PostgresError,), max_attempts=3, delay=1.0
    )
    async def get_existing_urls_for_source(self, source_id: int) -> Set[str]:
        """
        Получение существующих URL для источника.

        Args:
            source_id: ID источника

        Returns:
            Множество URL
        """
        async with self._pool_manager.acquire_connection() as conn:
            try:
                rows = await conn.fetch(
                    "SELECT url FROM raw_articles WHERE source_id = $1", source_id
                )
                urls = {row["url"] for row in rows}

                self.logger.debug(
                    f"📊 Загружено {len(urls)} URL для источника {source_id}"
                )
                return urls

            except Exception as e:
                self.logger.error(f"❌ Ошибка получения URL: {e}")
                return set()

    @async_retry(
        exceptions=(asyncpg.exceptions.PostgresError,), max_attempts=3, delay=1.0
    )
    async def get_raw_articles_for_processing(
        self, limit: int = 100, status: str = "raw"
    ) -> List[Dict[str, Any]]:
        """
        Получение сырых статей для NLP обработки.

        Args:
            limit: Лимит статей
            status: Статус статей

        Returns:
            Список статей
        """
        async with self._pool_manager.acquire_connection() as conn:
            try:
                sql = """
                SELECT 
                    id, source_id, original_id, url,
                    raw_title, raw_text, raw_html, media_content,
                    published_at, author, language, headers
                FROM raw_articles 
                WHERE status = $1
                ORDER BY published_at 
                LIMIT $2
                """

                rows = await conn.fetch(sql, status, limit)
                articles = [dict(row) for row in rows]

                self.logger.debug(f"📄 Получено {len(articles)} статей для обработки")
                return articles

            except Exception as e:
                self.logger.error(f"❌ Ошибка получения статей: {e}")
                raise DatabaseError(f"Ошибка получения статей: {e}")

    # ==================== СТАТИСТИКА ====================

    @async_retry(
        exceptions=(asyncpg.exceptions.PostgresError,), max_attempts=3, delay=1.0
    )
    async def get_processing_stats(
        self, source_id: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Получение статистики по обработке.

        Args:
            source_id: ID источника (опционально)

        Returns:
            Статистика
        """
        async with self._pool_manager.acquire_connection() as conn:
            try:
                if source_id:
                    sql = """
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN status = 'raw' THEN 1 END) as raw,
                        COUNT(CASE WHEN status = 'processed' THEN 1 END) as processed,
                        COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
                        MIN(published_at) as oldest,
                        MAX(published_at) as newest
                    FROM raw_articles 
                    WHERE source_id = $1
                    """
                    result = await conn.fetchrow(sql, source_id)
                else:
                    sql = """
                    SELECT 
                        COUNT(*) as total,
                        COUNT(CASE WHEN status = 'raw' THEN 1 END) as raw,
                        COUNT(CASE WHEN status = 'processed' THEN 1 END) as processed,
                        COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
                        MIN(published_at) as oldest,
                        MAX(published_at) as newest
                    FROM raw_articles
                    """
                    result = await conn.fetchrow(sql)

                if result:
                    stats = dict(result)
                    self.logger.debug(f"📊 Статистика: {stats}")
                    return stats

                return {}

            except Exception as e:
                self.logger.error(f"❌ Ошибка получения статистики: {e}")
                return {}

    # ==================== ТЕСТ ПОДКЛЮЧЕНИЯ ====================

    async def test_connection(self) -> bool:
        """Тест подключения к БД с гарантированным закрытием соединения"""
        try:
            async with DatabasePoolManager.connection() as conn:
                # Быстрый тест
                result = await conn.fetchval("SELECT 1")
                
                # Дополнительная проверка - можно ли выполнить простой запрос
                test_table_exists = await conn.fetchval(
                    "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'raw_articles')"
                )
                
                self.logger.debug(f"Тест подключения: result={result}, table_exists={test_table_exists}")
                return result == 1
                
        except asyncpg.exceptions.PostgresError as e:
            self.logger.error(f"❌ Ошибка БД при тесте: {e}")
            return False
        except Exception as e:
            self.logger.error(f"❌ Неизвестная ошибка при тесте подключения: {e}")
            return False

    # ==================== NLP ОБРАБОТКА ====================

    @async_retry(
        exceptions=(asyncpg.exceptions.PostgresError,), max_attempts=3, delay=2.0
    )
    async def save_processed_article(
        self,
        raw_article_id: int,
        title: str,
        text: str,
        summary: Optional[str] = None,
        topic: Optional[str] = None,
        sentiment_score: Optional[float] = None,
        sentiment_label: Optional[str] = None,
        embedding: Optional[List[float]] = None,
        entities: Optional[List[Dict[str, Any]]] = None,
    ) -> bool:
        """
        Сохранение обработанной статьи.

        Args:
            raw_article_id: ID сырой статьи
            title: Обработанный заголовок
            text: Обработанный текст
            summary: Резюме
            topic: Тема
            sentiment_score: Оценка тональности
            sentiment_label: Метка тональности
            embedding: Векторное представление
            entities: Список сущностей

        Returns:
            True если успешно
        """
        async with self._transaction() as conn:
            try:
                # 1. Сохраняем в processed_articles
                sql = """
                INSERT INTO processed_articles (
                    raw_article_id, title, text, summary,
                    topic, sentiment_score, sentiment_label,
                    embedding, published_at, processed_at
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, 
                    (SELECT published_at FROM raw_articles WHERE id = $9), NOW())
                ON CONFLICT (raw_article_id) DO UPDATE SET
                    title = EXCLUDED.title,
                    text = EXCLUDED.text,
                    summary = EXCLUDED.summary,
                    topic = EXCLUDED.topic,
                    sentiment_score = EXCLUDED.sentiment_score,
                    sentiment_label = EXCLUDED.sentiment_label,
                    embedding = EXCLUDED.embedding,
                    processed_at = NOW()
                RETURNING id;
                """

                result = await conn.fetchrow(
                    sql,
                    raw_article_id,
                    title,
                    text,
                    summary,
                    topic,
                    sentiment_score,
                    sentiment_label,
                    embedding,
                    raw_article_id,
                )

                if not result:
                    return False

                processed_id = result["id"]

                # 2. Сохраняем сущности если есть
                if entities:
                    await self._save_entities_for_article(conn, processed_id, entities)

                # 3. Обновляем статус в raw_articles
                await conn.execute(
                    "UPDATE raw_articles SET status = 'processed' WHERE id = $1",
                    raw_article_id,
                )

                self.logger.debug(f"✅ Обработана статья ID: {raw_article_id}")
                return True

            except Exception as e:
                self.logger.error(f"❌ Ошибка сохранения обработанной статьи: {e}")
                raise DatabaseError(f"Ошибка сохранения обработанной статьи: {e}")

    async def _save_entities_for_article(
        self,
        conn: asyncpg.Connection,
        processed_article_id: int,
        entities: List[Dict[str, Any]],
    ):
        """Сохранение сущностей для статьи"""
        for entity_data in entities:
            try:
                # 1. Находим или создаем сущность
                entity_id = await self._get_or_create_entity(conn, entity_data)

                # 2. Создаем связь
                sql = """
                INSERT INTO article_entities (
                    processed_article_id, entity_id,
                    count, importance_score, context_snippet
                ) VALUES ($1, $2, $3, $4, $5)
                ON CONFLICT (processed_article_id, entity_id) DO UPDATE SET
                    count = article_entities.count + EXCLUDED.count,
                    importance_score = EXCLUDED.importance_score,
                    context_snippet = EXCLUDED.context_snippet
                """

                await conn.execute(
                    sql,
                    processed_article_id,
                    entity_id,
                    entity_data.get("count", 1),
                    entity_data.get("importance_score"),
                    entity_data.get("context_snippet"),
                )

            except Exception as e:
                self.logger.warning(f"⚠️ Ошибка сохранения сущности: {e}")
                continue

    async def _get_or_create_entity(
        self, conn: asyncpg.Connection, entity_data: Dict[str, Any]
    ) -> int:
        """Находит или создает сущность"""
        normalized_name = entity_data["normalized_name"]
        entity_type = entity_data["type"]

        # Ищем существующую
        result = await conn.fetchrow(
            "SELECT id FROM entities WHERE normalized_name = $1 AND type = $2 LIMIT 1",
            normalized_name,
            entity_type,
        )

        if result:
            return result["id"]

        # Создаем новую
        sql = """
        INSERT INTO entities (
            normalized_name, type, original_name,
            external_ids, meta
        ) VALUES ($1, $2, $3, $4, $5)
        RETURNING id
        """

        result = await conn.fetchrow(
            sql,
            normalized_name,
            entity_type,
            entity_data.get("original_name", normalized_name),
            entity_data.get("external_ids"),
            entity_data.get("meta"),
        )

        return result["id"]

    # ==================== УТИЛИТЫ ====================

    async def cleanup(self):
        """
        Очистка ресурсов для ЭТОГО экземпляра репозитория.
        Не закрывает глобальный пул!
        """
        # У этой версии репозитория нет собственных соединений,
        # так как мы используем контекстные менеджеры.
        # Просто логируем для отладки.
        self.logger.debug("Очистка ресурсов репозитория")
        # НЕ закрываем глобальный пул!
        # await self._pool_manager.close_pool()  # <-- ЭТО УБИРАЕМ!


# Фабрика для создания репозиториев
def create_article_repository() -> ArticleRepository:
    """Создание экземпляра репозитория"""
    return ArticleRepository()
