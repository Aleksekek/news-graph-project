"""
Асинхронный репозиторий для работы с Supabase.
Использует пул соединений, ограничение параллелизма и пакетные операции.
"""

import asyncio
import json
from collections import OrderedDict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any, AsyncGenerator, Dict, List, Optional, Set, Union

import asyncpg
from asyncpg.exceptions import UniqueViolationError
from asyncpg.pool import Pool

from src.config.settings import settings
from src.core.exceptions import DatabaseError, PermanentError
from src.core.models import ArticleForDB, ProcessingStats
from src.utils.logging import get_logger
from src.utils.retry import async_retry

logger = get_logger("storage.database")


class DatabasePoolManager:
    """Менеджер пула соединений с heartbeat для старых версий asyncpg."""

    _pool: Optional[Pool] = None
    _semaphore: Optional[asyncio.Semaphore] = None
    _heartbeat_tasks: Set[asyncio.Task] = set()

    @classmethod
    async def get_pool(cls) -> Pool:
        """Получение или создание пула с fallback и server_settings для таймаутов."""
        if cls._pool is None:
            cls._semaphore = asyncio.Semaphore(3)

            base_pool_kwargs = {
                "user": settings.DB_USER,
                "password": settings.DB_PASSWORD,
                "database": settings.DB_NAME,
                "host": settings.DB_HOST,
                "port": settings.DB_PORT,
                "min_size": 1,
                "max_size": 3,
                "command_timeout": 300,
                "server_settings": {
                    "statement_timeout": "300000",  # 5 мин на запрос
                    "idle_in_transaction_session_timeout": "300000",  # 5 мин в транзакции
                },
            }

            try:
                pool_kwargs = base_pool_kwargs.copy()
                pool_kwargs.update(
                    {
                        "keepalive_interval": 30,
                        "keepalive_timeout": 10,
                        "max_inactive_connection_lifetime": 0,
                    }
                )
                cls._pool = await asyncpg.create_pool(**pool_kwargs)
                logger.info("✅ Пул с keepalive создан")
            except TypeError:
                try:
                    fallback_kwargs = base_pool_kwargs.copy()
                    fallback_kwargs["max_inactive_connection_lifetime"] = 0
                    cls._pool = await asyncpg.create_pool(**fallback_kwargs)
                    logger.info("✅ Пул с max_inactive_connection_lifetime создан")

                    # Запускаем heartbeat для поддержания соединения каждые 30 сек
                    cls._start_heartbeat()
                except TypeError:
                    cls._pool = await asyncpg.create_pool(**base_pool_kwargs)
                    logger.info("✅ Пул с базовыми параметрами создан")
                    cls._start_heartbeat()
        return cls._pool

    @classmethod
    def _start_heartbeat(cls):
        """Запуск heartbeat задачи для предотвращения закрытия соединений сервером."""

        async def heartbeat():
            while True:
                await asyncio.sleep(30)
                if cls._pool and not cls._pool._closed:
                    try:
                        async with cls.connection() as conn:
                            await conn.fetchval("SELECT 1")
                        logger.debug("💓 Heartbeat: соединение поддерживается")
                    except Exception as e:
                        logger.warning(f"💔 Heartbeat исключение: {e}")
                else:
                    break

        task = asyncio.create_task(heartbeat())
        cls._heartbeat_tasks.add(task)
        task.add_done_callback(cls._heartbeat_tasks.discard)

    @classmethod
    @asynccontextmanager
    async def connection(cls) -> AsyncGenerator[asyncpg.Connection, None]:
        """Контекстный менеджер для соединения."""
        pool = await cls.get_pool()
        async with cls._semaphore:
            conn = await pool.acquire()
            try:
                yield conn
            finally:
                await pool.release(conn)

    @classmethod
    async def reinitialize_pool_on_error(cls):
        """Re-инициализация пула после небольшой паузы."""
        await asyncio.sleep(1.0)  # Пауза перед re-init
        if cls._pool:
            try:
                await cls._pool.close()
            except Exception:
                pass
        cls._pool = None
        cls._semaphore = None
        for task in cls._heartbeat_tasks:
            task.cancel()
        cls._heartbeat_tasks.clear()
        logger.info("🔄 Пул re-инициализирован после ошибки")
        await cls.get_pool()

    @classmethod
    async def close_global_pool(cls):
        """Закрытие пула и heartbeat задач."""
        for task in cls._heartbeat_tasks:
            task.cancel()
        cls._heartbeat_tasks.clear()
        if cls._pool and not cls._pool._closed:
            await cls._pool.close()
        cls._pool = None
        cls._semaphore = None
        logger.info("🔌 Глобальный пул закрыт")


class ArticleRepository:
    """Репозиторий для статей с пакетным сохранением и обработкой ошибок."""

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)

    @asynccontextmanager
    async def _transaction(self) -> AsyncGenerator[asyncpg.Connection, None]:
        """Транзакция."""
        async with DatabasePoolManager.connection() as conn:
            async with conn.transaction():
                yield conn

    @async_retry(
        exceptions=(asyncpg.exceptions.PostgresError,),
        max_attempts=3,
        delay=2.0,
    )
    async def save_articles_batch(
        self, articles: List[ArticleForDB]
    ) -> ProcessingStats:
        """Пакетное сохранение с фильтрацией дубликатов."""
        if not articles:
            return ProcessingStats()

        stats = ProcessingStats(total_rows=len(articles))

        url_to_article = OrderedDict()
        for article in articles:
            url_to_article[article.url] = article

        deduped_articles = list(url_to_article.values())
        stats.skipped = len(articles) - len(deduped_articles)
        articles = deduped_articles

        existing_urls = set()
        try:
            async with self._transaction() as conn:

                # Batch проверка дубликатов (теперь articles без внутренних дубликатов)
                source_ids = list(set(a.source_id for a in articles))

                for sid in source_ids:
                    rows = await conn.fetch(
                        "SELECT url FROM raw_articles WHERE source_id = $1", sid
                    )
                    existing_urls.update({row["url"] for row in rows})

                unique_articles = [a for a in articles if a.url not in existing_urls]
                stats.skipped += len(articles) - len(unique_articles)

                if not unique_articles:
                    return stats

                # Batch insert
                batch_data = []
                for article in unique_articles:
                    try:
                        params = await self._prepare_article_params(article)
                        batch_data.append(params)
                    except Exception as e:
                        self.logger.error(f"Ошибка подготовки: {e}")
                        stats.errors += 1

                if batch_data:
                    sql = """
                    INSERT INTO raw_articles (
                    source_id, original_id, url, raw_title, raw_text, 
                    raw_html, media_content, published_at, author, language, headers, meta_info, status
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
                    """

                    await conn.executemany(sql, batch_data)
                    stats.saved += len(batch_data)
                    self.logger.info(
                        f"📦 Batch insert: {stats.saved} сохранено, {stats.skipped} пропущено"
                    )

        except asyncpg.InterfaceError as e:
            if "connection was closed" in str(e):
                await DatabasePoolManager.reinitialize_pool_on_error()
                raise PermanentError(
                    f"Connection closed, re-init done: {e}"
                )  # Не retry
            raise
        return stats

    async def _prepare_article_params(self, article: ArticleForDB) -> List[Any]:
        """Подготовка параметров."""
        raw_title = (article.raw_title or "Без заголовка")[:500]
        raw_text = (article.raw_text or "")[:10000]
        raw_html = (article.raw_html or "")[:50000] if article.raw_html else None
        author = (article.author or "")[:255] if article.author else None
        media_content = self._prepare_json_field(article.media_content)
        headers = self._prepare_json_field(article.headers)

        return [
            article.source_id,
            article.original_id,
            article.url,
            raw_title,
            raw_text,
            raw_html,
            media_content,
            article.published_at,
            author,
            article.language or "ru",
            headers,
            article.meta_info,
            article.status or "raw",
        ]

    def _prepare_json_field(self, value: Any) -> Optional[Union[dict, list]]:
        """Обработка JSON."""
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return value
        if isinstance(value, str):
            try:
                return json.loads(value) if value.strip() else None
            except json.JSONDecodeError:
                self.logger.warning(f"Некорректный JSON: {value[:100]}...")
        return None

    # ==================== ПОЛУЧЕНИЕ ДАННЫХ ====================

    @async_retry(exceptions=asyncpg.exceptions.PostgresError, max_attempts=3, delay=1.0)
    async def get_existing_urls_for_source(self, source_id: int) -> Set[str]:
        async with DatabasePoolManager.connection() as conn:
            rows = await conn.fetch(
                "SELECT url FROM raw_articles WHERE source_id = $1", source_id
            )
            urls = {row["url"] for row in rows}
            self.logger.debug(f"📊 Загружено {len(urls)} URL для источника {source_id}")
            return urls

    @async_retry(exceptions=asyncpg.exceptions.PostgresError, max_attempts=3, delay=1.0)
    async def get_raw_articles_for_processing(
        self, limit: int = 100, status: str = "raw"
    ) -> List[Dict[str, Any]]:
        async with DatabasePoolManager.connection() as conn:
            sql = """SELECT id, source_id, original_id, url, raw_title, raw_text, raw_html, media_content,
                           published_at, author, language, headers FROM raw_articles 
                     WHERE status = $1 ORDER BY published_at LIMIT $2"""
            rows = await conn.fetch(sql, status, limit)
            articles = [dict(row) for row in rows]
            self.logger.debug(f"📄 Получено {len(articles)} статей")
            return articles

    @async_retry(exceptions=asyncpg.exceptions.PostgresError, max_attempts=3, delay=1.0)
    async def get_articles_by_days(self, days: int, limit: int = 50) -> List[Dict]:
        """Получить статьи за N дней"""
        try:
            async with DatabasePoolManager.connection() as conn:
                cutoff_date = datetime.now() - timedelta(days=days)
                query = """
                    SELECT raw_title, raw_text, published_at, author, source_id
                    FROM raw_articles 
                    WHERE published_at >= $1
                    AND status != 'failed'
                    AND LENGTH(raw_text) > 100
                    ORDER BY published_at DESC
                    LIMIT $2
                """
                rows = await conn.fetch(query, cutoff_date, limit)
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения статей за дни: {e}")
            return []

    @async_retry(exceptions=asyncpg.exceptions.PostgresError, max_attempts=3, delay=1.0)
    async def get_articles_count(self) -> int:
        """Получить общее количество статей"""
        try:
            async with DatabasePoolManager.connection() as conn:
                count = await conn.fetchval(
                    "SELECT COUNT(*) FROM raw_articles WHERE status != 'failed'"
                )
                return count or 0
        except Exception as e:
            logger.error(f"Ошибка получения количества статей: {e}")
            return 0

    @async_retry(exceptions=asyncpg.exceptions.PostgresError, max_attempts=3, delay=1.0)
    async def get_sources_stats(self) -> List[tuple]:
        """Получить статистику по источникам"""
        try:
            async with DatabasePoolManager.connection() as conn:
                rows = await conn.fetch(
                    """
                    SELECT s.name, COUNT(r.id) as count
                    FROM sources s
                    LEFT JOIN raw_articles r ON s.id = r.source_id
                    GROUP BY s.id, s.name
                    ORDER BY count DESC
                """
                )
                return [(row["name"], row["count"]) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения статистики по источникам: {e}")
            return []

    @async_retry(exceptions=asyncpg.exceptions.PostgresError, max_attempts=3, delay=1.0)
    async def get_daily_stats(self, days: int = 7) -> List[tuple]:
        """Получить ежедневную статистику за последние N дней"""
        try:
            async with DatabasePoolManager.connection() as conn:
                cutoff_date = datetime.now() - timedelta(days=days)
                rows = await conn.fetch(
                    """
                    SELECT DATE(published_at) as date, COUNT(*) as count
                    FROM raw_articles
                    WHERE published_at >= $1
                    AND status != 'failed'
                    GROUP BY DATE(published_at)
                    ORDER BY date DESC
                """,
                    cutoff_date,
                )
                return [(row["date"], row["count"]) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка получения дневной статистики: {e}")
            return []

    @async_retry(exceptions=asyncpg.exceptions.PostgresError, max_attempts=3, delay=1.0)
    async def search_articles(self, query: str, limit: int = 10) -> List[Dict]:
        """Поиск статей по ключевым словам"""
        try:
            async with DatabasePoolManager.connection() as conn:
                rows = await conn.fetch(
                    """
                    SELECT raw_title, raw_text, published_at, author, source_id
                    FROM raw_articles 
                    WHERE (raw_title ILIKE $1 OR raw_text ILIKE $1)
                    AND status != 'failed'
                    ORDER BY published_at DESC
                    LIMIT $2
                """,
                    f"%{query}%",
                    limit,
                )
                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка поиска статей: {e}")
            return []

    # ==================== СТАТИСТИКА ====================

    @async_retry(exceptions=asyncpg.exceptions.PostgresError, max_attempts=3, delay=1.0)
    async def get_processing_stats(
        self, source_id: Optional[int] = None
    ) -> Dict[str, Any]:
        async with DatabasePoolManager.connection() as conn:
            if source_id:
                sql = """SELECT COUNT(*) as total, COUNT(CASE WHEN status = 'raw' THEN 1 END) as raw,
                               COUNT(CASE WHEN status = 'processed' THEN 1 END) as processed, 
                               COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed, 
                               MIN(published_at) as oldest, MAX(published_at) as newest 
                         FROM raw_articles WHERE source_id = $1"""
                result = await conn.fetchrow(sql, source_id)
            else:
                sql = """SELECT COUNT(*) as total, COUNT(CASE WHEN status = 'raw' THEN 1 END) as raw, 
                               COUNT(CASE WHEN status = 'processed' THEN 1 END) as processed, 
                               COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed, 
                               MIN(published_at) as oldest, MAX(published_at) as newest FROM raw_articles"""
                result = await conn.fetchrow(sql)
            if result:
                return dict(result)
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

                self.logger.debug(
                    f"Тест подключения: result={result}, table_exists={test_table_exists}"
                )
                return result == 1

        except asyncpg.exceptions.PostgresError as e:
            self.logger.error(f"❌ Ошибка БД при тесте: {e}")
            return False
        except Exception as e:
            self.logger.error(f"❌ Неизвестная ошибка при тесте подключения: {e}")
            return False

    # ==================== NLP ОБРАБОТКА ====================

    @async_retry(exceptions=asyncpg.exceptions.PostgresError, max_attempts=3, delay=1.0)
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


# Фабрика для создания репозиториев
def create_article_repository() -> ArticleRepository:
    return ArticleRepository()
