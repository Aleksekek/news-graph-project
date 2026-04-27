"""
Репозиторий для работы с raw_articles.
Только операции с этой таблицей.
"""

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

import asyncpg

from src.core.models import ArticleForDB, ProcessingStats
from src.database.pool import DatabasePoolManager
from src.utils.datetime_utils import format_for_db
from src.utils.logging import get_logger
from src.utils.retry import async_retry

logger = get_logger("database.article_repository")


class ArticleRepository:
    """Репозиторий для статей."""

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)

    @async_retry(
        exceptions=(asyncpg.exceptions.PostgresError,), max_attempts=3, delay=1.0
    )
    async def save_batch(self, articles: List[ArticleForDB]) -> ProcessingStats:
        """
        Пакетное сохранение статей.

        Args:
            articles: Список статей для сохранения

        Returns:
            Статистика сохранения
        """
        if not articles:
            return ProcessingStats(total_rows=0)

        stats = ProcessingStats(total_rows=len(articles))

        # Дедупликация по URL (простая)
        unique_by_url = {}
        for article in articles:
            if article.url not in unique_by_url:
                unique_by_url[article.url] = article
            else:
                stats.skipped += 1

        unique_articles = list(unique_by_url.values())

        async with DatabasePoolManager.connection() as conn:
            # Загружаем существующие URL для этого источника
            source_ids = {a.source_id for a in unique_articles}
            existing_urls = set()

            for sid in source_ids:
                rows = await conn.fetch(
                    "SELECT url FROM raw_articles WHERE source_id = $1", sid
                )
                existing_urls.update(row["url"] for row in rows)

            # Фильтруем новые
            new_articles = [a for a in unique_articles if a.url not in existing_urls]
            stats.skipped += len(unique_articles) - len(new_articles)

            if not new_articles:
                return stats

            # Подготавливаем данные для batch insert
            batch_data = []
            for article in new_articles:
                # Приводим дату к MSK naive
                published_at = (
                    format_for_db(article.published_at)
                    if article.published_at
                    else None
                )

                batch_data.append(
                    (
                        article.source_id,
                        article.original_id,
                        article.url,
                        (article.raw_title or "Без заголовка")[:500],
                        (article.raw_text or "")[:10000],
                        (article.raw_html or "")[:50000] if article.raw_html else None,
                        self._prepare_json(article.media_content),
                        published_at,
                        (article.author or "")[:255] if article.author else None,
                        article.language or "ru",
                        self._prepare_json(article.headers),
                        article.meta_info,
                        article.status or "raw",
                    )
                )

            # Batch insert
            sql = """
                INSERT INTO raw_articles (
                    source_id, original_id, url, raw_title, raw_text,
                    raw_html, media_content, published_at, author, language,
                    headers, meta_info, status
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
            """

            await conn.executemany(sql, batch_data)
            stats.saved = len(batch_data)

            logger.info(
                f"✅ Сохранено {stats.saved} статей (пропущено {stats.skipped})"
            )

        return stats

    @async_retry(exceptions=asyncpg.exceptions.PostgresError, max_attempts=3, delay=1.0)
    async def get_existing_urls(self, source_id: int) -> Set[str]:
        """Получение всех URL для источника."""
        async with DatabasePoolManager.connection() as conn:
            rows = await conn.fetch(
                "SELECT url FROM raw_articles WHERE source_id = $1", source_id
            )
            return {row["url"] for row in rows}

    @async_retry(exceptions=asyncpg.exceptions.PostgresError, max_attempts=3, delay=1.0)
    async def get_unprocessed(
        self, limit: int = 100, status: str = "raw"
    ) -> List[Dict[str, Any]]:
        """Получение необработанных статей для NLP."""
        async with DatabasePoolManager.connection() as conn:
            rows = await conn.fetch(
                """
                SELECT id, source_id, original_id, url, raw_title, raw_text,
                       raw_html, media_content, published_at, author, language
                FROM raw_articles 
                WHERE status = $1 
                ORDER BY published_at 
                LIMIT $2
                """,
                status,
                limit,
            )
            return [dict(row) for row in rows]

    @async_retry(exceptions=asyncpg.exceptions.PostgresError, max_attempts=3, delay=1.0)
    async def mark_processed(self, article_id: int) -> bool:
        """Помечает статью как обработанную."""
        async with DatabasePoolManager.connection() as conn:
            result = await conn.execute(
                "UPDATE raw_articles SET status = 'processed' WHERE id = $1", article_id
            )
            return result == "UPDATE 1"

    @async_retry(exceptions=asyncpg.exceptions.PostgresError, max_attempts=3, delay=1.0)
    async def get_stats(self) -> Dict[str, Any]:
        """Общая статистика по статьям."""
        async with DatabasePoolManager.connection() as conn:
            row = await conn.fetchrow(
                """
                SELECT 
                    COUNT(*) as total,
                    COUNT(CASE WHEN status = 'raw' THEN 1 END) as raw,
                    COUNT(CASE WHEN status = 'processed' THEN 1 END) as processed,
                    COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
                    MIN(published_at) as oldest,
                    MAX(published_at) as newest
                FROM raw_articles
                """
            )
            return dict(row) if row else {}

    @async_retry(exceptions=asyncpg.exceptions.PostgresError, max_attempts=3, delay=1.0)
    async def search(
        self, query: str, limit: int = 10, with_urls: bool = False
    ) -> List[Dict]:
        """Поиск статей по тексту."""
        async with DatabasePoolManager.connection() as conn:
            url_field = ", url" if with_urls else ""
            sql = f"""
                SELECT raw_title, raw_text, published_at, author, source_id {url_field}
                FROM raw_articles 
                WHERE (raw_title ILIKE $1 OR raw_text ILIKE $1)
                AND status != 'failed'
                ORDER BY published_at DESC
                LIMIT $2
            """
            rows = await conn.fetch(sql, f"%{query}%", limit)
            return [dict(row) for row in rows]

    @staticmethod
    def _prepare_json(value: Any) -> Any:
        """Подготовка JSON поля для asyncpg."""
        if value is None:
            return None
        if isinstance(value, (dict, list)):
            return value
        if isinstance(value, str) and value.strip():
            try:
                import json

                return json.loads(value)
            except:
                return None
        return None
