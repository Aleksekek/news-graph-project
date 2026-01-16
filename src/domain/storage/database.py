"""
Репозиторий с упрощенным управлением соединениями для Supabase.
Использует одно соединение на запрос без пула.
"""

import json
from datetime import datetime
from typing import Any, Dict, List, Optional, Set
import psycopg2
from psycopg2.extras import Json, RealDictCursor

from src.config.settings import settings
from src.core.exceptions import DatabaseError
from src.core.models import ArticleForDB, ProcessingStats
from src.utils.logging import get_logger
from src.utils.retry import retry


logger = get_logger("storage.database")


class DatabaseConnection:
    """
    Простой менеджер соединений для Supabase.
    Каждый запрос получает новое соединение и закрывает его.
    """
    
    @staticmethod
    @retry(
        exceptions=(psycopg2.OperationalError, psycopg2.InterfaceError),
        max_attempts=3,
        delay=1.0
    )
    def get_connection():
        """
        Получение нового соединения.
        Для Supabase лучше не использовать пул.
        """
        try:
            conn = psycopg2.connect(**settings.database_dict)
            logger.debug(f"✅ Соединение с БД установлено")
            return conn
        except Exception as e:
            logger.error(f"❌ Ошибка подключения к БД: {e}")
            raise DatabaseError(f"Ошибка подключения к БД: {e}")
    
    @staticmethod
    def close_connection(connection):
        """Закрытие соединения."""
        if connection and not connection.closed:
            try:
                connection.close()
                logger.debug(f"🔌 Соединение с БД закрыто")
            except Exception as e:
                logger.warning(f"Ошибка закрытия соединения: {e}")


class ArticleRepository:
    """
    Упрощенный репозиторий для работы с Supabase.
    Каждый метод открывает и закрывает соединение.
    """
    
    def __init__(self):
        self.logger = get_logger(f"{self.__class__.__name__}")
    
    # ==================== КОНТЕКСТНЫЙ МЕНЕДЖЕР ====================
    
    def _with_connection(self, func):
        """
        Декоратор для автоматического управления соединением.
        """
        def wrapper(*args, **kwargs):
            connection = None
            cursor = None
            
            try:
                # Получаем соединение
                connection = DatabaseConnection.get_connection()
                cursor = connection.cursor(cursor_factory=RealDictCursor)
                
                # Устанавливаем timeout для запросов
                cursor.execute("SET statement_timeout = 30000")  # 30 секунд
                
                # Вызываем функцию с курсором
                result = func(cursor, *args, **kwargs)
                
                # Коммитим если не было ошибок
                if not connection.closed:
                    connection.commit()
                
                return result
                
            except psycopg2.Error as e:
                # Откатываем транзакцию при ошибке
                if connection and not connection.closed:
                    connection.rollback()
                logger.error(f"❌ Ошибка БД: {e}")
                raise DatabaseError(f"Ошибка БД: {e}")
                
            finally:
                # Всегда закрываем курсор и соединение
                if cursor and not cursor.closed:
                    cursor.close()
                
                if connection:
                    DatabaseConnection.close_connection(connection)
        
        return wrapper
    
    # ==================== СОХРАНЕНИЕ СТАТЕЙ ====================
    
    def save_articles_batch(self, articles: List[ArticleForDB]) -> ProcessingStats:
        """
        Пакетное сохранение статей.
        Все статьи сохраняются в одной транзакции.
        """
        if not articles:
            return ProcessingStats()
        
        stats = ProcessingStats(total_rows=len(articles))
        
        @self._with_connection
        def save_batch_in_transaction(cursor):
            nonlocal stats
            
            for i, article in enumerate(articles):
                try:
                    # Проверяем дубликат
                    if self._check_duplicate_by_url(cursor, article.url):
                        stats.skipped += 1
                        continue
                    
                    # Сохраняем статью
                    article_id = self._save_article(cursor, article)
                    
                    if article_id:
                        stats.saved += 1
                        if stats.saved % 10 == 0:
                            self.logger.info(
                                f"Прогресс: {stats.saved}/{len(articles)} сохранено"
                            )
                    else:
                        stats.skipped += 1
                        
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
        
        try:
            return save_batch_in_transaction()
        except Exception as e:
            self.logger.error(f"❌ Ошибка пакетного сохранения: {e}")
            # Возвращаем статистику с ошибками
            return ProcessingStats(
                total_rows=len(articles),
                errors=len(articles)
            )
    
    def _save_article(self, cursor, article: ArticleForDB) -> Optional[int]:
        """
        Сохранение одной статьи.
        """
        try:
            # SQL запрос
            sql = """
            INSERT INTO raw_articles (
                source_id, original_id, url, canonical_url,
                raw_title, raw_text, raw_html, media_content,
                published_at, retrieved_at, author, language,
                headers, status
            ) VALUES (
                %s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), %s, %s, %s, %s
            )
            ON CONFLICT (url) DO NOTHING
            RETURNING id;
            """
            
            # Подготавливаем параметры
            params = self._prepare_article_params(article)
            
            # Выполняем запрос
            cursor.execute(sql, params)
            result = cursor.fetchone()
            
            if result:
                article_id = result["id"]
                self.logger.debug(f"✅ Статья сохранена: {article_id}")
                return article_id
            
            return None
            
        except psycopg2.IntegrityError:
            # Конфликт уникальности
            return None
        except Exception as e:
            self.logger.error(f"Ошибка сохранения статьи: {e}")
            raise
    
    def _prepare_article_params(self, article: ArticleForDB) -> tuple:
        """Подготовка параметров для SQL запроса."""
        # Обработка JSON полей
        media_content = self._prepare_json_field(article.media_content)
        headers = self._prepare_json_field(article.headers)
        
        # Ограничение длины полей для БД
        raw_title = (article.raw_title or "Без заголовка")[:500]
        raw_text = (article.raw_text or "")[:10000]
        raw_html = (article.raw_html or "")[:50000] if article.raw_html else None
        author = article.author[:255] if article.author else None
        
        return (
            article.source_id,
            article.original_id,
            article.url,
            article.canonical_url,
            raw_title,
            raw_text,
            raw_html,
            Json(media_content) if media_content else None,
            article.published_at,
            author,
            article.language or "ru",
            Json(headers) if headers else None,
            article.status or "raw",
        )
    
    def _prepare_json_field(self, value: Any) -> Optional[Dict]:
        """Подготовка JSON поля."""
        if value is None:
            return None
        
        if isinstance(value, dict):
            return value
        
        if isinstance(value, str):
            try:
                return json.loads(value) if value.strip() else None
            except json.JSONDecodeError:
                return None
        
        return None
    
    def _check_duplicate_by_url(self, cursor, url: str) -> bool:
        """Проверка дубликата по URL."""
        try:
            cursor.execute(
                "SELECT 1 FROM raw_articles WHERE url = %s LIMIT 1", 
                (url,)
            )
            return cursor.fetchone() is not None
        except Exception as e:
            self.logger.warning(f"Ошибка проверки дубликата: {e}")
            return False
    
    # ==================== ПОЛУЧЕНИЕ ДАННЫХ ====================
    
    def get_existing_urls_for_source(self, source_id: int) -> Set[str]:
        """Получение существующих URL для источника."""
        
        @self._with_connection
        def fetch_urls(cursor):
            cursor.execute(
                "SELECT url FROM raw_articles WHERE source_id = %s", 
                (source_id,)
            )
            urls = {row["url"] for row in cursor.fetchall()}
            
            self.logger.debug(
                f"📊 Загружено {len(urls)} URL для источника {source_id}"
            )
            return urls
        
        try:
            return fetch_urls()
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения URL: {e}")
            return set()
    
    def get_raw_articles_for_processing(
        self, limit: int = 100, status: str = "raw"
    ) -> List[Dict[str, Any]]:
        """
        Получение сырых статей для NLP обработки.
        """
        
        @self._with_connection
        def fetch_articles(cursor):
            sql = """
            SELECT 
                id, source_id, original_id, url,
                raw_title, raw_text, raw_html, media_content,
                published_at, author, language, headers
            FROM raw_articles 
            WHERE status = %s
            ORDER BY published_at 
            LIMIT %s
            FOR UPDATE SKIP LOCKED
            """
            
            cursor.execute(sql, (status, limit))
            articles = [dict(row) for row in cursor.fetchall()]
            
            self.logger.debug(f"📄 Получено {len(articles)} статей для обработки")
            return articles
        
        try:
            return fetch_articles()
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения статей: {e}")
            raise DatabaseError(f"Ошибка получения статей: {e}")
    
    # ==================== СТАТИСТИКА ====================
    
    def get_processing_stats(self, source_id: Optional[int] = None) -> Dict[str, Any]:
        """Получение статистики по обработке."""
        
        @self._with_connection
        def fetch_stats(cursor):
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
                WHERE source_id = %s
                """
                cursor.execute(sql, (source_id,))
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
                cursor.execute(sql)
            
            stats = dict(cursor.fetchone())
            self.logger.debug(f"📊 Статистика: {stats}")
            return stats
        
        try:
            return fetch_stats()
        except Exception as e:
            self.logger.error(f"❌ Ошибка получения статистики: {e}")
            return {}
    
    # ==================== ТЕСТ ПОДКЛЮЧЕНИЯ ====================
    
    def test_connection(self) -> bool:
        """Тест подключения к БД."""
        
        @self._with_connection
        def test_connection_internal(cursor):
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            return result["?column?"] == 1
        
        try:
            return test_connection_internal()
        except Exception as e:
            self.logger.error(f"❌ Тест подключения провален: {e}")
            return False
    
    # ==================== NLP ОБРАБОТКА ====================
    
    def save_processed_article(
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
        """
        
        @self._with_connection
        def save_processed(cursor):
            # 1. Сохраняем в processed_articles
            sql = """
            INSERT INTO processed_articles (
                raw_article_id, title, text, summary,
                topic, sentiment_score, sentiment_label,
                embedding, published_at, processed_at
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 
                (SELECT published_at FROM raw_articles WHERE id = %s), NOW())
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
            
            cursor.execute(
                sql,
                (
                    raw_article_id,
                    title,
                    text,
                    summary,
                    topic,
                    sentiment_score,
                    sentiment_label,
                    embedding,
                    raw_article_id,
                ),
            )
            
            processed_id = cursor.fetchone()["id"]
            
            # 2. Сохраняем сущности если есть
            if entities:
                self._save_entities_for_article(cursor, processed_id, entities)
            
            # 3. Обновляем статус в raw_articles
            cursor.execute(
                "UPDATE raw_articles SET status = 'processed' WHERE id = %s",
                (raw_article_id,),
            )
            
            self.logger.debug(f"✅ Обработана статья ID: {raw_article_id}")
            return True
        
        try:
            return save_processed()
        except Exception as e:
            self.logger.error(f"❌ Ошибка сохранения обработанной статьи: {e}")
            raise DatabaseError(f"Ошибка сохранения обработанной статьи: {e}")
    
    def _save_entities_for_article(
        self, cursor, processed_article_id: int, entities: List[Dict[str, Any]]
    ):
        """Сохранение сущностей для статьи."""
        for entity_data in entities:
            try:
                # 1. Находим или создаем сущность
                entity_id = self._get_or_create_entity(cursor, entity_data)
                
                # 2. Создаем связь
                sql = """
                INSERT INTO article_entities (
                    processed_article_id, entity_id,
                    count, importance_score, context_snippet
                ) VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (processed_article_id, entity_id) DO UPDATE SET
                    count = article_entities.count + EXCLUDED.count,
                    importance_score = EXCLUDED.importance_score,
                    context_snippet = EXCLUDED.context_snippet
                """
                
                cursor.execute(
                    sql,
                    (
                        processed_article_id,
                        entity_id,
                        entity_data.get("count", 1),
                        entity_data.get("importance_score"),
                        entity_data.get("context_snippet"),
                    ),
                )
                
            except Exception as e:
                self.logger.warning(f"⚠️ Ошибка сохранения сущности: {e}")
                continue
    
    def _get_or_create_entity(self, cursor, entity_data: Dict[str, Any]) -> int:
        """Находит или создает сущность."""
        normalized_name = entity_data["normalized_name"]
        entity_type = entity_data["type"]
        
        # Ищем существующую
        cursor.execute(
            "SELECT id FROM entities WHERE normalized_name = %s AND type = %s LIMIT 1",
            (normalized_name, entity_type),
        )
        result = cursor.fetchone()
        
        if result:
            return result["id"]
        
        # Создаем новую
        sql = """
        INSERT INTO entities (
            normalized_name, type, original_name,
            external_ids, meta
        ) VALUES (%s, %s, %s, %s, %s)
        RETURNING id
        """
        
        cursor.execute(
            sql,
            (
                normalized_name,
                entity_type,
                entity_data.get("original_name", normalized_name),
                Json(entity_data.get("external_ids")),
                Json(entity_data.get("meta")),
            ),
        )
        
        return cursor.fetchone()["id"]
    
    # ==================== УТИЛИТЫ ====================
    
    def cleanup(self):
        """
        Метод для совместимости со старым кодом.
        В новой версии ничего не делает, т.к. соединения закрываются автоматически.
        """
        pass


# Фабрика для создания репозиториев
def create_article_repository() -> ArticleRepository:
    """Создание экземпляра репозитория."""
    return ArticleRepository()