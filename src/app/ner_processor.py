"""
Use case: NER-обработка raw_articles.
Читает батч необработанных статей, извлекает сущности через Natasha,
сохраняет результаты в processed_articles, entities, article_entities.
"""

from src.core.models import NERStats
from src.database.repositories.article_entity_repository import ArticleEntityRepository
from src.database.repositories.article_repository import ArticleRepository
from src.database.repositories.entity_repository import EntityRepository
from src.database.repositories.processed_article_repository import ProcessedArticleRepository
from src.processing.ner.natasha_client import NatashaClient
from src.processing.ner.text_cleaner import clean_article_text
from src.utils.logging import get_logger

logger = get_logger("app.ner_processor")

_NER_FLAGS = {"ner": True, "sentiment": False, "embedding": False}


class NERProcessor:
    """Оркестрирует NER-пайплайн для одного батча статей."""

    def __init__(self) -> None:
        self.article_repo = ArticleRepository()
        self.processed_repo = ProcessedArticleRepository()
        self.entity_repo = EntityRepository()
        self.article_entity_repo = ArticleEntityRepository()
        self.natasha = NatashaClient()

    async def process_batch(self, batch_size: int = 100) -> NERStats:
        """
        Обрабатывает до batch_size статей со статусом 'raw'.
        Возвращает статистику прогона.
        """
        stats = NERStats()

        articles = await self.article_repo.get_unprocessed(limit=batch_size)
        stats.total_articles = len(articles)

        if not articles:
            logger.info("📭 Нет новых статей для NER-обработки")
            return stats

        logger.info(f"🔍 Начинаю NER-обработку {len(articles)} статей")

        for article in articles:
            try:
                n_entities, n_new = await self._process_one(article)
                stats.processed += 1
                stats.total_entities += n_entities
                stats.new_entities += n_new
            except Exception as e:
                logger.error(
                    f"❌ Ошибка обработки статьи id={article['id']}: {e}", exc_info=True
                )
                await self.article_repo.update_status(article["id"], "failed")
                stats.failed += 1

        logger.info(
            f"✅ NER завершён: обработано={stats.processed}, "
            f"ошибок={stats.failed}, "
            f"сущностей={stats.total_entities} (новых={stats.new_entities})"
        )
        return stats

    async def _process_one(self, article: dict) -> tuple[int, int]:
        """
        Обрабатывает одну статью.
        Возвращает (кол-во сущностей, кол-во новых сущностей).
        """
        article_id = article["id"]

        # 1. Чистим текст
        title, text = clean_article_text(
            article.get("raw_title") or "",
            article.get("raw_text") or "",
        )

        # 2. Создаём/обновляем запись в processed_articles (идемпотентно)
        processed_id = await self.processed_repo.create(
            raw_article_id=article_id,
            title=title,
            text=text,
            published_at=article["published_at"],
        )

        # 3. Извлекаем сущности (синхронный вызов Natasha)
        entities = self.natasha.extract(title, text)

        # 4. Сохраняем сущности и связи
        entity_links = []
        n_new = 0
        for entity in entities:
            entity_id, is_new = await self.entity_repo.upsert(entity)
            if entity_id is None:
                continue
            if is_new:
                n_new += 1
            entity_links.append((entity_id, entity))

        await self.article_entity_repo.save_batch(processed_id, entity_links)

        # 5. Обновляем флаги и статус raw_articles
        await self.processed_repo.update_processing_flags(processed_id, _NER_FLAGS)
        await self.article_repo.update_status(article_id, "processed")

        return len(entities), n_new
