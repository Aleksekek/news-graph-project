"""
Use case: NER-обработка raw_articles.
Читает батч необработанных статей, извлекает сущности через Natasha,
сохраняет результаты в processed_articles, entities, article_entities.
"""

import asyncio

from src.config.settings import settings
from src.core.constants import SOURCE_IDS
from src.core.models import NERStats
from src.database.repositories.article_entity_repository import ArticleEntityRepository
from src.database.repositories.article_repository import ArticleRepository
from src.database.repositories.entity_repository import EntityRepository
from src.database.repositories.processed_article_repository import ProcessedArticleRepository
from src.processing.ner.factory import create_ner_client
from src.processing.ner.text_cleaner import clean_article_text
from src.utils.logging import get_logger

logger = get_logger("app.ner_processor")

_NER_FLAGS = {"ner": True, "sentiment": False, "embedding": False}
_SOURCE_NAMES_BY_ID = {v: k for k, v in SOURCE_IDS.items()}


class NERProcessor:
    """Оркестрирует NER-пайплайн для одного батча статей."""

    def __init__(self) -> None:
        self.article_repo = ArticleRepository()
        self.processed_repo = ProcessedArticleRepository()
        self.entity_repo = EntityRepository()
        self.article_entity_repo = ArticleEntityRepository()
        # NER-движок выбирается через settings.NER_ENGINE (natasha | llm)
        self.ner = create_ner_client()

    async def process_batch(
        self,
        batch_size: int | None = None,
        concurrency: int | None = None,
    ) -> NERStats:
        """
        Обрабатывает до batch_size статей со статусом 'raw'.
        Параллелизм внутри батча — через asyncio.gather + Semaphore.

        Args:
            batch_size: размер батча; default — settings.NER_BATCH_SIZE
            concurrency: одновременных обработок; default — settings.NER_BATCH_CONCURRENCY
        """
        batch_size = batch_size or settings.NER_BATCH_SIZE
        concurrency = concurrency or settings.NER_BATCH_CONCURRENCY

        stats = NERStats()
        articles = await self.article_repo.get_unprocessed(limit=batch_size)
        stats.total_articles = len(articles)

        if not articles:
            logger.info("📭 Нет новых статей для NER-обработки")
            return stats

        logger.info(
            f"🔍 Начинаю NER-обработку {len(articles)} статей "
            f"(concurrency={concurrency})"
        )

        sem = asyncio.Semaphore(concurrency)

        async def process_with_sem(article: dict) -> tuple[bool, int, int]:
            """Возвращает (success, n_entities, n_new)."""
            async with sem:
                try:
                    n_entities, n_new = await self._process_one(article)
                    return True, n_entities, n_new
                except Exception as e:
                    logger.error(
                        f"❌ Ошибка обработки статьи id={article['id']}: {e}",
                        exc_info=True,
                    )
                    # update_status на ошибку — отдельно, может тоже упасть
                    try:
                        await self.article_repo.update_status(article["id"], "failed")
                    except Exception as e2:
                        logger.error(f"   + не удалось пометить как failed: {e2}")
                    return False, 0, 0

        results = await asyncio.gather(
            *(process_with_sem(a) for a in articles)
        )

        for success, n_entities, n_new in results:
            if success:
                stats.processed += 1
                stats.total_entities += n_entities
                stats.new_entities += n_new
            else:
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

        # 1. Чистим текст (включая source-специфичные дейтлайны для ТАСС/Интерфакса)
        source_key = _SOURCE_NAMES_BY_ID.get(article.get("source_id"))
        title, text = clean_article_text(
            article.get("raw_title") or "",
            article.get("raw_text") or "",
            source=source_key,
        )

        # 2. Создаём/обновляем запись в processed_articles (идемпотентно)
        processed_id = await self.processed_repo.create(
            raw_article_id=article_id,
            title=title,
            text=text,
            published_at=article["published_at"],
        )

        # 3. Извлекаем сущности. Natasha — sync, LLMNERClient — async; duck-typing.
        result = self.ner.extract(title, text)
        entities = await result if asyncio.iscoroutine(result) else result

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
