"""
Тесты для src/app/ner_processor.py (все зависимости замокированы).
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.core.models import ExtractedEntity, NERStats


def _make_raw_article(article_id: int = 1) -> dict:
    return {
        "id": article_id,
        "raw_title": "Путин встретился с Грефом",
        "raw_text": "Президент России Владимир Путин встретился с главой Сбербанка.",
        "published_at": datetime(2026, 1, 15, 10, 0, tzinfo=timezone.utc),
    }


def _make_entity(name: str = "Путин") -> ExtractedEntity:
    return ExtractedEntity(
        original_name=name,
        normalized_name=name,
        entity_type="person",
        count=1,
        importance_score=1.0,
        context_snippet="Путин встретился с Грефом.",
    )


class TestNERProcessorBatch:
    @pytest.fixture
    def processor(self):
        from src.app.ner_processor import NERProcessor
        p = NERProcessor.__new__(NERProcessor)
        p.article_repo = AsyncMock()
        p.processed_repo = AsyncMock()
        p.entity_repo = AsyncMock()
        p.article_entity_repo = AsyncMock()
        p.natasha = MagicMock()
        return p

    @pytest.mark.asyncio
    async def test_empty_batch_returns_zero_stats(self, processor):
        processor.article_repo.get_unprocessed.return_value = []

        stats = await processor.process_batch(batch_size=50)

        assert stats.total_articles == 0
        assert stats.processed == 0
        assert stats.failed == 0
        processor.natasha.extract.assert_not_called()

    @pytest.mark.asyncio
    async def test_successful_processing_increments_stats(self, processor):
        processor.article_repo.get_unprocessed.return_value = [_make_raw_article(1)]
        processor.processed_repo.create.return_value = 10
        processor.entity_repo.upsert.return_value = (42, True)
        processor.article_entity_repo.save_batch.return_value = 1
        processor.natasha.extract.return_value = [_make_entity("Путин"), _make_entity("Сбербанк")]

        stats = await processor.process_batch()

        assert stats.total_articles == 1
        assert stats.processed == 1
        assert stats.failed == 0
        assert stats.total_entities == 2
        assert stats.new_entities == 2

    @pytest.mark.asyncio
    async def test_new_vs_existing_entities_counted_correctly(self, processor):
        processor.article_repo.get_unprocessed.return_value = [_make_raw_article()]
        processor.processed_repo.create.return_value = 1
        # Первая сущность новая, вторая уже существует
        processor.entity_repo.upsert.side_effect = [(1, True), (2, False)]
        processor.article_entity_repo.save_batch.return_value = 2
        processor.natasha.extract.return_value = [
            _make_entity("Путин"),
            _make_entity("Сбербанк"),
        ]

        stats = await processor.process_batch()

        assert stats.new_entities == 1
        assert stats.total_entities == 2

    @pytest.mark.asyncio
    async def test_failed_article_increments_failed_counter(self, processor):
        processor.article_repo.get_unprocessed.return_value = [_make_raw_article()]
        processor.processed_repo.create.side_effect = Exception("DB error")

        stats = await processor.process_batch()

        assert stats.failed == 1
        assert stats.processed == 0
        processor.article_repo.update_status.assert_called_once_with(1, "failed")

    @pytest.mark.asyncio
    async def test_one_failure_does_not_stop_other_articles(self, processor):
        articles = [_make_raw_article(1), _make_raw_article(2), _make_raw_article(3)]
        processor.article_repo.get_unprocessed.return_value = articles
        processor.processed_repo.create.side_effect = [Exception("DB error"), 10, 11]
        processor.entity_repo.upsert.return_value = (1, True)
        processor.article_entity_repo.save_batch.return_value = 1
        processor.natasha.extract.return_value = [_make_entity()]

        stats = await processor.process_batch()

        assert stats.processed == 2
        assert stats.failed == 1
        assert stats.total_articles == 3

    @pytest.mark.asyncio
    async def test_article_status_set_to_processed_on_success(self, processor):
        processor.article_repo.get_unprocessed.return_value = [_make_raw_article(7)]
        processor.processed_repo.create.return_value = 1
        processor.entity_repo.upsert.return_value = (1, False)
        processor.article_entity_repo.save_batch.return_value = 1
        processor.natasha.extract.return_value = [_make_entity()]

        await processor.process_batch()

        processor.article_repo.update_status.assert_called_once_with(7, "processed")

    @pytest.mark.asyncio
    async def test_processing_flags_updated_on_success(self, processor):
        processor.article_repo.get_unprocessed.return_value = [_make_raw_article()]
        processor.processed_repo.create.return_value = 55
        processor.entity_repo.upsert.return_value = (1, True)
        processor.article_entity_repo.save_batch.return_value = 1
        processor.natasha.extract.return_value = [_make_entity()]

        await processor.process_batch()

        processor.processed_repo.update_processing_flags.assert_called_once()
        call_args = processor.processed_repo.update_processing_flags.call_args
        assert call_args[0][0] == 55                  # processed_id
        assert call_args[0][1].get("ner") is True     # ner флаг выставлен

    @pytest.mark.asyncio
    async def test_natasha_called_with_cleaned_text(self, processor):
        """NatashaClient получает очищенный текст, а не сырой HTML."""
        article = {
            "id": 1,
            "raw_title": "<b>Заголовок</b>",
            "raw_text": "<p>Текст <em>статьи</em></p>",
            "published_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        }
        processor.article_repo.get_unprocessed.return_value = [article]
        processor.processed_repo.create.return_value = 1
        processor.entity_repo.upsert.return_value = (1, True)
        processor.article_entity_repo.save_batch.return_value = 0
        processor.natasha.extract.return_value = []

        await processor.process_batch()

        title_arg, text_arg = processor.natasha.extract.call_args[0]
        assert "<b>" not in title_arg
        assert "<p>" not in text_arg
        assert "Заголовок" in title_arg
        assert "Текст" in text_arg
