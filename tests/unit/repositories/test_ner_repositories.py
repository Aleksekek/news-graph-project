"""
Тесты для NER-репозиториев (моки БД, без реального подключения).
"""

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.core.models import ExtractedEntity


class TestProcessedArticleRepository:
    @pytest.fixture
    def repo(self):
        from src.database.repositories.processed_article_repository import ProcessedArticleRepository
        return ProcessedArticleRepository()

    @pytest.fixture
    def mock_conn(self):
        conn = AsyncMock()
        conn.fetchrow = AsyncMock()
        conn.execute = AsyncMock()
        return conn

    @pytest.mark.asyncio
    async def test_create_returns_id(self, repo, mock_conn):
        mock_conn.fetchrow.return_value = {"id": 42}

        with patch("src.database.repositories.processed_article_repository.DatabasePoolManager") as mock_pool:
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            result = await repo.create(
                raw_article_id=1,
                title="Заголовок",
                text="Текст статьи",
                published_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
            )

        assert result == 42

    @pytest.mark.asyncio
    async def test_create_passes_correct_params(self, repo, mock_conn):
        mock_conn.fetchrow.return_value = {"id": 7}
        pub_at = datetime(2026, 3, 15, 10, 0, tzinfo=timezone.utc)

        with patch("src.database.repositories.processed_article_repository.DatabasePoolManager") as mock_pool:
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            await repo.create(raw_article_id=5, title="Тест", text="Текст", published_at=pub_at)

        args = mock_conn.fetchrow.call_args[0]
        assert args[1] == 5       # raw_article_id
        assert args[2] == "Тест"  # title
        assert args[3] == "Текст" # text
        assert args[4] == pub_at  # published_at

    @pytest.mark.asyncio
    async def test_update_processing_flags(self, repo, mock_conn):
        with patch("src.database.repositories.processed_article_repository.DatabasePoolManager") as mock_pool:
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            await repo.update_processing_flags(processed_id=10, flags={"ner": True})

        mock_conn.execute.assert_called_once()
        args = mock_conn.execute.call_args[0]
        assert args[2] == 10  # processed_id

    @pytest.mark.asyncio
    async def test_get_by_raw_id_returns_none_when_not_found(self, repo, mock_conn):
        mock_conn.fetchrow.return_value = None

        with patch("src.database.repositories.processed_article_repository.DatabasePoolManager") as mock_pool:
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            result = await repo.get_by_raw_id(999)

        assert result is None


class TestEntityRepository:
    @pytest.fixture
    def repo(self):
        from src.database.repositories.entity_repository import EntityRepository
        return EntityRepository()

    @pytest.fixture
    def mock_conn(self):
        conn = AsyncMock()
        conn.fetchrow = AsyncMock()
        return conn

    @pytest.fixture
    def entity(self):
        return ExtractedEntity(
            original_name="Сбербанком",
            normalized_name="Сбербанк",
            entity_type="organization",
            count=2,
            importance_score=1.0,
            context_snippet="Сбербанк снизил ставки.",
        )

    @pytest.mark.asyncio
    async def test_upsert_new_entity_returns_id_and_is_new_true(self, repo, mock_conn, entity):
        # 1-й fetchrow — alias lookup (None = алиас не найден)
        # 2-й fetchrow — INSERT entities
        mock_conn.fetchrow.side_effect = [None, {"id": 1, "is_new": True}]

        with patch("src.database.repositories.entity_repository.DatabasePoolManager") as mock_pool:
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            entity_id, is_new = await repo.upsert(entity)

        assert entity_id == 1
        assert is_new is True

    @pytest.mark.asyncio
    async def test_upsert_existing_entity_returns_id_and_is_new_false(self, repo, mock_conn, entity):
        mock_conn.fetchrow.side_effect = [None, {"id": 99, "is_new": False}]

        with patch("src.database.repositories.entity_repository.DatabasePoolManager") as mock_pool:
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            entity_id, is_new = await repo.upsert(entity)

        assert entity_id == 99
        assert is_new is False

    @pytest.mark.asyncio
    async def test_upsert_passes_normalized_name_and_type(self, repo, mock_conn, entity):
        mock_conn.fetchrow.side_effect = [None, {"id": 5, "is_new": True}]

        with patch("src.database.repositories.entity_repository.DatabasePoolManager") as mock_pool:
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            await repo.upsert(entity)

        # call_args_list[1] — второй вызов fetchrow (INSERT entities)
        args = mock_conn.fetchrow.call_args_list[1][0]
        assert "Сбербанк" in args[1]       # normalized_name
        assert "organization" in args[2]   # type

    @pytest.mark.asyncio
    async def test_upsert_skips_discard_entity(self, repo, mock_conn, entity):
        discard_row = {"canonical_name": "Сбербанк", "canonical_type": "discard"}
        mock_conn.fetchrow.return_value = discard_row

        with patch("src.database.repositories.entity_repository.DatabasePoolManager") as mock_pool:
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            entity_id, is_new = await repo.upsert(entity)

        assert entity_id is None
        assert is_new is False
        assert mock_conn.fetchrow.call_count == 1  # INSERT не вызывался

    @pytest.mark.asyncio
    async def test_upsert_resolves_alias(self, repo, mock_conn, entity):
        # Алиас найден: Сбербанк → Сбер
        alias_row = {"canonical_name": "Сбер", "canonical_type": "organization"}
        mock_conn.fetchrow.side_effect = [alias_row, {"id": 7, "is_new": False}]

        with patch("src.database.repositories.entity_repository.DatabasePoolManager") as mock_pool:
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            entity_id, _ = await repo.upsert(entity)

        assert entity_id == 7
        # Второй fetchrow должен использовать каноническое имя
        args = mock_conn.fetchrow.call_args_list[1][0]
        assert "Сбер" in args[1]           # canonical_name подставлено
        assert "organization" in args[2]


class TestArticleEntityRepository:
    @pytest.fixture
    def repo(self):
        from src.database.repositories.article_entity_repository import ArticleEntityRepository
        return ArticleEntityRepository()

    @pytest.fixture
    def mock_conn(self):
        conn = AsyncMock()
        conn.executemany = AsyncMock()
        return conn

    def _make_entity(self, name: str, count: int = 1, score: float = 0.5) -> ExtractedEntity:
        return ExtractedEntity(
            original_name=name,
            normalized_name=name,
            entity_type="person",
            count=count,
            importance_score=score,
            context_snippet=f"Контекст для {name}.",
        )

    @pytest.mark.asyncio
    async def test_save_batch_empty_returns_zero(self, repo, mock_conn):
        with patch("src.database.repositories.article_entity_repository.DatabasePoolManager") as mock_pool:
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            result = await repo.save_batch(processed_article_id=1, entity_links=[])

        assert result == 0
        mock_conn.executemany.assert_not_called()

    @pytest.mark.asyncio
    async def test_save_batch_returns_count(self, repo, mock_conn):
        links = [
            (1, self._make_entity("Путин")),
            (2, self._make_entity("Москва")),
            (3, self._make_entity("Сбербанк")),
        ]

        with patch("src.database.repositories.article_entity_repository.DatabasePoolManager") as mock_pool:
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            result = await repo.save_batch(processed_article_id=10, entity_links=links)

        assert result == 3

    @pytest.mark.asyncio
    async def test_save_batch_passes_correct_data(self, repo, mock_conn):
        entity = self._make_entity("Греф", count=2, score=0.7)
        links = [(55, entity)]

        with patch("src.database.repositories.article_entity_repository.DatabasePoolManager") as mock_pool:
            mock_pool.connection.return_value.__aenter__.return_value = mock_conn

            await repo.save_batch(processed_article_id=99, entity_links=links)

        batch_data = mock_conn.executemany.call_args[0][1]
        assert len(batch_data) == 1
        row = batch_data[0]
        assert row[0] == 99   # processed_article_id
        assert row[1] == 55   # entity_id
        assert row[2] == 2    # count
        assert row[3] == 0.7  # importance_score
