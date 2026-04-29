# tests/unit/test_models.py
from datetime import datetime

import pytest

from src.core.models import ArticleForDB, ParsedItem, ProcessingStats


class TestModels:
    def test_parsed_item_creation(self):
        item = ParsedItem(
            source_id=1,
            source_name="test",
            original_id="123",
            url="https://example.com",
            title="Test Title",
            content="Test content here",
            published_at=datetime.now(),
            author="Test Author",
        )

        assert item.source_id == 1
        assert item.title == "Test Title"
        assert item.content == "Test content here"

    def test_processing_stats_add(self):
        s1 = ProcessingStats(total_rows=10, saved=5, skipped=3, errors=2)
        s2 = ProcessingStats(total_rows=5, saved=2, skipped=2, errors=1)
        s3 = s1.add(s2)

        assert s3.total_rows == 15
        assert s3.saved == 7
        assert s3.skipped == 5
        assert s3.errors == 3
