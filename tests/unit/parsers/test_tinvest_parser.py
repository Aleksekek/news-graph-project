"""
Unit-тесты для TInvest парсера.
"""

from datetime import datetime, timezone

import pytest

from src.parsers.base import ParserConfig
from src.parsers.tinvest.parser import TInvestParser
from src.utils.datetime_utils import now_msk


class TestTInvestExtractDate:
    """Тесты извлечения даты из постов tpulse."""

    @pytest.fixture
    def parser(self):
        """Фикстура парсера."""
        config = ParserConfig(source_id=1, source_name="tinvest")
        return TInvestParser(config)

    def test_extract_date_utc_with_z(self, parser):
        """API возвращает UTC с Z — должно конвертироваться в MSK."""
        post = {"inserted": "2026-04-28T21:28:25.102Z"}
        result = parser._extract_date(post)

        assert result is not None
        assert result.tzinfo is None  # naive
        assert result.hour == 0  # 21:28 UTC -> 00:28 MSK
        assert result.minute == 28
        assert result.day == 29  # день перешёл

    def test_extract_date_utc_with_z_early_time(self, parser):
        """Раннее UTC время, которое не переходит на следующий день."""
        post = {"inserted": "2026-04-28T10:00:00.000Z"}
        result = parser._extract_date(post)

        assert result is not None
        assert result.hour == 13  # 10:00 UTC -> 13:00 MSK
        assert result.day == 28  # тот же день

    def test_extract_date_utc_midnight(self, parser):
        """Полночь UTC -> 03:00 MSK."""
        post = {"inserted": "2026-04-28T00:00:00.000Z"}
        result = parser._extract_date(post)

        assert result is not None
        assert result.hour == 3
        assert result.day == 28

    def test_extract_date_empty(self, parser):
        """Пустая дата."""
        assert parser._extract_date({"inserted": ""}) is None
        assert parser._extract_date({}) is None

    def test_extract_date_invalid(self, parser):
        """Невалидная дата."""
        assert parser._extract_date({"inserted": "not-a-date"}) is None

    def test_extract_date_fresh_posts(self, parser):
        """Свежие посты должны быть близки к now_msk."""
        # Симулируем свежий пост (5 минут назад в UTC)
        now_utc = datetime.now(timezone.utc)
        post_time = now_utc.replace(second=0, microsecond=0)
        post = {"inserted": post_time.strftime("%Y-%m-%dT%H:%M:%S.000Z")}

        result = parser._extract_date(post)
        now = now_msk()

        # Разница должна быть не более 1 минуты
        diff = abs((now - result).total_seconds())
        assert diff < 60, (
            f"Разница между now_msk и parsed date слишком большая: {diff} сек. "
            f"now_msk={now}, parsed={result}"
        )


class TestTInvestGenerateId:
    """Тесты генерации ID."""

    @pytest.fixture
    def parser(self):
        return TInvestParser.__new__(TInvestParser)

    def test_generate_id_deterministic(self, parser):
        """ID должен быть детерминированным."""
        post = {
            "id": "abc123",
            "inserted": "2026-01-01T00:00:00Z",
            "content": {"text": "Hello world"},
        }
        id1 = parser._generate_id(post)
        id2 = parser._generate_id(post)
        assert id1 == id2
        assert id1.startswith("tinvest_")

    def test_generate_id_unique(self, parser):
        """Разные посты — разные ID."""
        post1 = {
            "id": "abc123",
            "inserted": "2026-01-01T00:00:00Z",
            "content": {"text": "Hello"},
        }
        post2 = {
            "id": "abc123",
            "inserted": "2026-01-01T00:00:00Z",
            "content": {"text": "World"},
        }
        assert parser._generate_id(post1) != parser._generate_id(post2)
