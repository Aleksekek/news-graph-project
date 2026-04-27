"""
Тест фабрики парсеров.
"""

import pytest

from src.parsers.factory import ParserFactory
from src.parsers.lenta.parser import LentaParser
from src.parsers.tinvest.parser import TInvestParser


class TestParserFactory:
    def test_create_lenta_parser(self):
        parser = ParserFactory.create("lenta")
        assert isinstance(parser, LentaParser)
        assert parser.source_name == "lenta"

    def test_create_tinvest_parser(self):
        parser = ParserFactory.create("tinvest", {"tickers": ["SBER", "VTBR"]})
        assert isinstance(parser, TInvestParser)
        assert parser.source_name == "tinvest"

    def test_create_with_categories(self):
        parser = ParserFactory.create(
            "lenta", {"categories": ["Политика", "Экономика"]}
        )
        assert parser.default_categories == ["Политика", "Экономика"]

    def test_create_unknown_source(self):
        with pytest.raises(Exception):
            ParserFactory.create("unknown_source")

    def test_list_available(self):
        available = ParserFactory.list_available()
        assert "lenta" in available
        assert "tinvest" in available
