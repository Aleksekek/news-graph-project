"""
Юнит-тесты для парсера ТАСС.
Покрывают:
  - _clean_rss_full_text: декодирование HTML-сущностей и тегов из yandex_full-text
  - _get_sitemap_news: парсинг sitemap_news{N}.xml, извлечение (url, lastmod)
  - _get_sitemap_urls_for_date: логика выбора подходящего sitemap по диапазону дат
"""

from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest

from src.core.exceptions import ParserError
from src.parsers.factory import ParserFactory
from src.parsers.tass.parser import TassParser


@pytest.fixture
def tass_parser() -> TassParser:
    return ParserFactory.create("tass")


class TestTassCleanRssFullText:
    """_clean_rss_full_text — извлечение полного текста из yandex_full-text."""

    def test_empty_input_returns_empty(self):
        assert TassParser._clean_rss_full_text("") == ""

    def test_decodes_html_entities(self):
        raw = "Текст с &laquo;кавычками&raquo; и тире&nbsp;&mdash;&nbsp;вот."
        out = TassParser._clean_rss_full_text(raw)
        assert "«кавычками»" in out
        assert "—" in out
        assert "&laquo;" not in out
        assert "&nbsp;" not in out

    def test_strips_html_tags(self):
        raw = "<p>Первый.</p><p>Второй <b>жирный</b> текст.</p>"
        out = TassParser._clean_rss_full_text(raw)
        assert "Первый" in out
        assert "Второй" in out
        assert "жирный" in out
        assert "<p>" not in out
        assert "<b>" not in out

    def test_br_becomes_newline(self):
        raw = "Строка 1<br>Строка 2<br/>Строка 3"
        out = TassParser._clean_rss_full_text(raw)
        lines = [line for line in out.split("\n") if line.strip()]
        assert len(lines) == 3

    def test_p_tags_become_paragraph_breaks(self):
        raw = "<p>Абзац А</p><p>Абзац Б</p>"
        out = TassParser._clean_rss_full_text(raw)
        assert "Абзац А" in out and "Абзац Б" in out
        assert "\n\n" in out

    def test_collapses_whitespace(self):
        raw = "Слово   с    лишними   пробелами"
        out = TassParser._clean_rss_full_text(raw)
        assert "  " not in out


class TestTassSitemapParsing:
    """_get_sitemap_news — парсинг sitemap_news{N}.xml."""

    @pytest.mark.asyncio
    async def test_parses_url_lastmod_pairs(self, tass_parser):
        xml = """<?xml version="1.0" encoding="UTF-8"?>
        <urlset xmlns="http://www.sitemaps.org/schemas/sitemap/0.9">
        <url><loc>https://tass.ru/sport/27282547</loc><lastmod>2026-05-01T09:54:38+03:00</lastmod></url>
        <url><loc>https://tass.ru/ekonomika/27282535</loc><lastmod>2026-04-30T18:30:00+03:00</lastmod></url>
        </urlset>"""

        with patch.object(tass_parser, "_fetch_url", new=AsyncMock(return_value=xml)):
            entries = await tass_parser._get_sitemap_news(0)

        assert entries is not None
        assert len(entries) == 2
        assert entries[0][0] == "https://tass.ru/sport/27282547"
        assert entries[0][1] == datetime(2026, 5, 1, 9, 54, 38)  # MSK naive
        assert entries[1][1] == datetime(2026, 4, 30, 18, 30, 0)

    @pytest.mark.asyncio
    async def test_filters_non_article_urls(self, tass_parser):
        """Из sitemap'а отбрасываются URL, не похожие на статьи."""
        xml = """<urlset>
        <url><loc>https://tass.ru/sport/27282547</loc><lastmod>2026-05-01T09:54:38+03:00</lastmod></url>
        <url><loc>https://tass.ru/about</loc><lastmod>2026-05-01T09:54:38+03:00</lastmod></url>
        <url><loc>https://tass.ru/static/index.html</loc><lastmod>2026-05-01T09:54:38+03:00</lastmod></url>
        </urlset>"""

        with patch.object(tass_parser, "_fetch_url", new=AsyncMock(return_value=xml)):
            entries = await tass_parser._get_sitemap_news(0)

        assert entries is not None
        urls = [u for u, _ in entries]
        assert "https://tass.ru/sport/27282547" in urls
        assert "https://tass.ru/about" not in urls
        assert "https://tass.ru/static/index.html" not in urls

    @pytest.mark.asyncio
    async def test_returns_none_on_404(self, tass_parser):
        async def raise_404(url, **kwargs):
            raise ParserError("HTTP 404 для " + url)

        with patch.object(tass_parser, "_fetch_url", side_effect=raise_404):
            entries = await tass_parser._get_sitemap_news(99)

        assert entries is None

    @pytest.mark.asyncio
    async def test_caches_result(self, tass_parser):
        xml = """<urlset>
        <url><loc>https://tass.ru/sport/123456</loc><lastmod>2026-05-01T09:54:38+03:00</lastmod></url>
        </urlset>"""

        fetch_mock = AsyncMock(return_value=xml)
        with patch.object(tass_parser, "_fetch_url", fetch_mock):
            await tass_parser._get_sitemap_news(0)
            await tass_parser._get_sitemap_news(0)
            await tass_parser._get_sitemap_news(0)

        # _fetch_url должен быть вызван только один раз — остальные взяты из кеша
        assert fetch_mock.call_count == 1

    @pytest.mark.asyncio
    async def test_skips_malformed_lastmod(self, tass_parser):
        """Записи с битым lastmod пропускаются, остальные сохраняются."""
        xml = """<urlset>
        <url><loc>https://tass.ru/sport/123456</loc><lastmod>not-a-date</lastmod></url>
        <url><loc>https://tass.ru/sport/234567</loc><lastmod>2026-05-01T09:54:38+03:00</lastmod></url>
        </urlset>"""

        with patch.object(tass_parser, "_fetch_url", new=AsyncMock(return_value=xml)):
            entries = await tass_parser._get_sitemap_news(0)

        assert entries is not None
        assert len(entries) == 1
        assert entries[0][0] == "https://tass.ru/sport/234567"


class TestTassSitemapDateSelection:
    """_get_sitemap_urls_for_date — выбор подходящего sitemap по диапазону дат."""

    @pytest.mark.asyncio
    async def test_returns_urls_for_date_in_first_sitemap(self, tass_parser):
        """Целевая дата внутри диапазона sitemap_news0 → возвращаем URL'ы из него."""
        sitemap_data = {
            0: [
                ("https://tass.ru/x/1", datetime(2026, 5, 1, 10, 0)),
                ("https://tass.ru/x/2", datetime(2026, 4, 15, 12, 0)),
                ("https://tass.ru/x/3", datetime(2026, 4, 15, 14, 0)),
                ("https://tass.ru/x/4", datetime(2026, 3, 1, 8, 0)),
            ],
        }

        async def fake_get_sitemap(n):
            return sitemap_data.get(n)

        with patch.object(tass_parser, "_get_sitemap_news", side_effect=fake_get_sitemap):
            urls = await tass_parser._get_sitemap_urls_for_date(datetime(2026, 4, 15))

        # Должны вернуться обе статьи 15 апреля + их lastmod
        assert len(urls) == 2
        urls_only = [u for u, _ in urls]
        assert "https://tass.ru/x/2" in urls_only
        assert "https://tass.ru/x/3" in urls_only

    @pytest.mark.asyncio
    async def test_advances_to_next_sitemap_when_target_older(self, tass_parser):
        """Если в sitemap_news0 цель старее всех записей — переходим к sitemap_news1."""
        sitemap_data = {
            0: [
                ("https://tass.ru/x/1", datetime(2026, 5, 1, 10, 0)),
                ("https://tass.ru/x/2", datetime(2026, 3, 11, 12, 0)),
            ],
            1: [
                ("https://tass.ru/x/10", datetime(2026, 3, 10, 10, 0)),
                ("https://tass.ru/x/11", datetime(2026, 1, 1, 12, 0)),
                ("https://tass.ru/x/12", datetime(2025, 12, 1, 8, 0)),
            ],
        }

        async def fake_get_sitemap(n):
            return sitemap_data.get(n)

        with patch.object(tass_parser, "_get_sitemap_news", side_effect=fake_get_sitemap):
            urls = await tass_parser._get_sitemap_urls_for_date(datetime(2026, 1, 1))

        assert len(urls) == 1
        assert urls[0][0] == "https://tass.ru/x/11"

    @pytest.mark.asyncio
    async def test_returns_empty_when_target_newer_than_all(self, tass_parser):
        """Цель новее, чем максимум sitemap_news0 — возвращаем пусто (это работа parse(), не parse_period)."""
        sitemap_data = {
            0: [("https://tass.ru/x/1", datetime(2026, 5, 1, 10, 0))],
        }

        async def fake_get_sitemap(n):
            return sitemap_data.get(n)

        with patch.object(tass_parser, "_get_sitemap_news", side_effect=fake_get_sitemap):
            urls = await tass_parser._get_sitemap_urls_for_date(datetime(2026, 12, 31))

        assert urls == []

    @pytest.mark.asyncio
    async def test_returns_empty_when_archive_exhausted(self, tass_parser):
        """Если все sitemap'ы кончились (404) и нужная дата не найдена — пусто."""
        async def fake_get_sitemap(n):
            if n == 0:
                return [("https://tass.ru/x/1", datetime(2026, 5, 1, 10, 0))]
            return None  # 404 для всех остальных

        with patch.object(tass_parser, "_get_sitemap_news", side_effect=fake_get_sitemap):
            urls = await tass_parser._get_sitemap_urls_for_date(datetime(2020, 1, 1))

        assert urls == []

    @pytest.mark.asyncio
    async def test_returns_url_lastmod_pairs(self, tass_parser):
        """Возвращаемые элементы — (url, lastmod), а не только URL."""
        sitemap_data = {
            0: [("https://tass.ru/x/1", datetime(2026, 5, 1, 14, 30, 45))],
        }

        async def fake_get_sitemap(n):
            return sitemap_data.get(n)

        with patch.object(tass_parser, "_get_sitemap_news", side_effect=fake_get_sitemap):
            urls = await tass_parser._get_sitemap_urls_for_date(datetime(2026, 5, 1))

        assert len(urls) == 1
        url, lastmod = urls[0]
        assert url == "https://tass.ru/x/1"
        assert lastmod == datetime(2026, 5, 1, 14, 30, 45)
