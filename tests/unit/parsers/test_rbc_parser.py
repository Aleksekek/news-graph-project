"""
Юнит-тесты для парсера РБК.
Покрывают:
  - _clean_rss_full_text: декодирование HTML-сущностей, теги, шумовые строки
  - _fetch_rss_items: выбор основной ссылки из entry.links[0] (а не последней через entry.link)
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.parsers.factory import ParserFactory
from src.parsers.rbc.parser import RbcParser


@pytest.fixture
def rbc_parser() -> RbcParser:
    return ParserFactory.create("rbc")


class TestRbcCleanRssFullText:
    """_clean_rss_full_text — извлечение полного текста из rbc_news_full-text RSS-поля."""

    def test_empty_input_returns_empty(self, rbc_parser):
        assert rbc_parser._clean_rss_full_text("") == ""
        assert rbc_parser._clean_rss_full_text(None) == ""  # type: ignore[arg-type]

    def test_decodes_html_entities(self, rbc_parser):
        raw = "Слова с &laquo;кавычками&raquo; и тире&nbsp;&mdash;&nbsp;вот так."
        out = rbc_parser._clean_rss_full_text(raw)
        assert "«кавычками»" in out
        assert "—" in out
        assert "&laquo;" not in out
        assert "&nbsp;" not in out
        assert "&mdash;" not in out

    def test_strips_html_tags(self, rbc_parser):
        raw = "<p>Первый абзац</p><p>Второй <a href='x'>с ссылкой</a>.</p>"
        out = rbc_parser._clean_rss_full_text(raw)
        assert "Первый абзац" in out
        assert "Второй" in out
        assert "с ссылкой" in out
        assert "<p>" not in out
        assert "<a" not in out

    def test_br_becomes_newline(self, rbc_parser):
        raw = "Строка 1<br>Строка 2<br/>Строка 3<br />Строка 4"
        out = rbc_parser._clean_rss_full_text(raw)
        lines = [line for line in out.split("\n") if line.strip()]
        assert len(lines) == 4

    def test_p_tags_become_paragraph_breaks(self, rbc_parser):
        raw = "<p>Абзац А</p><p>Абзац Б</p>"
        out = rbc_parser._clean_rss_full_text(raw)
        assert "Абзац А" in out and "Абзац Б" in out
        # Двойной перенос между абзацами
        assert "\n\n" in out

    def test_collapses_whitespace(self, rbc_parser):
        raw = "Слово   с    лишними   пробелами"
        out = rbc_parser._clean_rss_full_text(raw)
        assert "  " not in out  # никаких двойных пробелов

    def test_filters_noise_lines(self, rbc_parser):
        raw = (
            "<p>Полезный текст статьи.</p>"
            "<p>Читайте РБК в социальных сетях.</p>"
            "<p>Оставайтесь на связи с РБК.</p>"
            "<p>Ещё один полезный абзац.</p>"
        )
        out = rbc_parser._clean_rss_full_text(raw)
        assert "Полезный текст" in out
        assert "Ещё один полезный" in out
        assert "Читайте РБК" not in out
        assert "Оставайтесь на связи" not in out

    def test_preserves_normal_quotes_and_dashes(self, rbc_parser):
        raw = "Текст без специальных сущностей с обычными «кавычками» и тире — вот."
        out = rbc_parser._clean_rss_full_text(raw)
        assert "«кавычками»" in out
        assert "тире — вот" in out


class TestRbcFetchRssItemsLinkSelection:
    """_fetch_rss_items: проверка, что URL берётся из entry.links[0], а не entry.link."""

    @pytest.mark.asyncio
    async def test_picks_links_zero_when_multiple_present(self, rbc_parser):
        """RBC RSS даёт несколько <link> на запись; entry.link feedparser берёт ПОСЛЕДНИЙ.
        Парсер должен брать первый (canonical, основная статья)."""
        # Мокируем feedparser response: одна запись с 4 links
        canonical_url = "https://www.rbc.ru/politics/03/05/2026/CANONICAL"
        related_url = "https://www.rbc.ru/rbcfreenews/RELATED-FROM-PAST"

        fake_entry = {
            "link": related_url,  # entry.link = последняя ссылка (related)
            "links": [
                {"rel": "alternate", "type": "text/html", "href": canonical_url},
                {"rel": "alternate", "type": "text/html", "href": "https://www.rbc.ru/x/1"},
                {"rel": "alternate", "type": "text/html", "href": "https://www.rbc.ru/x/2"},
                {"rel": "alternate", "type": "text/html", "href": related_url},
            ],
            "title": "Заголовок основной статьи",
            "published": "Sun, 03 May 2026 03:38:40 +0300",
            "rbc_news_full-text": "<p>Полный текст статьи длиной более ста символов.</p>" * 5,
        }
        fake_feed = MagicMock()
        fake_feed.bozo = False
        fake_feed.entries = [_dict_to_attr_obj(fake_entry)]

        with patch.object(rbc_parser, "_fetch_url", new=AsyncMock(return_value="<rss/>")):
            with patch("src.parsers.rbc.parser.feedparser.parse", return_value=fake_feed):
                items = await rbc_parser._fetch_rss_items(limit=5)

        assert len(items) == 1
        assert items[0]["link"] == canonical_url
        assert items[0]["link"] != related_url

    @pytest.mark.asyncio
    async def test_falls_back_to_link_when_no_links_array(self, rbc_parser):
        """Старый формат RSS без links[]: используем entry.link."""
        fake_entry = {
            "link": "https://www.rbc.ru/x/1",
            "links": [],
            "title": "Title",
            "published": "Sun, 03 May 2026 03:38:40 +0300",
            "rbc_news_full-text": "<p>Текст.</p>",
        }
        fake_feed = MagicMock()
        fake_feed.bozo = False
        fake_feed.entries = [_dict_to_attr_obj(fake_entry)]

        with patch.object(rbc_parser, "_fetch_url", new=AsyncMock(return_value="<rss/>")):
            with patch("src.parsers.rbc.parser.feedparser.parse", return_value=fake_feed):
                items = await rbc_parser._fetch_rss_items(limit=5)

        assert len(items) == 1
        assert items[0]["link"] == "https://www.rbc.ru/x/1"

    @pytest.mark.asyncio
    async def test_dedups_duplicate_urls(self, rbc_parser):
        """Если RSS содержит два item'а с одной и той же canonical ссылкой — один пропускается."""
        canonical_url = "https://www.rbc.ru/politics/03/05/2026/SAME"
        e1 = _dict_to_attr_obj({
            "link": "x", "links": [{"rel": "alternate", "type": "text/html", "href": canonical_url}],
            "title": "A", "published": "Sun, 03 May 2026 03:38:40 +0300",
            "rbc_news_full-text": "<p>Text.</p>",
        })
        e2 = _dict_to_attr_obj({
            "link": "x", "links": [{"rel": "alternate", "type": "text/html", "href": canonical_url}],
            "title": "B", "published": "Sun, 03 May 2026 03:38:40 +0300",
            "rbc_news_full-text": "<p>Text.</p>",
        })
        fake_feed = MagicMock()
        fake_feed.bozo = False
        fake_feed.entries = [e1, e2]

        with patch.object(rbc_parser, "_fetch_url", new=AsyncMock(return_value="<rss/>")):
            with patch("src.parsers.rbc.parser.feedparser.parse", return_value=fake_feed):
                items = await rbc_parser._fetch_rss_items(limit=5)

        assert len(items) == 1
        assert items[0]["link"] == canonical_url

    @pytest.mark.asyncio
    async def test_skips_textonlines(self, rbc_parser):
        """Онлайн-трансляции (без статичного текста) пропускаются."""
        e = _dict_to_attr_obj({
            "link": "x",
            "links": [{"rel": "alternate", "type": "text/html",
                       "href": "https://www.rbc.ru/textonlines/12345"}],
            "title": "Live", "published": "Sun, 03 May 2026 03:38:40 +0300",
            "rbc_news_full-text": "<p>Не должна попасть.</p>",
        })
        fake_feed = MagicMock()
        fake_feed.bozo = False
        fake_feed.entries = [e]

        with patch.object(rbc_parser, "_fetch_url", new=AsyncMock(return_value="<rss/>")):
            with patch("src.parsers.rbc.parser.feedparser.parse", return_value=fake_feed):
                items = await rbc_parser._fetch_rss_items(limit=5)

        assert items == []


def _dict_to_attr_obj(d: dict):
    """Превращает dict в объект, поддерживающий и d['key'], и d.get('key')."""
    obj = MagicMock()
    obj.__getitem__ = lambda self, k: d[k]
    obj.get = lambda k, default=None: d.get(k, default)
    obj.keys = lambda: d.keys()
    # feedparser entries поддерживают атрибутный доступ — links нужен как реальный список
    for k, v in d.items():
        setattr(obj, k.replace("-", "_"), v)
    return obj
