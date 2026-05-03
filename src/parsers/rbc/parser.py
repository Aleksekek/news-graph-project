"""
Парсер РБК (rbc.ru).
parse()        — RSS rbc_news_full-text (полный текст в самой ленте); fallback на HTML.
parse_period() — архивный парсер через AJAX API поиска + HTML страниц.
"""

import asyncio
import hashlib
import html
import re
from datetime import datetime, timedelta
from typing import Any

import feedparser
from bs4 import BeautifulSoup

from src.core.exceptions import ParserError
from src.core.models import ParsedItem
from src.parsers.base import BaseParser, ParserConfig, ParseResult
from src.utils.datetime_utils import parse_rfc2822_date
from src.utils.logging import log_async_execution_time

RSS_URL = "https://rssexport.rbc.ru/rbcnews/news/30/full.rss"
# AJAX API поиска (не требует JS, возвращает JSON)
# Пагинация через page=N (1-indexed); moreExists=false — конец.
# offset-параметр API игнорирует.
ARCHIVE_AJAX_URL = (
    "https://www.rbc.ru/search/ajax/"
    "?query=&dateFrom={date}&dateTo={date}&limit=20&page={page}"
)
_MAX_ARCHIVE_PAGES = 50
# Типы материалов, которые пропускаем при архивном парсинге
_SKIP_TYPES = {"uploaded_video", "special_project", "photogallery", "photoreport"}

# Строки, которые убираем из любого извлечённого текста
_NOISE_LINES = (
    "оставайтесь на связи",
    "читайте рбк",
    "рбк в «максе»",
    "рбк в max",
    "материал дополняется",
    "[ рбк ]",           # инлайн-ссылки на другие материалы
)

# Селекторы для текста статьи (HTML-страница, fallback и архив)
_ARTICLE_SELECTORS = [
    "div.article__text",
    "div.article__content",
    "div[class*='article__text']",
    "div.l-col-center article",
    "div.article-body",
]


class RbcParser(BaseParser):
    """
    Парсер РБК.
    Filters для parse():
        - min_length: int — минимальная длина текста (по умолчанию 100)
    Filters для parse_period():
        - max_per_day: int — максимум статей за день (по умолчанию 300; AJAX API
          отдаёт по 20/стр, мы пагинируемся до moreExists=false или _MAX_ARCHIVE_PAGES)
        - min_length: int — минимальная длина текста (по умолчанию 100)
    """

    def __init__(self, config: ParserConfig):
        super().__init__(config)
        self.max_concurrent = 5

    @log_async_execution_time()
    async def parse(self, limit: int = 100, **filters) -> ParseResult:
        """Парсинг свежих статей: RSS для метаданных, страницы для полного текста."""
        min_length = filters.get("min_length", 100)

        self.logger.info(f"Парсинг РБК RSS: лимит={limit}")
        rss_items = await self._fetch_rss_items(limit)
        if not rss_items:
            return ParseResult([])

        articles = await self._fetch_articles_from_rss(rss_items, min_length)
        self.logger.info(f"Распарсено {len(articles)} статей из RSS")
        return ParseResult(articles)

    @log_async_execution_time()
    async def parse_period(
        self, start_date: datetime, end_date: datetime, limit: int = 100, **filters
    ) -> ParseResult:
        """Архивный парсинг через AJAX API с итерацией по дням (с пагинацией по 20/стр)."""
        max_per_day = filters.get("max_per_day", 300)
        min_length = filters.get("min_length", 100)

        self.logger.info(f"Архивный парсинг РБК: {start_date.date()} — {end_date.date()}")

        all_articles: list[ParsedItem] = []
        current = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_day = end_date.replace(hour=23, minute=59, second=59)

        while current <= end_day and len(all_articles) < limit:
            remaining = limit - len(all_articles)
            day_articles = await self._parse_archive_day(
                current,
                max_articles=min(max_per_day, remaining),
                min_length=min_length,
            )
            all_articles.extend(day_articles)
            self.logger.info(
                f"День {current.date()}: +{len(day_articles)} статей "
                f"(всего {len(all_articles)}/{limit})"
            )
            current += timedelta(days=1)
            if current <= end_day:
                await self._delay()

        self.logger.info(f"Архивный парсинг завершён: {len(all_articles)} статей")
        return ParseResult(all_articles)

    def to_parsed_item(self, raw_data: dict[str, Any]) -> ParsedItem:
        return ParsedItem(
            source_id=self.source_id,
            source_name=self.source_name,
            original_id=raw_data["original_id"],
            url=raw_data["url"],
            title=raw_data["title"],
            content=raw_data["content"],
            published_at=raw_data.get("published_at"),
            metadata={"text_length": len(raw_data["content"])},
        )

    # ── RSS (parse) ────────────────────────────────────────────────────────────

    async def _fetch_rss_items(self, limit: int) -> list[dict]:
        try:
            xml_text = await self._fetch_url(RSS_URL)
            loop = asyncio.get_event_loop()
            feed = await loop.run_in_executor(None, feedparser.parse, xml_text)

            if feed.bozo:
                self.logger.warning(f"RSS warning: {feed.bozo_exception}")

            items = []
            seen_urls: set[str] = set()
            for entry in feed.entries:
                if len(items) >= limit:
                    break
                # RBC RSS даёт несколько <link> на запись: первая — основная статья,
                # остальные — related/recommended (могут быть многомесячной давности).
                # feedparser для entry.link возвращает ПОСЛЕДНЮЮ ссылку, не первую.
                link = ""
                if entry.get("links"):
                    link = entry.links[0].get("href", "")
                if not link:
                    link = entry.get("link", "")
                if not link or link in seen_urls:
                    continue
                # Пропускаем онлайн-трансляции (живые тикеры без статичного текста)
                if "/textonlines/" in link:
                    continue
                seen_urls.add(link)

                published_at = None
                if entry.get("published"):
                    published_at = parse_rfc2822_date(entry.published)

                items.append({
                    "link": link,
                    "title": entry.get("title", ""),
                    "published_at": published_at,
                    # RBC RSS отдаёт полный текст в rbc_news_full-text — используем его
                    # вместо хождения за HTML страницы. Содержит HTML-сущности и теги.
                    "full_text_raw": entry.get("rbc_news_full-text", ""),
                })

            return items

        except Exception as e:
            self.logger.error(f"Ошибка RSS: {e}")
            return []

    async def _fetch_articles_from_rss(
        self, rss_items: list[dict], min_length: int
    ) -> list[ParsedItem]:
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def fetch_one(item: dict) -> ParsedItem | None:
            try:
                # 1. Пробуем взять текст прямо из RSS (rbc_news_full-text)
                content = self._clean_rss_full_text(item.get("full_text_raw", ""))
                title = item.get("title", "") or ""

                # 2. Fallback: если в RSS пусто/коротко — идём за HTML страницей
                if not content or len(content) < min_length:
                    async with semaphore:
                        await self._delay()
                        page_title, page_content = await self._fetch_article_full(item["link"])
                    if page_content and len(page_content) >= min_length:
                        content = page_content
                    if not title:
                        title = page_title

                if not content or len(content) < min_length:
                    return None
                if not title:
                    title = "Без заголовка"

                url_hash = hashlib.md5(item["link"].encode()).hexdigest()[:12]
                raw = {
                    "original_id": f"rbc_{url_hash}",
                    "url": item["link"],
                    "title": title,
                    "content": content,
                    "published_at": item["published_at"],
                }
                parsed = self.to_parsed_item(raw)
                return parsed if self._validate_item(parsed) else None
            except Exception as e:
                self.logger.error(f"Ошибка {item['link']}: {e}")
                return None

        results = await asyncio.gather(*[fetch_one(item) for item in rss_items])
        return [r for r in results if r is not None]

    def _clean_rss_full_text(self, raw: str) -> str:
        """Очищает rbc_news_full-text: HTML-сущности, теги, шумовые строки."""
        if not raw:
            return ""
        text = html.unescape(raw)
        # <br>, <br/>, <br /> → перенос строки
        text = re.sub(r"<\s*br\s*/?\s*>", "\n", text, flags=re.I)
        # <p>, </p> → двойной перенос (граница абзаца)
        text = re.sub(r"</?p[^>]*>", "\n\n", text, flags=re.I)
        # Прочие теги — выкидываем
        text = re.sub(r"<[^>]+>", "", text)
        # Нормализуем пробелы и переносы
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n[ \t]+", "\n", text)
        text = re.sub(r"\n\s*\n+", "\n\n", text)
        return self._clean_rbc_text(text.strip())

    # ── Архив (parse_period) ───────────────────────────────────────────────────

    async def _parse_archive_day(
        self,
        date: datetime,
        max_articles: int,
        min_length: int,
    ) -> list[ParsedItem]:
        api_items = await self._get_archive_items_api(date, max_articles)
        if not api_items:
            return []

        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def fetch_full(api_item: dict) -> ParsedItem | None:
            url = api_item["url"]
            async with semaphore:
                try:
                    await self._delay()
                    title, content = await self._fetch_article_full(url)
                    if not content or len(content) < min_length:
                        return None
                    if not title:
                        title = api_item.get("title", "Без заголовка")

                    url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
                    raw = {
                        "original_id": f"rbc_{url_hash}",
                        "url": url,
                        "title": title,
                        "content": content,
                        "published_at": api_item.get("published_at", date),
                    }
                    parsed = self.to_parsed_item(raw)
                    return parsed if self._validate_item(parsed) else None
                except Exception as e:
                    self.logger.error(f"Ошибка {url}: {e}")
                    return None

        results = await asyncio.gather(*[fetch_full(it) for it in api_items])
        return [r for r in results if r is not None]

    async def _get_archive_items_api(
        self, date: datetime, max_results: int
    ) -> list[dict]:
        """Получает список статей за день через AJAX API РБК (JSON).

        Пагинация через page=N (1-indexed).
        Останавливаемся когда moreExists=false или страниц > _MAX_ARCHIVE_PAGES.
        """
        date_str = date.strftime("%d.%m.%Y")
        items: list[dict] = []
        seen_urls: set[str] = set()

        for page in range(1, _MAX_ARCHIVE_PAGES + 1):
            api_url = ARCHIVE_AJAX_URL.format(date=date_str, page=page)
            try:
                data = await self._fetch_json(
                    api_url, headers={"Referer": "https://www.rbc.ru/"}
                )
            except Exception as e:
                self.logger.error(f"Ошибка AJAX API РБК за {date_str} стр.{page}: {e}")
                break

            page_items = data.get("items", [])
            if not page_items:
                break

            for it in page_items:
                if it.get("type") in _SKIP_TYPES:
                    continue
                url = it.get("fronturl", "")
                # Оставляем только www.rbc.ru (не региональные поддомены)
                if not url.startswith("https://www.rbc.ru"):
                    continue
                if url in seen_urls:
                    continue
                seen_urls.add(url)

                published_at = None
                ts = it.get("publish_date_t")
                if ts:
                    try:
                        published_at = datetime.fromtimestamp(int(ts))
                    except (ValueError, OSError):
                        pass
                items.append({
                    "url": url,
                    "title": it.get("title", ""),
                    "published_at": published_at or date,
                })
                if len(items) >= max_results:
                    return items

            if not data.get("moreExists", False):
                break

            await self._delay()

        return items

    async def _fetch_article_full(self, url: str) -> tuple[str, str]:
        """Возвращает (title, content) для одной статьи."""
        try:
            page_html = await self._fetch_url(url)
        except ParserError:
            return "", ""

        soup = BeautifulSoup(page_html, "html.parser")

        # Заголовок
        title = ""
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(strip=True)
        if not title:
            og = soup.find("meta", property="og:title")
            if og:
                title = og.get("content", "").strip()

        content = self._extract_article_content(soup)
        return title, content

    @staticmethod
    def _clean_rbc_text(text: str) -> str:
        lines = []
        for line in text.splitlines():
            if any(n in line.lower() for n in _NOISE_LINES):
                continue
            lines.append(line)
        return "\n".join(lines).strip()

    def _extract_article_content(self, soup: "BeautifulSoup") -> str:
        # 1) meta[itemprop="articleBody"] — предзаготовленный чистый текст
        meta_body = soup.find("meta", itemprop="articleBody")
        if meta_body:
            text = self._clean_rbc_text(meta_body.get("content", "").strip())
            if len(text) > 100:
                return text

        # 2) article.article-feature-item p.paragraph (rbcfreenews / новости-однострочники)
        article_el = soup.select_one("article.article-feature-item")
        if article_el:
            ps = [
                p.get_text(strip=True)
                for p in article_el.select("p.paragraph")
                if p.get_text(strip=True)
            ]
            text = self._clean_rbc_text("\n\n".join(ps))
            if len(text) > 100:
                return text

        # 3) Стандартные CSS-селекторы
        for selector in _ARTICLE_SELECTORS:
            container = soup.select_one(selector)
            if not container:
                continue
            for tag in container.find_all(["script", "style", "figure", "aside", "nav"]):
                tag.decompose()
            text = self._clean_rbc_text(container.get_text(separator="\n", strip=True))
            if len(text) > 100:
                return text

        return ""
