"""
Парсер РБК (rbc.ru).
parse()        — RSS для метаданных, полный текст загружается со страницы статьи.
parse_period() — архивный парсер через /search/ (по образцу Lenta).
"""

import asyncio
import hashlib
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
ARCHIVE_URL = "https://www.rbc.ru/search/?query=&dateFrom={date}&dateTo={date}"
ARCHIVE_PAGE_SIZE = 15  # результатов на страницу (RBC по умолчанию)

# Селекторы для ссылок на статьи в архивной выдаче
_ARCHIVE_LINK_SELECTORS = [
    "a.search-item__title",
    "a.item__link",
    "div.search-item a[href]",
    "li.search-item a[href]",
    "div.item a.item__link",
]

# Строки, которые убираем из любого извлечённого текста
_NOISE_LINES = (
    "оставайтесь на связи",
    "читайте рбк",
    "рбк в «максе»",
    "рбк в max",
    "материал дополняется",
    "[ рбк ]",           # инлайн-ссылки на другие материалы
)

# Селекторы для текста статьи
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
        - max_per_day: int — максимум статей за день (по умолчанию 30)
        - max_pages_per_day: int — страниц архива за день (по умолчанию 3)
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
        """Архивный парсинг через /search/ с итерацией по дням."""
        max_per_day = filters.get("max_per_day", 30)
        max_pages = filters.get("max_pages_per_day", 3)
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
                max_pages=max_pages,
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
            for entry in feed.entries[:limit]:
                link = entry.get("link", "")
                if not link:
                    continue

                published_at = None
                if entry.get("published"):
                    published_at = parse_rfc2822_date(entry.published)

                items.append({
                    "link": link,
                    "title": entry.get("title", ""),
                    "published_at": published_at,
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
            async with semaphore:
                try:
                    await self._delay()
                    title, content = await self._fetch_article_full(item["link"])
                    if not content or len(content) < min_length:
                        return None
                    if not title:
                        title = item.get("title", "Без заголовка")

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

    # ── Архив (parse_period) ───────────────────────────────────────────────────

    async def _parse_archive_day(
        self,
        date: datetime,
        max_articles: int,
        max_pages: int,
        min_length: int,
    ) -> list[ParsedItem]:
        article_urls = await self._get_archive_links(date, max_articles, max_pages)
        if not article_urls:
            return []

        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def fetch_full(url: str) -> ParsedItem | None:
            async with semaphore:
                try:
                    await self._delay()
                    title, content = await self._fetch_article_full(url)
                    if not content or len(content) < min_length:
                        return None
                    if not title:
                        title = "Без заголовка"

                    url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
                    raw = {
                        "original_id": f"rbc_{url_hash}",
                        "url": url,
                        "title": title,
                        "content": content,
                        "published_at": date,
                    }
                    parsed = self.to_parsed_item(raw)
                    return parsed if self._validate_item(parsed) else None
                except Exception as e:
                    self.logger.error(f"Ошибка {url}: {e}")
                    return None

        results = await asyncio.gather(*[fetch_full(url) for url in article_urls])
        return [r for r in results if r is not None]

    async def _get_archive_links(
        self, date: datetime, max_results: int, max_pages: int
    ) -> list[str]:
        date_str = date.strftime("%d.%m.%Y")
        urls: list[str] = []

        for page in range(max_pages):
            offset = page * ARCHIVE_PAGE_SIZE
            page_url = ARCHIVE_URL.format(date=date_str)
            if offset:
                page_url += f"&offset={offset}"

            try:
                html = await self._fetch_url(page_url)
                page_urls = self._extract_archive_links(html)
                if not page_urls:
                    break

                new = [u for u in page_urls if u not in urls]
                if not new:
                    break

                urls.extend(new)
                if len(urls) >= max_results:
                    break

                await self._delay()

            except Exception as e:
                self.logger.error(f"Ошибка архива {page_url}: {e}")
                break

        return urls[:max_results]

    def _extract_archive_links(self, html: str) -> list[str]:
        soup = BeautifulSoup(html, "html.parser")
        links: list[str] = []

        for selector in _ARCHIVE_LINK_SELECTORS:
            elements = soup.select(selector)
            for el in elements:
                href = el.get("href", "")
                if not href:
                    continue
                if not href.startswith("http"):
                    href = "https://www.rbc.ru" + href
                if "rbc.ru" in href and href not in links:
                    links.append(href)
            if links:
                break

        # Fallback: все ссылки внутри .search-item или похожих блоков
        if not links:
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if (
                    "rbc.ru" in href
                    and "/search/" not in href
                    and href not in links
                    and len(href) > 20
                ):
                    links.append(href)

        return links

    async def _fetch_article_full(self, url: str) -> tuple[str, str]:
        """Возвращает (title, content) для одной статьи."""
        try:
            html = await self._fetch_url(url)
        except ParserError:
            return "", ""

        soup = BeautifulSoup(html, "html.parser")

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

    @staticmethod
    def _strip_html(html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all(["script", "style"]):
            tag.decompose()
        return soup.get_text(separator="\n", strip=True)
