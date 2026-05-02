"""
Парсер Интерфакс (interfax.ru).
parse()        — RSS + загрузка полного текста со страницы статьи.
parse_period() — архивный парсер через /search/ (server-side HTML, ~50 статей/стр.).
"""

import asyncio
import hashlib
import re
from datetime import datetime, timedelta
from typing import Any

import feedparser
from bs4 import BeautifulSoup

from src.core.exceptions import ParserError
from src.core.models import ParsedItem
from src.parsers.base import BaseParser, ParserConfig, ParseResult
from src.utils.datetime_utils import MSK_TZ, parse_rfc2822_date
from src.utils.logging import log_async_execution_time

RSS_FEEDS: dict[str, str] = {
    "main":     "https://www.interfax.ru/rss.asp",
    # "russia":   "https://www.interfax.ru/russia/rss.asp",
    # "business": "https://www.interfax.ru/business/rss.asp",
    # "world":    "https://www.interfax.ru/world/rss.asp",
}

# Дата в формате YYYY-MM-DD; пагинация через &p=N
ARCHIVE_SEARCH_URL = (
    "https://www.interfax.ru/search/?phrase=&df={date}&dt={date}&sec=0&p={page}"
)

# Паттерн для извлечения относительных URL статей: /russia/1066065
_ARCHIVE_LINK_RE = re.compile(r'href="(/[a-z\-]+/\d+)"')

_ARTICLE_BODY_SELECTORS = [
    "article[itemprop='articleBody']",
    "div[itemprop='articleBody']",
]

_CONTENT_SELECTORS = [
    "article.textMaterial",
    "div.articleBody",
    "div.material-text",
    "div.article__text",
    "div.news-text",
]

_MAX_ARCHIVE_PAGES = 20


class InterfaxParser(BaseParser):
    """
    Парсер Интерфакс.
    Filters для parse():
        - sections: list[str] — ключи из RSS_FEEDS, по умолчанию ["main"]
        - min_length: int — минимальная длина текста (по умолчанию 100)
    Filters для parse_period():
        - max_per_day: int — максимум статей за день (по умолчанию 200)
        - min_length: int — минимальная длина текста (по умолчанию 100)
    """

    def __init__(self, config: ParserConfig):
        super().__init__(config)
        self.max_concurrent = 5

    @log_async_execution_time()
    async def parse(self, limit: int = 100, **filters) -> ParseResult:
        sections = filters.get("sections", ["main"])
        min_length = filters.get("min_length", 100)

        self.logger.info(f"Парсинг Interfax: лимит={limit}, разделы={sections}")

        rss_items = await self._fetch_rss(sections, limit * 2)
        if not rss_items:
            return ParseResult([])

        rss_items = rss_items[:limit]
        articles = await self._fetch_articles_parallel(rss_items, min_length)

        self.logger.info(f"Распарсено {len(articles)}/{len(rss_items)} статей")
        return ParseResult(articles)

    @log_async_execution_time()
    async def parse_period(
        self, start_date: datetime, end_date: datetime, limit: int = 100, **filters
    ) -> ParseResult:
        """Архивный парсинг через /search/ с итерацией по дням и страницам."""
        max_per_day = filters.get("max_per_day", 200)
        min_length = filters.get("min_length", 100)

        self.logger.info(f"Архивный парсинг Interfax: {start_date.date()} — {end_date.date()}")

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
            published_at=raw_data["published_at"],
            metadata={
                "section": raw_data.get("section", ""),
                "description": raw_data.get("description", ""),
                "text_length": len(raw_data["content"]),
            },
        )

    # ── RSS (parse) ────────────────────────────────────────────────────────────

    async def _fetch_rss(self, sections: list[str], limit: int) -> list[dict]:
        seen_urls: set[str] = set()
        items: list[dict] = []

        for section in sections:
            url = RSS_FEEDS.get(section, RSS_FEEDS["main"])
            try:
                xml_text = await self._fetch_url(url)
                loop = asyncio.get_event_loop()
                feed = await loop.run_in_executor(None, feedparser.parse, xml_text)

                if feed.bozo:
                    self.logger.warning(f"RSS warning ({section}): {feed.bozo_exception}")

                for entry in feed.entries:
                    link = entry.get("link", "")
                    if not link or link in seen_urls:
                        continue
                    seen_urls.add(link)

                    published_at = None
                    if entry.get("published"):
                        published_at = parse_rfc2822_date(entry.published)

                    items.append({
                        "link": link,
                        "title": entry.get("title", ""),
                        "summary": entry.get("summary", ""),
                        "published_at": published_at,
                        "section": section,
                    })

            except Exception as e:
                self.logger.error(f"Ошибка RSS {section}: {e}")

        return items[:limit]

    async def _fetch_articles_parallel(
        self, rss_items: list[dict], min_length: int
    ) -> list[ParsedItem]:
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def fetch_one(item: dict) -> ParsedItem | None:
            async with semaphore:
                try:
                    await self._delay()
                    content = await self._extract_article_text(item["link"])

                    # Fallback на RSS summary
                    if not content or len(content) < min_length:
                        content = item.get("summary", "")
                    if len(content) < min_length:
                        return None

                    url_hash = hashlib.md5(item["link"].encode()).hexdigest()[:12]
                    raw = {
                        "original_id": f"interfax_{url_hash}",
                        "url": item["link"],
                        "title": item["title"],
                        "content": content,
                        "published_at": item["published_at"],
                        "section": item["section"],
                        "description": item.get("summary", ""),
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
        self, date: datetime, max_articles: int, min_length: int
    ) -> list[ParsedItem]:
        date_str = date.strftime("%Y-%m-%d")
        all_paths: list[str] = []
        seen_paths: set[str] = set()

        for page in range(1, _MAX_ARCHIVE_PAGES + 1):
            search_url = ARCHIVE_SEARCH_URL.format(date=date_str, page=page)
            try:
                html = await self._fetch_url(search_url)
            except ParserError:
                self.logger.error(
                    f"Не удалось загрузить архив Interfax за {date_str} стр.{page}"
                )
                break

            new_paths = [
                p for p in dict.fromkeys(_ARCHIVE_LINK_RE.findall(html))
                if p not in seen_paths
            ]

            if not new_paths:
                break

            for p in new_paths:
                seen_paths.add(p)
                all_paths.append(p)
                if len(all_paths) >= max_articles:
                    break

            if len(all_paths) >= max_articles:
                break

            self.logger.debug(f"Interfax архив {date_str}: стр.{page} — {len(new_paths)} ссылок")
            await self._delay()

        article_urls = [
            f"https://www.interfax.ru{p}" for p in all_paths[:max_articles]
        ]

        if not article_urls:
            self.logger.warning(f"Нет ссылок в архиве Interfax за {date_str}")
            return []

        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def fetch_archive(url: str) -> ParsedItem | None:
            async with semaphore:
                try:
                    await self._delay()
                    title, content, published_at = await self._fetch_article_full(url)
                    if not content or len(content) < min_length:
                        return None
                    if not title:
                        title = "Без заголовка"

                    url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
                    raw = {
                        "original_id": f"interfax_{url_hash}",
                        "url": url,
                        "title": title,
                        "content": content,
                        "published_at": published_at or date,
                        "section": "",
                        "description": "",
                    }
                    parsed = self.to_parsed_item(raw)
                    return parsed if self._validate_item(parsed) else None
                except Exception as e:
                    self.logger.error(f"Ошибка архива {url}: {e}")
                    return None

        results = await asyncio.gather(*[fetch_archive(u) for u in article_urls])
        return [r for r in results if r is not None]

    async def _fetch_article_full(self, url: str) -> tuple[str, str, datetime | None]:
        """Возвращает (title, content, published_at) для одной статьи."""
        try:
            html = await self._fetch_url(url)
        except ParserError:
            return "", "", None

        soup = BeautifulSoup(html, "html.parser")

        title = ""
        h1 = soup.find("h1")
        if h1:
            title = h1.get_text(strip=True)
        if not title:
            og = soup.find("meta", property="og:title")
            if og:
                title = og.get("content", "").strip()

        content = self._extract_content_from_soup(soup)
        published_at = self._extract_published_at(soup)
        return title, content, published_at

    @staticmethod
    def _extract_published_at(soup: BeautifulSoup) -> datetime | None:
        def _to_msk_naive(val: str) -> datetime | None:
            try:
                dt = datetime.fromisoformat(val.replace("Z", "+00:00"))
                if dt.tzinfo is not None:
                    dt = dt.astimezone(MSK_TZ).replace(tzinfo=None)
                return dt
            except ValueError:
                return None

        meta = soup.find("meta", property="article:published_time")
        if meta:
            result = _to_msk_naive((meta.get("content") or "").strip())
            if result:
                return result

        time_el = soup.find("time", attrs={"datetime": True})
        if time_el:
            result = _to_msk_naive(time_el.get("datetime", ""))
            if result:
                return result

        return None

    # ── Общий экстрактор текста ────────────────────────────────────────────────

    async def _extract_article_text(self, url: str) -> str:
        try:
            html = await self._fetch_url(url)
        except ParserError:
            return ""
        return self._extract_content_from_soup(BeautifulSoup(html, "html.parser"))

    @staticmethod
    def _extract_content_from_soup(soup: BeautifulSoup) -> str:
        # Первый приоритет: article[itemprop="articleBody"] — только <p> теги, без мусора
        for selector in _ARTICLE_BODY_SELECTORS:
            body = soup.select_one(selector)
            if not body:
                continue
            ps = [p.get_text(strip=True) for p in body.find_all("p") if p.get_text(strip=True)]
            text = "\n\n".join(ps)
            if len(text) > 50:
                return text

        # Запасные CSS-селекторы
        for selector in _CONTENT_SELECTORS:
            container = soup.select_one(selector)
            if not container:
                continue
            for tag in container.find_all(["script", "style", "figure", "aside", "nav"]):
                tag.decompose()
            text = container.get_text(separator="\n", strip=True)
            if len(text) > 100:
                return text

        return ""
