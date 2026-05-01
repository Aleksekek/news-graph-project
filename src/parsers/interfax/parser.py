"""
Парсер Интерфакс (interfax.ru).
RSS — частичный текст, полный текст загружается со страницы статьи.
"""

import asyncio
import hashlib
from datetime import datetime
from typing import Any

import feedparser
from bs4 import BeautifulSoup

from src.core.exceptions import ParserError
from src.core.models import ParsedItem
from src.parsers.base import BaseParser, ParserConfig, ParseResult
from src.utils.datetime_utils import parse_rfc2822_date
from src.utils.logging import log_async_execution_time

RSS_FEEDS: dict[str, str] = {
    "main":     "https://www.interfax.ru/rss.asp",
    "russia":   "https://www.interfax.ru/russia/rss.asp",
    "business": "https://www.interfax.ru/business/rss.asp",
    "world":    "https://www.interfax.ru/world/rss.asp",
}

_CONTENT_SELECTORS = [
    "article.textMaterial",
    "div.articleBody",
    "div[itemprop='articleBody']",
    "div.material-text",
    "div.article__text",
    "div.news-text",
]


class InterfaxParser(BaseParser):
    """
    Парсер Интерфакс.
    Filters для parse():
        - sections: list[str] — ключи из RSS_FEEDS, по умолчанию ["main"]
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
        # RSS не хранит историю — фильтруем текущую ленту по дате
        result = await self.parse(limit=limit, **filters)
        filtered = [
            item for item in result.items
            if item.published_at and start_date <= item.published_at <= end_date
        ]
        return ParseResult(filtered)

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

    async def _extract_article_text(self, url: str) -> str:
        try:
            html = await self._fetch_url(url)
        except ParserError:
            return ""

        soup = BeautifulSoup(html, "html.parser")

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
