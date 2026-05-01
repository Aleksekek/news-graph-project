"""
Парсер РБК (rbc.ru).
Использует full.rss — полный текст статьи уже в описании фида, HTML не нужен.
"""

import asyncio
import hashlib
from datetime import datetime
from typing import Any

import feedparser
from bs4 import BeautifulSoup

from src.core.models import ParsedItem
from src.parsers.base import BaseParser, ParserConfig, ParseResult
from src.utils.datetime_utils import parse_rfc2822_date
from src.utils.logging import log_async_execution_time

# Полный текст в фиде (20 последних статей)
RSS_URL = "https://rssexport.rbc.ru/rbcnews/news/20/full.rss"


class RbcParser(BaseParser):
    """
    Парсер РБК.
    Полный текст берётся из RSS description — запросы к сайту не нужны.
    Filters для parse():
        - min_length: int — минимальная длина текста (по умолчанию 100)
    """

    @log_async_execution_time()
    async def parse(self, limit: int = 100, **filters) -> ParseResult:
        min_length = filters.get("min_length", 100)

        self.logger.info(f"Парсинг РБК: лимит={limit}")

        items = await self._fetch_rss(limit, min_length)

        self.logger.info(f"Распарсено {len(items)} статей")
        return ParseResult(items)

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
                "text_length": len(raw_data["content"]),
            },
        )

    async def _fetch_rss(self, limit: int, min_length: int) -> list[ParsedItem]:
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

                content = self._extract_content(entry)
                if len(content) < min_length:
                    continue

                url_hash = hashlib.md5(link.encode()).hexdigest()[:12]
                raw = {
                    "original_id": f"rbc_{url_hash}",
                    "url": link,
                    "title": entry.get("title", ""),
                    "content": content,
                    "published_at": published_at,
                }
                parsed = self.to_parsed_item(raw)
                if self._validate_item(parsed):
                    items.append(parsed)

            return items

        except Exception as e:
            self.logger.error(f"Ошибка RSS: {e}")
            return []

    def _extract_content(self, entry: Any) -> str:
        """Извлекает полный текст из RSS entry (content:encoded или description)."""
        # feedparser кладёт <content:encoded> в entry.content[0].value
        if hasattr(entry, "content") and entry.content:
            html = entry.content[0].get("value", "")
            if html:
                return self._strip_html(html)

        # Fallback: summary (description)
        summary = entry.get("summary", "")
        if summary:
            return self._strip_html(summary)

        return ""

    @staticmethod
    def _strip_html(html: str) -> str:
        soup = BeautifulSoup(html, "html.parser")
        for tag in soup.find_all(["script", "style"]):
            tag.decompose()
        return soup.get_text(separator="\n", strip=True)
