"""
Парсер ТАСС (tass.ru).
RSS — частичный текст, полный текст загружается со страницы статьи.

Cloudflare-bypass: используется curl-cffi с имитацией TLS-отпечатка Chrome,
что позволяет обходить JS-challenge без headless-браузера.
"""

import asyncio
import hashlib
import re
from datetime import datetime, timedelta
from typing import Any

import feedparser
from bs4 import BeautifulSoup

try:
    from curl_cffi.requests import AsyncSession as CurlSession
    _CURL_AVAILABLE = True
except ImportError:
    _CURL_AVAILABLE = False

from src.core.exceptions import ParserError
from src.core.models import ParsedItem
from src.parsers.base import BaseParser, ParserConfig, ParseResult
from src.utils.datetime_utils import MSK_TZ, parse_rfc2822_date
from src.utils.logging import log_async_execution_time

RSS_URL = "https://tass.ru/rss/v2.xml"

# ТАСС использует Next.js с CSS-модулями; имена классов содержат хеши.
# Стабильные селекторы — по тегу и data-атрибутам.
_CONTENT_SELECTORS = [
    "article",                          # ТАСС: ContentPageContainer_content (CSS-хеш меняется)
    "div[data-testid='article-body']",
    "div.article-body__content",
    "div.ArticleView__content",
    "div.text-content",
    "div.article__text",
    "div.article",
]

# Паттерн для архивных URL статей ТАСС из страницы поиска
_ARCHIVE_LINK_RE = re.compile(r'href="(/[a-z\-]+/\d+)"')

# Шаблон архивного поиска (UTC диапазон для московских суток)
ARCHIVE_SEARCH_URL = (
    "https://tass.ru/search"
    "?from_date={from_dt}&to_date={to_dt}&sort=date"
)

_MAX_ARCHIVE_PAGES = 20


class TassParser(BaseParser):
    """
    Парсер ТАСС.
    Filters для parse():
        - min_length: int — минимальная длина текста (по умолчанию 100)
    """

    def __init__(self, config: ParserConfig):
        super().__init__(config)
        self.max_concurrent = 5
        self._curl_session: "CurlSession | None" = None

    async def _setup_session(self):
        if _CURL_AVAILABLE:
            if self._curl_session is None:
                self._curl_session = CurlSession()
                self.logger.debug(f"curl-cffi сессия создана для {self.source_name}")
        else:
            await super()._setup_session()

    async def _close_session(self):
        if self._curl_session is not None:
            await self._curl_session.close()
            self._curl_session = None
            self.logger.debug(f"curl-cffi сессия закрыта для {self.source_name}")
        await super()._close_session()

    async def _fetch_url(self, url: str, **kwargs) -> str:
        if not _CURL_AVAILABLE or self._curl_session is None:
            return await super()._fetch_url(url, **kwargs)

        try:
            self.logger.debug(f"Загрузка: {url}")
            headers = kwargs.pop("headers", {})
            r = await self._curl_session.get(
                url,
                headers=headers,
                impersonate="chrome110",
                timeout=self.config.timeout,
            )
            if r.status_code == 404:
                self.logger.warning(f"Страница не найдена: {url}")
                raise ParserError(f"HTTP 404 для {url}")
            if r.status_code == 429:
                self.logger.warning(f"Rate limit для {url}, пауза 30 сек")
                await asyncio.sleep(30)
                raise ParserError(f"HTTP 429 для {url}")
            r.raise_for_status()
            self.logger.debug(f"Загружено: {url} ({len(r.text)} байт)")
            return r.text
        except ParserError:
            raise
        except Exception as e:
            raise ParserError(f"Ошибка загрузки {url}: {e}") from e

    @log_async_execution_time()
    async def parse(self, limit: int = 100, **filters) -> ParseResult:
        min_length = filters.get("min_length", 100)

        self.logger.info(f"Парсинг ТАСС: лимит={limit}")

        rss_items = await self._fetch_rss(limit * 2)
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
        """Архивный парсинг через tass.ru/search (SSR Next.js страница)."""
        min_length = filters.get("min_length", 100)

        self.logger.info(f"Архивный парсинг ТАСС: {start_date.date()} — {end_date.date()}")

        all_articles: list[ParsedItem] = []
        current = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end_day = end_date.replace(hour=23, minute=59, second=59)

        while current <= end_day and len(all_articles) < limit:
            remaining = limit - len(all_articles)
            day_articles = await self._parse_archive_day(current, remaining, min_length)
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
                "description": raw_data.get("description", ""),
                "text_length": len(raw_data["content"]),
            },
        )

    async def _fetch_rss(self, limit: int) -> list[dict]:
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
                    "summary": entry.get("summary", ""),
                    "published_at": published_at,
                })

            return items

        except Exception as e:
            self.logger.error(f"Ошибка RSS: {e}")
            return []

    async def _fetch_articles_parallel(
        self, rss_items: list[dict], min_length: int
    ) -> list[ParsedItem]:
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def fetch_one(item: dict) -> ParsedItem | None:
            async with semaphore:
                try:
                    await self._delay()
                    content = await self._extract_article_text(item["link"])

                    if not content or len(content) < min_length:
                        content = item.get("summary", "")
                    if len(content) < min_length:
                        return None

                    url_hash = hashlib.md5(item["link"].encode()).hexdigest()[:12]
                    raw = {
                        "original_id": f"tass_{url_hash}",
                        "url": item["link"],
                        "title": item["title"],
                        "content": content,
                        "published_at": item["published_at"],
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
        """Архивный парсинг одного дня через tass.ru/search (SSR Next.js)."""
        # ТАСС хранит даты в UTC; московские сутки = UTC предыдущего дня 21:00 → 20:59:59
        msk_start = date.replace(tzinfo=MSK_TZ)
        utc_start = msk_start.astimezone(__import__("datetime").timezone.utc)
        utc_end = (msk_start + timedelta(days=1)).astimezone(__import__("datetime").timezone.utc)
        from_dt = utc_start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        to_dt = utc_end.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        all_paths: list[str] = []
        seen_paths: set[str] = set()

        for page in range(1, _MAX_ARCHIVE_PAGES + 1):
            search_url = ARCHIVE_SEARCH_URL.format(from_dt=from_dt, to_dt=to_dt)
            if page > 1:
                search_url += f"&page={page}"

            try:
                html = await self._fetch_url(search_url)
            except ParserError:
                self.logger.error(f"Не удалось загрузить архив ТАСС за {date.date()} стр.{page}")
                break

            new_paths = [
                p for p in dict.fromkeys(_ARCHIVE_LINK_RE.findall(html))
                if p not in seen_paths
                and not p.startswith("/search")
                and not p.startswith("/static")
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

            await self._delay()

        article_urls = [f"https://tass.ru{p}" for p in all_paths[:max_articles]]

        if not article_urls:
            self.logger.warning(f"Нет ссылок в архиве ТАСС за {date.date()}")
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
                        "original_id": f"tass_{url_hash}",
                        "url": url,
                        "title": title,
                        "content": content,
                        "published_at": published_at or date,
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

        content = self._extract_article_text_from_soup(soup)
        published_at = self._extract_published_at(soup)
        return title, content, published_at

    async def _extract_article_text(self, url: str) -> str:
        try:
            html = await self._fetch_url(url)
        except ParserError:
            return ""
        return self._extract_article_text_from_soup(BeautifulSoup(html, "html.parser"))

    @staticmethod
    def _extract_article_text_from_soup(soup: BeautifulSoup) -> str:
        for selector in _CONTENT_SELECTORS:
            container = soup.select_one(selector)
            if not container:
                continue
            for tag in container.find_all(["script", "style", "figure", "aside", "nav"]):
                tag.decompose()
            # Предпочитаем <p>-теги для чистого текста
            ps = [p.get_text(strip=True) for p in container.find_all("p") if len(p.get_text(strip=True)) > 30]
            if ps:
                text = "\n\n".join(ps)
                if len(text) > 100:
                    return text
            # Fallback: весь текст контейнера
            text = container.get_text(separator="\n", strip=True)
            if len(text) > 100:
                return text

        return ""

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
