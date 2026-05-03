"""
Парсер ТАСС (tass.ru).
parse()        — RSS yandex.xml: полный текст уже в фиде (yandex_full-text), HTML не нужен.
parse_period() — архивный парсинг через sitemap_news{N}.xml; sitemap кешируется на сессию.

Cloudflare-bypass: используется curl-cffi с имитацией TLS-отпечатка Chrome,
что позволяет обходить JS-challenge без headless-браузера.
"""

import asyncio
import hashlib
import html
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

# Yandex-специфичный RSS включает <yandex:full-text> с полным текстом статьи.
# feedparser отдаёт его как ключ "yandex_full-text".
RSS_URL = "https://tass.ru/rss/yandex.xml"

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

# Sitemap: индекс sitemap_news{N}.xml перечисляют все статьи с <lastmod>.
# sitemap_news0 — самые свежие, более высокие N — старее.
# На 2026-05 у ТАСС около 68 sitemap'ов; ставим запас.
_SITEMAP_NEWS_URL = "https://tass.ru/sitemap/sitemap_news{n}.xml"
_MAX_SITEMAP_INDEX = 100

# Извлечение <loc>…</loc> и следующего <lastmod>…</lastmod> из sitemap-XML.
_SITEMAP_URL_RE = re.compile(r"<loc>([^<]+)</loc>\s*<lastmod>([^<]+)</lastmod>")
# Фильтр: оставляем только URL, похожие на статьи (/section/digits).
_TASS_ARTICLE_URL_RE = re.compile(r"^https://tass\.ru/[a-z\-]+/\d{6,}$")


class TassParser(BaseParser):
    """
    Парсер ТАСС.
    Filters для parse():
        - min_length: int — минимальная длина текста (по умолчанию 100)
    Filters для parse_period():
        - min_length: int — минимальная длина текста (по умолчанию 100)
    """

    def __init__(self, config: ParserConfig):
        super().__init__(config)
        self.max_concurrent = 5
        self._curl_session: "CurlSession | None" = None
        # Кеш sitemap_news{N} на жизнь сессии: N → list[(url, lastmod)] или None (404).
        self._sitemap_cache: dict[int, list[tuple[str, datetime]] | None] = {}

    async def _setup_session(self):
        if _CURL_AVAILABLE:
            if self._curl_session is None:
                self._curl_session = CurlSession()
                self.logger.debug(f"curl-cffi сессия создана для {self.source_name}")
        else:
            await super()._setup_session()

    async def _close_session(self):
        self._sitemap_cache.clear()
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
        """Архивный парсинг через sitemap_news{N}.xml + индивидуальный fetch HTML."""
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

    # ── Свежие новости (parse) ─────────────────────────────────────────────────

    async def _fetch_rss(self, limit: int) -> list[dict]:
        try:
            xml_text = await self._fetch_url(RSS_URL)
            loop = asyncio.get_event_loop()
            feed = await loop.run_in_executor(None, feedparser.parse, xml_text)

            if feed.bozo:
                self.logger.warning(f"RSS warning: {feed.bozo_exception}")

            items = []
            seen_urls: set[str] = set()
            for entry in feed.entries[:limit]:
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
                    # yandex.xml содержит полный текст в <yandex:full-text>
                    "full_text_raw": entry.get("yandex_full-text", ""),
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
            try:
                # 1. Пробуем взять текст прямо из RSS (yandex_full-text)
                content = self._clean_rss_full_text(item.get("full_text_raw", ""))

                # 2. Fallback: если в RSS пусто/коротко — идём за HTML страницей
                if not content or len(content) < min_length:
                    async with semaphore:
                        await self._delay()
                        page_content = await self._extract_article_text(item["link"])
                    if page_content and len(page_content) >= min_length:
                        content = page_content
                    elif item.get("summary"):
                        content = item["summary"]

                if not content or len(content) < min_length:
                    return None

                url_hash = hashlib.md5(item["link"].encode()).hexdigest()[:12]
                raw = {
                    "original_id": f"tass_{url_hash}",
                    "url": item["link"],
                    "title": item["title"] or "Без заголовка",
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

    @staticmethod
    def _clean_rss_full_text(raw: str) -> str:
        """Очищает yandex_full-text: HTML-сущности, теги, нормализация пробелов."""
        if not raw:
            return ""
        text = html.unescape(raw)
        # <br>, <br/>, <br /> → перенос строки
        text = re.sub(r"<\s*br\s*/?\s*>", "\n", text, flags=re.I)
        # <p>, </p> → двойной перенос (граница абзаца)
        text = re.sub(r"</?p[^>]*>", "\n\n", text, flags=re.I)
        # Прочие теги — выкидываем
        text = re.sub(r"<[^>]+>", "", text)
        # Нормализация пробелов и переносов
        text = re.sub(r"[ \t]+", " ", text)
        text = re.sub(r"\n[ \t]+", "\n", text)
        text = re.sub(r"\n\s*\n+", "\n\n", text)
        return text.strip()

    # ── Архив через sitemap (parse_period) ─────────────────────────────────────

    async def _parse_archive_day(
        self, date: datetime, max_articles: int, min_length: int
    ) -> list[ParsedItem]:
        target_day = date.date()
        url_lastmods = await self._get_sitemap_urls_for_date(date)
        if not url_lastmods:
            self.logger.warning(f"Sitemap не вернул статей для {target_day}")
            return []

        self.logger.info(f"Sitemap для {target_day}: {len(url_lastmods)} статей")
        if len(url_lastmods) > max_articles:
            url_lastmods = url_lastmods[:max_articles]

        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def fetch_archive(url: str, sitemap_lastmod: datetime) -> ParsedItem | None:
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
                        # published_at со страницы → sitemap lastmod → midnight целевого дня
                        "published_at": published_at or sitemap_lastmod or date,
                        "description": "",
                    }
                    parsed = self.to_parsed_item(raw)
                    return parsed if self._validate_item(parsed) else None
                except Exception as e:
                    self.logger.error(f"Ошибка архива {url}: {e}")
                    return None

        results = await asyncio.gather(*[fetch_archive(u, lm) for u, lm in url_lastmods])
        return [r for r in results if r is not None]

    async def _get_sitemap_urls_for_date(
        self, target_date: datetime
    ) -> list[tuple[str, datetime]]:
        """Ищет (url, lastmod) для статей с lastmod.date() == target_date через sitemap_news{N}.xml.

        Стратегия: sitemap_news0 — самые свежие; идём в сторону больших N
        (более старые статьи), пока не найдём sitemap, покрывающий target_date.
        Кешируем уже скачанные sitemap'ы на жизнь сессии.
        """
        target_day = target_date.date()

        for n in range(_MAX_SITEMAP_INDEX):
            entries = await self._get_sitemap_news(n)
            if entries is None:
                # 404 — sitemap'ов больше нет (вышли за пределы архива)
                self.logger.info(f"sitemap_news{n} не существует — архив исчерпан")
                return []
            if not entries:
                continue

            min_date = min(lm.date() for _, lm in entries)
            max_date = max(lm.date() for _, lm in entries)

            if max_date < target_day:
                # Цель новее всех записей этого sitemap'а; для свежих дат должен
                # сработать parse() на yandex.xml, не parse_period.
                self.logger.warning(
                    f"target_day={target_day} новее, чем максимум sitemap_news{n} ({max_date})"
                )
                return []
            if min_date > target_day:
                # Цель старее этого sitemap'а — пробуем следующий (более старые статьи)
                continue

            # target_day внутри диапазона sitemap'а
            return [(url, lm) for url, lm in entries if lm.date() == target_day]

        return []

    async def _get_sitemap_news(self, n: int) -> list[tuple[str, datetime]] | None:
        """Возвращает [(url, lastmod_msk_naive), ...] или None при 404."""
        if n in self._sitemap_cache:
            return self._sitemap_cache[n]

        url = _SITEMAP_NEWS_URL.format(n=n)
        try:
            xml_text = await self._fetch_url(url)
        except ParserError as e:
            if "404" in str(e):
                self._sitemap_cache[n] = None
                return None
            self.logger.error(f"Ошибка загрузки {url}: {e}")
            self._sitemap_cache[n] = []
            return []

        entries: list[tuple[str, datetime]] = []
        for loc, lm_str in _SITEMAP_URL_RE.findall(xml_text):
            if not _TASS_ARTICLE_URL_RE.match(loc):
                continue
            try:
                lm = datetime.fromisoformat(lm_str.strip())
                if lm.tzinfo is not None:
                    lm = lm.astimezone(MSK_TZ).replace(tzinfo=None)
                entries.append((loc, lm))
            except ValueError:
                continue

        self._sitemap_cache[n] = entries
        return entries

    # ── Вытяжка статьи со страницы (общая для fresh fallback и архива) ─────────

    async def _fetch_article_full(self, url: str) -> tuple[str, str, datetime | None]:
        """Возвращает (title, content, published_at) для одной статьи."""
        try:
            page_html = await self._fetch_url(url)
        except ParserError:
            return "", "", None

        soup = BeautifulSoup(page_html, "html.parser")

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
            page_html = await self._fetch_url(url)
        except ParserError:
            return ""
        return self._extract_article_text_from_soup(BeautifulSoup(page_html, "html.parser"))

    @staticmethod
    def _extract_article_text_from_soup(soup: BeautifulSoup) -> str:
        for selector in _CONTENT_SELECTORS:
            container = soup.select_one(selector)
            if not container:
                continue
            for tag in container.find_all(["script", "style", "figure", "aside", "nav"]):
                tag.decompose()
            # Предпочитаем <p>-теги для чистого текста
            ps = [p.get_text(" ", strip=True) for p in container.find_all("p") if len(p.get_text(strip=True)) > 30]
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
