"""
Парсер Lenta.ru.
Поддерживает RSS и HTML парсинг с единой политикой часовых поясов.
"""

import asyncio
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Set

import feedparser
from bs4 import BeautifulSoup

from src.core.exceptions import ParserError
from src.core.models import ParsedItem
from src.parsers.base import BaseParser, ParserConfig, ParseResult
from src.utils.datetime_utils import parse_html_date, parse_rfc2822_date
from src.utils.logging import log_async_execution_time


class LentaParser(BaseParser):
    """
    Парсер Lenta.ru.
    Параметры через filters в parse():
        - categories: List[str] - фильтр по категориям
        - min_length: int - минимальная длина статьи (по умолчанию 100)
    """

    def __init__(self, config: ParserConfig):
        super().__init__(config)
        self.base_url = "https://lenta.ru"
        self.rss_url = f"{self.base_url}/rss"

        # Категории из конфига (если есть)
        self.default_categories = getattr(config, "categories", None)

        # Параметры парсинга
        self.max_concurrent = 5  # Максимум параллельных запросов

    @log_async_execution_time()
    async def parse(self, limit: int = 100, **filters) -> ParseResult:
        """
        Парсинг свежих статей через RSS.

        Filters:
            categories: List[str] - фильтр по категориям
            min_length: int - минимальная длина текста
        """
        categories = filters.get("categories", self.default_categories)
        min_length = filters.get("min_length", 100)

        self.logger.info(f"Парсинг Lenta.ru: лимит={limit}, категории={categories}")

        # 1. Получаем RSS
        rss_items = await self._fetch_rss(limit * 2, categories)

        if not rss_items:
            self.logger.warning("RSS не вернул статей")
            return ParseResult([])

        # 2. Ограничиваем количество
        rss_items = rss_items[:limit]

        # 3. Параллельная загрузка статей
        articles = await self._fetch_articles_parallel(rss_items, min_length)

        self.logger.info(f"Распарсено {len(articles)}/{len(rss_items)} статей")

        return ParseResult(articles)

    @log_async_execution_time()
    async def parse_period(
        self, start_date: datetime, end_date: datetime, limit: int = 100, **filters
    ) -> ParseResult:
        """
        Архивный парсинг Lenta.ru за период.

        Filters:
            categories: List[str] - фильтр по категориям
            max_per_day: int - максимум статей в день (default 20)
            max_pages_per_day: int - максимум страниц в день (default 5)
        """
        categories = filters.get("categories", self.default_categories)
        max_per_day = filters.get("max_per_day", 20)
        max_pages_per_day = filters.get("max_pages_per_day", 5)
        min_length = filters.get("min_length", 100)

        self.logger.info(
            f"Архивный парсинг Lenta: {start_date.date()} - {end_date.date()}"
        )

        all_articles = []
        current_date = start_date

        while current_date <= end_date and len(all_articles) < limit:
            self.logger.info(f"Парсинг дня: {current_date.date()}")

            day_articles = await self._parse_day(
                date=current_date,
                max_articles=max_per_day,
                max_pages=max_pages_per_day,
                categories=categories,
                min_length=min_length,
            )

            all_articles.extend(day_articles)

            if len(all_articles) > limit:
                all_articles = all_articles[:limit]

            current_date = current_date.replace(day=current_date.day + 1)
            await self._delay()

        self.logger.info(f"Архивный парсинг завершён: {len(all_articles)} статей")

        return ParseResult(all_articles)

    def to_parsed_item(self, raw_data: Dict[str, Any]) -> ParsedItem:
        """Конвертация сырых данных в ParsedItem."""
        return ParsedItem(
            source_id=self.source_id,
            source_name=self.source_name,
            original_id=raw_data["original_id"],
            url=raw_data["url"],
            title=raw_data["title"],
            content=raw_data["content"],
            published_at=raw_data["published_at"],
            author=raw_data.get("author"),
            metadata={
                "category": raw_data.get("category", ""),
                "description": raw_data.get("description", ""),
                "text_length": len(raw_data["content"]),
            },
            raw_data={
                "rss": raw_data.get("rss_data", {}),
                "html": raw_data.get("html", ""),
            },
        )

    # ==================== Внутренние методы ====================

    async def _fetch_rss(
        self, limit: int, categories: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Получение RSS ленты с фильтрацией."""
        self.logger.debug(f"Загрузка RSS: {self.rss_url}")

        try:
            # feedparser синхронный, но быстрый
            loop = asyncio.get_event_loop()
            feed = await loop.run_in_executor(None, feedparser.parse, self.rss_url)

            if feed.bozo:
                self.logger.warning(f"RSS warning: {feed.bozo_exception}")

            items = []
            for entry in feed.entries[:limit]:
                # Извлекаем категорию
                category = ""
                if entry.get("tags"):
                    category = entry.tags[0].term if entry.tags else ""

                # Фильтр по категориям
                if categories and category:
                    if not any(cat.lower() in category.lower() for cat in categories):
                        continue

                # Парсим дату (RSS в UTC, конвертируем в MSK)
                published_at = None
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    import time

                    timestamp = time.mktime(entry.published_parsed)
                    from datetime import timezone

                    utc_dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                    published_at = self._to_msk(utc_dt)

                items.append(
                    {
                        "guid": entry.get("id", ""),
                        "title": entry.get("title", ""),
                        "link": entry.get("link", ""),
                        "author": entry.get("author", ""),
                        "published_at": published_at,
                        "category": category,
                        "summary": entry.get("summary", ""),
                    }
                )

            self.logger.info(f"RSS загружен: {len(items)} статей")
            return items

        except Exception as e:
            raise ParserError(f"Ошибка загрузки RSS: {e}")

    async def _fetch_articles_parallel(
        self, rss_items: List[Dict], min_length: int
    ) -> List[ParsedItem]:
        """Параллельная загрузка и парсинг статей."""
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def fetch_one(item: Dict) -> Optional[ParsedItem]:
            async with semaphore:
                try:
                    article_data = await self._parse_article_page(
                        item["link"], item["published_at"]
                    )

                    if not article_data:
                        return None

                    if len(article_data["content"]) < min_length:
                        self.logger.debug(f"Статья слишком короткая: {item['link']}")
                        return None

                    return self.to_parsed_item(article_data)

                except Exception as e:
                    self.logger.error(f"Ошибка {item['link']}: {e}")
                    return None

        tasks = [fetch_one(item) for item in rss_items]
        results = await asyncio.gather(*tasks)

        return [r for r in results if r is not None]

    async def _parse_article_page(
        self, url: str, fallback_date: Optional[datetime] = None
    ) -> Optional[Dict[str, Any]]:
        """Парсинг HTML страницы статьи."""
        html = await self._fetch_url(url)
        soup = BeautifulSoup(html, "html.parser")

        # Извлекаем данные
        title = self._extract_title(soup)
        if not title:
            return None

        content = self._extract_content(soup)
        if not content or len(content) < 50:
            return None

        author = self._extract_author(soup)

        # Дата: сначала из HTML, потом fallback из RSS
        published_at = self._extract_published_time(soup)
        if not published_at and fallback_date:
            published_at = fallback_date

        category = self._extract_category(soup)
        description = self._extract_description(soup)

        # Генерируем ID
        import hashlib

        url_hash = hashlib.md5(url.encode()).hexdigest()[:12]
        original_id = f"lenta_{url_hash}"

        return {
            "original_id": original_id,
            "url": url,
            "title": title,
            "content": content,
            "published_at": published_at,
            "author": author,
            "category": category,
            "description": description,
            "html": html,
            "rss_data": None,
        }

    async def _parse_day(
        self,
        date: datetime,
        max_articles: int,
        max_pages: int,
        categories: Optional[List[str]],
        min_length: int,
    ) -> List[ParsedItem]:
        """Парсинг одного дня архива."""
        date_str = date.strftime("%Y/%m/%d")
        archive_url = f"{self.base_url}/news/{date_str}/"

        # Получаем ссылки на статьи
        article_urls = await self._get_archive_links(archive_url, max_pages, categories)

        if not article_urls:
            return []

        # Ограничиваем количество
        if len(article_urls) > max_articles:
            article_urls = article_urls[:max_articles]

        # Параллельная загрузка
        semaphore = asyncio.Semaphore(self.max_concurrent)

        async def fetch_archive(url: str) -> Optional[ParsedItem]:
            async with semaphore:
                try:
                    article_data = await self._parse_article_page(url)

                    if not article_data:
                        return None

                    if len(article_data["content"]) < min_length:
                        return None

                    # Для архива дата может быть из URL
                    if not article_data["published_at"]:
                        article_data["published_at"] = date

                    return self.to_parsed_item(article_data)

                except Exception as e:
                    self.logger.error(f"Ошибка {url}: {e}")
                    return None

        tasks = [fetch_archive(url) for url in article_urls]
        results = await asyncio.gather(*tasks)

        return [r for r in results if r is not None]

    async def _get_archive_links(
        self,
        archive_url: str,
        max_pages: int,
        categories: Optional[List[str]],
    ) -> List[str]:
        """Получение ссылок на статьи из архивной страницы."""
        all_urls = []

        for page in range(1, max_pages + 1):
            if page == 1:
                page_url = archive_url
            else:
                page_url = archive_url.rstrip("/") + f"/page/{page}/"

            try:
                html = await self._fetch_url(page_url)
                soup = BeautifulSoup(html, "html.parser")

                page_urls = []

                # Ищем карточки новостей
                cards = soup.find_all("div", class_="card-full-news")
                for card in cards:
                    # Ищем ссылку
                    link = card.find("a", href=True)
                    if not link:
                        continue

                    href = link.get("href", "")
                    if not href or not href.startswith("/news/"):
                        continue

                    # Фильтр по категориям
                    if categories:
                        rubric = card.find("span", class_="card-full-news__rubric")
                        rubric_text = rubric.get_text(strip=True) if rubric else ""
                        if rubric_text not in categories:
                            continue

                    full_url = self.base_url + href
                    page_urls.append(full_url)

                if not page_urls:
                    # Пробуем регуляркой
                    pattern = r'href=["\'](/news/\d{4}/\d{2}/\d{2}/[^"\']*?/)["\']'
                    matches = re.findall(pattern, html)
                    for match in matches:
                        full_url = self.base_url + match
                        page_urls.append(full_url)

                # Убираем дубликаты и добавляем
                new_urls = [u for u in set(page_urls) if u not in all_urls]
                if not new_urls:
                    break

                all_urls.extend(new_urls)
                self.logger.debug(f"Страница {page}: +{len(new_urls)} ссылок")

                await self._delay()

            except Exception as e:
                self.logger.error(f"Ошибка страницы {page_url}: {e}")
                break

        return list(set(all_urls))

    # ==================== HTML парсинг ====================

    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Извлечение заголовка."""
        selectors = [
            "h1.topic-body__title",
            "h1",
            'meta[property="og:title"]',
            "title",
        ]

        for selector in selectors:
            try:
                if selector.startswith("meta"):
                    elem = soup.find("meta", property="og:title")
                    if elem and elem.get("content"):
                        return elem["content"].strip()
                else:
                    elem = soup.select_one(selector)
                    if elem and elem.get_text(strip=True):
                        return elem.get_text(strip=True)
            except:
                continue

        return "Без заголовка"

    def _extract_content(self, soup: BeautifulSoup) -> str:
        """Извлечение текста статьи."""
        selectors = [
            'div[id^="articleBody_"]',
            ".topic-body__content-text",
            ".article-content",
            "article",
        ]

        for selector in selectors:
            try:
                container = soup.select_one(selector)
                if container:
                    # Собираем все параграфы
                    paragraphs = container.find_all(["p", "div"], recursive=False)
                    if paragraphs:
                        text_parts = []
                        for p in paragraphs:
                            text = p.get_text(separator=" ", strip=True)
                            if text and len(text) > 20:
                                text_parts.append(text)

                        if text_parts:
                            return "\n\n".join(text_parts)

                    # Если нет параграфов, берём весь текст
                    full_text = container.get_text(separator="\n", strip=True)
                    if len(full_text) > 100:
                        return full_text
            except:
                continue

        return ""

    def _extract_author(self, soup: BeautifulSoup) -> Optional[str]:
        """Извлечение автора."""
        selectors = [
            "a.topic-authors__author",
            ".topic-authors__name",
            'meta[name="author"]',
        ]

        for selector in selectors:
            try:
                if selector.startswith("meta"):
                    elem = soup.find("meta", name="author")
                    if elem and elem.get("content"):
                        return elem["content"].strip()
                else:
                    elem = soup.select_one(selector)
                    if elem:
                        text = elem.get_text(strip=True)
                        if text and len(text) > 2:
                            return text
            except:
                continue

        return None

    def _extract_published_time(self, soup: BeautifulSoup) -> Optional[datetime]:
        """Извлечение даты публикации."""
        # Мета-теги
        meta_selectors = [
            'meta[property="article:published_time"]',
            'meta[name="pubdate"]',
        ]

        for selector in meta_selectors:
            try:
                elem = soup.select_one(selector)
                if elem and elem.get("content"):
                    dt = parse_html_date(elem["content"], source="lenta")
                    if dt:
                        return dt
            except:
                continue

        # Элементы времени на странице
        time_selectors = [
            "a.topic-header__time",
            "span.topic-header__time",
            ".topic-header__time",
            "time[datetime]",
        ]

        for selector in time_selectors:
            try:
                elem = soup.select_one(selector)
                if elem:
                    # Пробуем datetime атрибут
                    datetime_attr = elem.get("datetime")
                    if datetime_attr:
                        dt = parse_html_date(datetime_attr, source="lenta")
                        if dt:
                            return dt

                    # Пробуем текст
                    text = elem.get_text(strip=True)
                    if text:
                        dt = parse_html_date(text, source="lenta")
                        if dt:
                            return dt
            except:
                continue

        return None

    def _extract_category(self, soup: BeautifulSoup) -> str:
        """Извлечение категории."""
        selectors = [
            "a.topic-header__rubric",
            ".rubric",
            'meta[property="article:section"]',
        ]

        for selector in selectors:
            try:
                if selector.startswith("meta"):
                    elem = soup.find("meta", property="article:section")
                    if elem and elem.get("content"):
                        return elem["content"].strip()
                else:
                    elem = soup.select_one(selector)
                    if elem:
                        text = elem.get_text(strip=True)
                        if text:
                            return text
            except:
                continue

        return ""

    def _extract_description(self, soup: BeautifulSoup) -> str:
        """Извлечение описания."""
        selectors = [
            'meta[name="description"]',
            'meta[property="og:description"]',
            ".lead",
        ]

        for selector in selectors:
            try:
                if selector.startswith("meta"):
                    elem = soup.select_one(selector)
                    if elem and elem.get("content"):
                        return elem["content"].strip()
                else:
                    elem = soup.select_one(selector)
                    if elem:
                        text = elem.get_text(strip=True)
                        if text:
                            return text
            except:
                continue

        return ""

    def _to_msk(self, utc_dt: datetime) -> datetime:
        """Конвертация UTC в MSK naive."""
        from src.utils.datetime_utils import utc_to_msk

        return utc_to_msk(utc_dt)
