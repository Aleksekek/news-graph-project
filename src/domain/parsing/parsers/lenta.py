"""
Парсер Lenta.ru с унифицированным интерфейсом.
"""

import asyncio
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set
from urllib.parse import urljoin

import feedparser
from bs4 import BeautifulSoup

from src.core.exceptions import ParserError
from src.domain.parsing.base import BaseParser, ParsedItem, ParserConfig
from src.utils.data import clean_text, extract_domain, safe_datetime, safe_str
from src.utils.logging import log_async_execution_time
from src.utils.retry import retry_network


class LentaParser(BaseParser):
    """
    Парсер Lenta.ru с поддержкой RSS и HTML парсинга.
    Унифицированный интерфейс с поддержкой **kwargs.
    """

    def __init__(self, config: ParserConfig, max_concurrent_requests: int = 5):
        super().__init__(config)

        # Настройки специфичные для Lenta
        self.base_url = "https://lenta.ru"
        self.rss_url = f"{self.base_url}/rss"
        self.archive_base_url = f"{self.base_url}/news"

        # Категории из конфигурации
        self.default_categories = getattr(config, "categories", None)

        # Кэширование
        self._last_rss_fetch = None
        self._rss_cache = []

        # Асинхронная загрузка
        self.max_concurrent_requests = max_concurrent_requests

    @log_async_execution_time()
    async def parse(
        self,
        limit: int = 100,
        categories: Optional[List[str]] = None,
        **kwargs,
    ) -> List[ParsedItem]:
        """
        Унифицированный метод парсинга Lenta.ru.

        Args:
            limit: Максимальное количество новостей
            categories: Фильтр по категориям (если None, берем из конфига)
            **kwargs: Дополнительные параметры

        Returns:
            Список ParsedItem
        """
        # Используем переданные категории или из конфига
        target_categories = categories if categories else self.default_categories

        self.logger.info(
            f"Парсинг Lenta.ru: лимит={limit}, категории={target_categories}"
        )

        try:
            # Получаем RSS с фильтрацией по категориям
            rss_items = await self._fetch_rss_feed(limit, target_categories)

            # Парсим каждую статью
            parsed_items = []
            for i, rss_item in enumerate(rss_items[:limit]):
                try:
                    self.logger.debug(
                        f"Парсинг статьи {i+1}/{len(rss_items)}: {rss_item.get('title', '')[:50]}..."
                    )

                    # Парсим HTML страницу
                    article_data = await self._parse_article_page(rss_item["link"])

                    if article_data:
                        # Создаем ParsedItem
                        parsed_item = self._create_parsed_item(rss_item, article_data)

                        if self.validate_item(parsed_item):
                            parsed_items.append(parsed_item)

                        # Задержка между запросами
                        if i < len(rss_items) - 1:
                            await self.delay_between_requests()

                except Exception as e:
                    self.logger.error(f"Ошибка парсинга статьи: {e}")
                    continue

            self.logger.info(
                f"Успешно распарсено {len(parsed_items)}/{len(rss_items)} статей"
            )
            return parsed_items

        except Exception as e:
            self.logger.error(f"Ошибка парсинга Lenta.ru: {e}")
            raise ParserError(f"Ошибка парсинга Lenta.ru: {e}")

    @log_async_execution_time()
    async def parse_period(
        self,
        start_date: datetime,
        end_date: datetime,
        limit: int = 100,
        max_articles_per_day: int = 999,
        max_pages_per_day: int = 999,
        skip_existing_urls: Optional[Set[str]] = None,
        categories: Optional[List[str]] = None,
        **kwargs,
    ) -> List[ParsedItem]:
        """
        Архивный парсинг Lenta.ru за период.

        Args:
            start_date: Начальная дата
            end_date: Конечная дата
            limit: Общий лимит статей
            max_articles_per_day: Максимум статей в день
            max_pages_per_day: Максимум страниц в день
            skip_existing_urls: URL для пропуска
            categories: Фильтр по категориям (если None, берем из конфига)
            **kwargs: Дополнительные параметры
        """
        target_categories = categories if categories else self.default_categories

        self.logger.info(
            f"Архивный парсинг Lenta.ru: {start_date.date()} - {end_date.date()}, "
            f"категории: {target_categories}"
        )

        parsed_items = []
        current_date = start_date

        while current_date <= end_date and len(parsed_items) < limit:
            try:
                day_items = await self._parse_archive_day(
                    date=current_date,
                    max_articles_per_day=min(
                        max_articles_per_day, limit - len(parsed_items)
                    ),
                    max_pages_per_day=max_pages_per_day,
                    skip_existing_urls=skip_existing_urls,
                    categories=target_categories,
                )

                parsed_items.extend(day_items)

                self.logger.info(
                    f"День {current_date.date()}: {len(day_items)} статей "
                    f"(всего: {len(parsed_items)})"
                )

            except Exception as e:
                self.logger.error(f"Ошибка парсинга дня {current_date.date()}: {e}")

            current_date += timedelta(days=1)
            await asyncio.sleep(self.config.request_delay)

        self.logger.info(f"Архивный парсинг завершен: {len(parsed_items)} статей")
        return parsed_items

    async def _fetch_rss_feed(
        self, limit: int = 100, categories: Optional[List[str]] = None
    ) -> List[Dict[str, Any]]:
        """Получение RSS ленты с фильтрацией по категориям"""
        self.logger.debug(f"Загрузка RSS: {self.rss_url}")

        try:
            feed = feedparser.parse(self.rss_url)

            if feed.bozo:
                raise ParserError(f"Ошибка RSS: {feed.bozo_exception}")

            items = []
            for entry in feed.entries[: limit * 2]:
                try:
                    # Парсим категорию
                    entry_category = ""
                    if entry.get("tags"):
                        entry_category = safe_str(entry.tags[0].term)

                    # Фильтруем по категориям если указаны
                    if categories and entry_category:
                        if not any(
                            cat.lower() in entry_category.lower() for cat in categories
                        ):
                            continue

                    # Парсим дату публикации - (timezone-aware)
                    published_dt = None
                    if hasattr(entry, "published_parsed") and entry.published_parsed:
                        # Создаем timestamp и конвертируем в UTC datetime
                        timestamp = time.mktime(entry.published_parsed)
                        utc_dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
                        # Конвертируем в MSK (UTC+3) и убираем tzinfo для БД
                        published_dt = (utc_dt + timedelta(hours=3)).replace(tzinfo=None)
                        self.logger.debug(f"RSS дата: {published_dt} (MSK)")

                    items.append(
                        {
                            "guid": safe_str(entry.get("id", "")),
                            "title": safe_str(entry.get("title", "")),
                            "link": safe_str(entry.get("link", "")),
                            "author": safe_str(entry.get("author", "")),
                            "published": published_dt,
                            "category": entry_category,
                            "summary": safe_str(entry.get("summary", "")),
                        }
                    )

                except Exception as e:
                    self.logger.warning(f"Ошибка обработки RSS элемента: {e}")
                    continue

                if len(items) >= limit:
                    break

            return items

        except Exception as e:
            raise ParserError(f"Ошибка загрузки RSS: {e}")

    async def _parse_article_page(self, url: str) -> Optional[Dict[str, Any]]:
        """Парсинг HTML страницы статьи"""
        try:
            html = await self.fetch_url(url)
            soup = BeautifulSoup(html, "html.parser")

            # 1. Заголовок
            title = self._extract_title(soup)

            # 2. Автор
            author = self._extract_author(soup)

            # 3. Текст статьи
            content = self._extract_content(soup)

            # 4. Дата публикации
            published_time = self._extract_published_time(soup, url)

            # 5. Категория
            category = self._extract_category(soup)

            # 6. Мета-описание
            description = self._extract_description(soup)

            if not content or len(content.strip()) < 100:
                self.logger.warning(f"Статья слишком короткая: {url}")
                return None

            return {
                "url": url,
                "title": title,
                "author": author,
                "content": content,
                "published_time": published_time,
                "category": category,
                "description": description,
                "html": html,
                "text_length": len(content),
                "retrieved_at": datetime.now(),
            }

        except Exception as e:
            self.logger.error(f"Ошибка парсинга страницы {url}: {e}")
            return None

    async def _parse_archive_day(
        self,
        date: datetime,
        max_articles_per_day: int = 20,
        max_pages_per_day: int = 5,
        skip_existing_urls: Optional[Set[str]] = None,
        categories: Optional[List[str]] = None,
    ) -> List[ParsedItem]:
        """Парсинг одного дня архива с параллельной загрузкой в батчах"""
        date_str = date.strftime("%Y/%m/%d")
        archive_url = f"{self.archive_base_url}/{date_str}/"

        self.logger.info(f"Парсинг дня: {date.date()} ({archive_url})")

        # Получаем ссылки на статьи
        article_urls = await self._get_article_links_from_archive(
            archive_url=archive_url, max_pages=max_pages_per_day, categories=categories
        )

        if not article_urls:
            return []

        # Ограничиваем количество
        if len(article_urls) > max_articles_per_day:
            article_urls = article_urls[:max_articles_per_day]
            self.logger.info(f"Ограничено до {max_articles_per_day} статей")

        # Параллельная загрузка в батчах
        parsed_items = []
        semaphore = asyncio.Semaphore(self.max_concurrent_requests)

        async def parse_article_with_semaphore(url: str):
            async with semaphore:
                try:
                    result = await self._parse_article_page(url)
                    return result
                except Exception as e:
                    self.logger.error(f"Ошибка парсинга {url}: {e}")
                    return None

        batch_size = self.max_concurrent_requests
        total_urls = len(article_urls)

        for batch_start in range(0, total_urls, batch_size):
            batch_urls = article_urls[batch_start : batch_start + batch_size]
            batch_num = batch_start // batch_size + 1
            total_batches = (total_urls + batch_size - 1) // batch_size

            self.logger.debug(f"Парсим батч {batch_num}/{total_batches}: {len(batch_urls)} статей")

            # Параллельная загрузка батча
            batch_results = await asyncio.gather(
                *[parse_article_with_semaphore(url) for url in batch_urls],
                return_exceptions=False
            )

            for url, result in zip(batch_urls, batch_results):
                if not result:
                    continue

                # Создаем ParsedItem
                parsed_item = ParsedItem(
                    source_id=self.source_id,
                    source_name=self.source_name,
                    original_id=self._generate_article_id(
                        url, result["published_time"]
                    ),
                    url=result["url"],
                    title=result["title"],
                    content=result["content"],
                    published_at=result["published_time"],
                    author=result["author"],
                    metadata={
                        "category": result["category"],
                        "description": result["description"],
                        "archive_date": date.date().isoformat(),
                        "text_length": result["text_length"],
                    },
                    raw_data={"html": result["html"]},
                )

                # Пропускаем существующие
                if skip_existing_urls and url in skip_existing_urls:
                    self.logger.debug(f"Пропускаем существующую статью: {url}")
                    continue

                if self.validate_item(parsed_item):
                    parsed_items.append(parsed_item)

            self.logger.debug(f"Батч {batch_num}/{total_batches} завершен: +{len([r for r in batch_results if r])} статей")

            # Небольшая пауза между батчами (для rate limit)
            if batch_start + batch_size < total_urls:
                await asyncio.sleep(0.5)

        self.logger.info(f"День {date.date()}: распарсено {len(parsed_items)} статей")

        return parsed_items

    async def _get_article_links_from_archive(
        self,
        archive_url: str,
        max_pages: int = 10,
        categories: Optional[List[str]] = None,
    ) -> List[str]:
        """Получение ссылок на статьи из архивной страницы"""
        self.logger.debug(f"Поиск статей в архиве: {archive_url}")
        if categories:
            self.logger.info(f"Фильтр по категориям: {categories}")

        all_urls = []
        current_page = 1

        while current_page <= max_pages:
            if current_page == 1:
                page_url = archive_url
            else:
                if archive_url.endswith("/"):
                    page_url = archive_url[:-1] + f"/page/{current_page}/"
                else:
                    page_url = archive_url + f"/page/{current_page}/"

            self.logger.debug(f"Загружаем страницу {current_page}: {page_url}")

            try:
                html = await self.fetch_url(page_url)
                soup = BeautifulSoup(html, "html.parser")

                # Находим все карточки новостей
                news_cards = soup.find_all(class_="card-full-news")

                page_links = []

                for card in news_cards:
                    href = ""
                    if card.name == "a" and "href" in card.attrs:
                        href = card.get("href", "")
                    else:
                        link = card.find("a", href=True)
                        if link:
                            href = link.get("href", "")

                    if not href or not href.startswith("/"):
                        continue

                    # Фильтрация по категориям
                    if categories:
                        category = ""
                        rubric_elem = card.find("span", class_="card-full-news__rubric")
                        if rubric_elem:
                            category = rubric_elem.get_text(strip=True)
                        if not category or category not in categories:
                            continue

                    # Формируем полный URL
                    full_url = urljoin(self.base_url, href)
                    if "/news/" in full_url or "/articles/" in full_url:
                        page_links.append(full_url)

                # Запасной вариант: регулярка
                if not page_links:
                    self.logger.debug("Пробуем регулярку")
                    html_text = html
                    pattern = r'href=["\'](/news/\d{4}/\d{2}/\d{2}/[^"\']*?/)["\']'
                    matches = re.findall(pattern, html_text)
                    self.logger.debug(f"По регулярке: {len(matches)}")
                    for match in matches:
                        full_url = urljoin(self.base_url, match)
                        if full_url not in page_links:
                            page_links.append(full_url)

                # Убираем дубликаты
                unique_links = list(set(page_links))

                # Проверяем лимит страниц
                if current_page >= max_pages:
                    break

                # Если нет новых ссылок, прекращаем
                existing_urls = set(all_urls)
                new_links = [url for url in unique_links if url not in existing_urls]
                if not new_links:
                    self.logger.debug(f"Нет новых ссылок на странице {current_page}")
                    break

                all_urls.extend(new_links)
                self.logger.debug(
                    f"Страница {current_page}: +{len(new_links)} (всего: {len(all_urls)})"
                )

                current_page += 1
                await self.delay_between_requests()

            except Exception as e:
                self.logger.error(f"Ошибка страницы {page_url}: {e}")
                break

        # Финальные уникальные
        unique_urls = list(set(all_urls))
        self.logger.info(f"Найдено {len(unique_urls)} уникальных статей")

        return unique_urls

    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Извлечение заголовка"""
        selectors = [
            "h1.topic-body__title",
            "span.topic-body__title",
            "h1",
            'meta[property="og:title"]',
            "title",
        ]

        for selector in selectors:
            try:
                if selector.startswith("meta"):
                    elem = soup.find("meta", property="og:title")
                    if elem and elem.get("content"):
                        return safe_str(elem["content"])
                else:
                    elem = soup.select_one(selector)
                    if elem and elem.get_text(strip=True):
                        return safe_str(elem.get_text(strip=True))
            except:
                continue

        return "Без заголовка"

    def _extract_author(self, soup: BeautifulSoup) -> Optional[str]:
        """Извлечение автора"""
        selectors = [
            "a.topic-authors__author",
            ".topic-authors__name",
            ".author",
            'meta[name="author"]',
        ]

        for selector in selectors:
            try:
                if selector.startswith("meta"):
                    elem = soup.find("meta", name="author")
                    if elem and elem.get("content"):
                        return safe_str(elem["content"])
                else:
                    elem = soup.select_one(selector)
                    if elem:
                        text = elem.get_text(strip=True)
                        if text and len(text) > 2:
                            return safe_str(text)
            except:
                continue

        return None

    def _extract_content(self, soup: BeautifulSoup) -> str:
        """Извлечение текста статьи"""
        content_selectors = [
            'div[id^="articleBody_"]',
            ".topic-body__content-text",
            ".article-content",
            "article",
        ]

        for selector in content_selectors:
            try:
                content_div = soup.select_one(selector)
                if content_div:
                    paragraphs = content_div.find_all(["p", "div"], recursive=False)
                    if paragraphs:
                        text_parts = []
                        for p in paragraphs:
                            text = p.get_text(separator=" ", strip=True)
                            if text and len(text) > 20:
                                text_parts.append(text)

                        if text_parts:
                            return "\n\n".join(text_parts)

                    full_text = content_div.get_text(separator="\n", strip=True)
                    if len(full_text) > 100:
                        return full_text
            except:
                continue

        return ""

    def _extract_published_time(
        self, soup: BeautifulSoup, url: str
    ) -> Optional[datetime]:
        """Извлечение даты публикации с поддержкой форматов Lenta.ru"""
        # 1. Мета-теги
        meta_selectors = [
            'meta[property="article:published_time"]',
            'meta[name="pubdate"]',
            'meta[name="publish_date"]',
            'meta[property="og:article:published_time"]',
        ]

        for selector in meta_selectors:
            try:
                elem = soup.select_one(selector)
                if elem and elem.get("content"):
                    content = elem["content"]
                    dt = safe_datetime(content)
                    if dt:
                        return dt
            except Exception as e:
                self.logger.debug(f"Ошибка парсинга мета-тега {selector}: {e}")
                continue

        # 2. Элементы времени Lenta.ru
        time_selectors = [
            "a.topic-header__time",
            "span.topic-header__time",
            ".topic-header__time",
            ".article-date",
            ".published",
            "time[datetime]",
            "time",
        ]

        for selector in time_selectors:
            try:
                elem = soup.select_one(selector)
                if elem:
                    datetime_attr = elem.get("datetime")
                    if datetime_attr:
                        dt = safe_datetime(datetime_attr)
                        if dt:
                            return dt

                    text = elem.get_text(strip=True)
                    if text:
                        dt = self._parse_lenta_time_format(text)
                        if dt:
                            return dt

                        dt = safe_datetime(text)
                        if dt:
                            return dt
            except:
                continue

        # 3. Из URL (только дата)
        date_match = re.search(r"/(\d{4})/(\d{2})/(\d{2})/", url)
        if date_match:
            year, month, day = map(int, date_match.groups())
            try:
                return datetime(year, month, day)
            except:
                pass

        return None

    def _parse_lenta_time_format(self, text: str) -> Optional[datetime]:
        """
        Парсинг специфичного формата времени Lenta.ru.
        Форматы:
            "19:09, 17 января 2026"
            "19:09, 17 января 2026 г."
            "19:09, 17 янв 2026"
        """
        try:
            text = text.replace(" г.", "")

            pattern = r"(\d{1,2}):(\d{2})\s*,\s*(\d{1,2})\s+([а-яё]+)\s+(\d{4})"
            match = re.search(pattern, text, re.IGNORECASE)

            if match:
                hour, minute, day, month_str, year = match.groups()

                month_map = {
                    "января": 1, "янв": 1,
                    "февраля": 2, "фев": 2,
                    "марта": 3, "мар": 3,
                    "апреля": 4, "апр": 4,
                    "мая": 5, "май": 5,
                    "июня": 6, "июн": 6,
                    "июля": 7, "июл": 7,
                    "августа": 8, "авг": 8,
                    "сентября": 9, "сен": 9,
                    "октября": 10, "окт": 10,
                    "ноября": 11, "ноя": 11,
                    "декабря": 12, "дек": 12,
                }

                month = month_map.get(month_str.lower())
                if month:
                    return datetime(int(year), month, int(day), int(hour), int(minute))

        except Exception as e:
            self.logger.debug(f"Ошибка парсинга формата Lenta: {text} - {e}")

        return None

    def _extract_category(self, soup: BeautifulSoup) -> str:
        """Извлечение категории"""
        selectors = [
            "a.topic-header__rubric",
            ".rubric",
            ".category",
            'meta[property="article:section"]',
        ]

        for selector in selectors:
            try:
                if selector.startswith("meta"):
                    elem = soup.find("meta", property="article:section")
                    if elem and elem.get("content"):
                        return safe_str(elem["content"])
                else:
                    elem = soup.select_one(selector)
                    if elem:
                        text = elem.get_text(strip=True)
                        if text:
                            return safe_str(text)
            except:
                continue

        return ""

    def _extract_description(self, soup: BeautifulSoup) -> str:
        """Извлечение описания"""
        selectors = [
            'meta[name="description"]',
            'meta[property="og:description"]',
            ".lead",
            ".description",
        ]

        for selector in selectors:
            try:
                if selector.startswith("meta"):
                    elem = soup.select_one(selector)
                    if elem and elem.get("content"):
                        return safe_str(elem["content"])
                else:
                    elem = soup.select_one(selector)
                    if elem:
                        text = elem.get_text(strip=True)
                        if text:
                            return safe_str(text)
            except:
                continue

        return ""

    def _create_parsed_item(
        self, rss_item: Dict[str, Any], article_data: Dict[str, Any]
    ) -> ParsedItem:
        """Создание ParsedItem из RSS и HTML данных"""
        return ParsedItem(
            source_id=self.source_id,
            source_name=self.source_name,
            original_id=self._generate_article_id(
                article_data["url"], article_data["published_time"]
            ),
            url=article_data["url"],
            title=article_data["title"] or rss_item["title"],
            content=article_data["content"],
            published_at=rss_item["published"] or article_data["published_time"],
            author=article_data["author"] or rss_item["author"],
            metadata={
                "category": article_data["category"] or rss_item["category"],
                "description": article_data["description"] or rss_item["summary"],
                "text_length": article_data["text_length"],
            },
            raw_data={"rss": rss_item, "html": article_data["html"]},
        )

    def _generate_article_id(self, url: str, published_at: Optional[datetime]) -> str:
        """Генерация ID статьи"""
        import hashlib

        if published_at:
            date_part = published_at.strftime("%Y%m%d")
            base_str = f"{url}|{date_part}"
        else:
            base_str = url

        url_hash = hashlib.md5(base_str.encode()).hexdigest()[:12]
        return f"lenta_{url_hash}"