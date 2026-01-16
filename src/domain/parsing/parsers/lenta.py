"""
Парсер Lenta.ru
Объединяет LentaParser и LentaArchiveParser
"""

import asyncio
import re
import time
from datetime import datetime, timedelta
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
    """

    def __init__(self, config: ParserConfig):
        super().__init__(config)

        # Настройки специфичные для Lenta
        self.base_url = "https://lenta.ru"
        self.rss_url = f"{self.base_url}/rss"
        self.archive_base_url = f"{self.base_url}/news"

        # Кэширование
        self._last_rss_fetch = None
        self._rss_cache = []

    @log_async_execution_time()
    async def parse_recent(self, limit: int = 100) -> List[ParsedItem]:
        """
        Парсинг последних новостей через RSS.

        Args:
            limit: Максимальное количество новостей

        Returns:
            Список ParsedItem
        """
        self.logger.info(f"Парсинг последних {limit} новостей Lenta.ru")

        try:
            # Получаем RSS
            rss_items = await self._fetch_rss_feed(limit)

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

    async def parse_period(
        self,
        start_date: datetime,
        end_date: datetime,
        max_articles_per_day: int = 20,
        max_pages_per_day: int = 5,
        skip_existing_urls: Optional[Set[str]] = None,
        categories: Optional[List[str]] = None,
    ) -> List[ParsedItem]:
        """
        Архивный парсинг Lenta.ru за период.

        Args:
            start_date: Начальная дата
            end_date: Конечная дата
            max_articles_per_day: Максимум статей в день
            max_pages_per_day: Максимум страниц в день
            skip_existing_urls: URL для пропуска (дедупликация)
            categories: Фильтр по категориям

        Returns:
            Список ParsedItem
        """
        self.logger.info(
            f"Архивный парсинг Lenta.ru: {start_date.date()} - {end_date.date()}"
        )

        parsed_items = []
        current_date = start_date

        while current_date <= end_date:
            try:
                day_items = await self._parse_archive_day(
                    date=current_date,
                    max_articles_per_day=max_articles_per_day,
                    max_pages_per_day=max_pages_per_day,
                    skip_existing_urls=skip_existing_urls,
                    categories=categories,
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

    async def _fetch_rss_feed(self, limit: int = 100) -> List[Dict[str, Any]]:
        """Получение RSS ленты"""
        self.logger.debug(f"Загрузка RSS: {self.rss_url}")

        try:
            # Используем feedparser (синхронный, но быстрый)
            # В будущем можно заменить на aiohttp + парсинг XML
            feed = feedparser.parse(self.rss_url)

            if feed.bozo:
                raise ParserError(f"Ошибка RSS: {feed.bozo_exception}")

            items = []
            for entry in feed.entries[:limit]:
                try:
                    # Парсим дату публикации
                    published_dt = None
                    if hasattr(entry, "published_parsed") and entry.published_parsed:
                        published_dt = datetime.fromtimestamp(
                            time.mktime(entry.published_parsed)
                        )

                    items.append(
                        {
                            "guid": safe_str(entry.get("id", "")),
                            "title": safe_str(entry.get("title", "")),
                            "link": safe_str(entry.get("link", "")),
                            "author": safe_str(entry.get("author", "")),
                            "published": published_dt,
                            "category": safe_str(
                                entry.tags[0].term if entry.get("tags") else ""
                            ),
                            "summary": safe_str(entry.get("summary", "")),
                        }
                    )

                except Exception as e:
                    self.logger.warning(f"Ошибка обработки RSS элемента: {e}")
                    continue

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
        """Парсинг одного дня архива"""
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

        # Парсим статьи
        parsed_items = []
        for i, url in enumerate(article_urls):
            # Пропускаем существующие
            if skip_existing_urls and url in skip_existing_urls:
                self.logger.debug(f"Пропускаем существующую статью: {url}")
                continue

            try:
                self.logger.debug(f"Парсинг статьи {i+1}/{len(article_urls)}: {url}")

                article_data = await self._parse_article_page(url)

                if article_data:
                    # Создаем ParsedItem
                    parsed_item = ParsedItem(
                        source_id=self.source_id,
                        source_name=self.source_name,
                        original_id=self._generate_article_id(
                            url, article_data["published_time"]
                        ),
                        url=url,
                        title=article_data["title"],
                        content=article_data["content"],
                        published_at=article_data["published_time"],
                        author=article_data["author"],
                        metadata={
                            "category": article_data["category"],
                            "description": article_data["description"],
                            "archive_date": date.date().isoformat(),
                            "text_length": article_data["text_length"],
                        },
                        raw_data={"html": article_data["html"]},
                    )

                    if self.validate_item(parsed_item):
                        parsed_items.append(parsed_item)

                    # Задержка между запросами
                    if i < len(article_urls) - 1:
                        await self.delay_between_requests()

            except Exception as e:
                self.logger.error(f"Ошибка парсинга статьи {url}: {e}")
                continue

        return parsed_items

    async def _get_article_links_from_archive(
        self,
        archive_url: str,
        max_pages: int = 5,
        categories: Optional[List[str]] = None,
    ) -> List[str]:
        """Получение ссылок на статьи из архивной страницы"""
        self.logger.debug(f"Поиск статей в архиве: {archive_url}")

        all_urls = []
        current_page = 1

        while current_page <= max_pages:
            # Формируем URL страницы
            if current_page == 1:
                page_url = archive_url
            else:
                page_url = f"{archive_url.rstrip('/')}/page/{current_page}/"

            try:
                html = await self.fetch_url(page_url)
                soup = BeautifulSoup(html, "html.parser")

                # Ищем ссылки на статьи
                article_links = []

                # Паттерн для ссылок Lenta.ru
                link_patterns = [
                    r"/news/\d{4}/\d{2}/\d{2}/[^/]+/$",
                    r"/articles/\d{4}/\d{2}/\d{2}/[^/]+/$",
                ]

                for a_tag in soup.find_all("a", href=True):
                    href = a_tag["href"]

                    # Проверяем паттерны
                    for pattern in link_patterns:
                        if re.match(pattern, href):
                            full_url = urljoin(self.base_url, href)
                            if full_url not in article_links:
                                article_links.append(full_url)

                # Если нет ссылок, прекращаем пагинацию
                if not article_links:
                    break

                # Фильтруем по категориям если указаны
                if categories:
                    filtered_links = []
                    for url in article_links:
                        # Парсим категорию из URL или страницы
                        # (упрощенная логика, можно улучшить)
                        category_match = re.search(
                            r"/news/\d{4}/\d{2}/\d{2}/([^/]+)/", url
                        )
                        if category_match:
                            url_category = category_match.group(1)
                            if any(
                                cat.lower() in url_category.lower()
                                for cat in categories
                            ):
                                filtered_links.append(url)

                    article_links = filtered_links

                all_urls.extend(article_links)

                self.logger.debug(
                    f"Страница {current_page}: найдено {len(article_links)} ссылок "
                    f"(всего: {len(all_urls)})"
                )

                # Проверяем наличие следующей страницы
                next_page_link = soup.find(
                    "a", class_=lambda x: x and "next" in x.lower()
                )
                if not next_page_link:
                    break

                current_page += 1
                await self.delay_between_requests()

            except Exception as e:
                self.logger.error(f"Ошибка загрузки архивной страницы {page_url}: {e}")
                break

        # Убираем дубликаты
        unique_urls = list(set(all_urls))
        self.logger.info(f"Найдено {len(unique_urls)} уникальных статей")

        return unique_urls

    def _extract_title(self, soup: BeautifulSoup) -> str:
        """Извлечение заголовка"""
        # Пробуем разные селекторы
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
        # Основные селекторы для контента Lenta.ru
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
                    # Извлекаем все параграфы
                    paragraphs = content_div.find_all(["p", "div"], recursive=False)
                    if paragraphs:
                        text_parts = []
                        for p in paragraphs:
                            text = p.get_text(separator=" ", strip=True)
                            if text and len(text) > 20:  # Фильтруем короткие блоки
                                text_parts.append(text)

                        if text_parts:
                            return "\n\n".join(text_parts)

                    # Если не нашли параграфы, берем весь текст
                    full_text = content_div.get_text(separator="\n", strip=True)
                    if len(full_text) > 100:
                        return full_text
            except:
                continue

        return ""

    def _extract_published_time(
        self, soup: BeautifulSoup, url: str
    ) -> Optional[datetime]:
        """Извлечение даты публикации"""
        # Из мета-тегов
        meta_selectors = [
            'meta[property="article:published_time"]',
            'meta[name="pubdate"]',
            'meta[name="publish_date"]',
        ]

        for selector in meta_selectors:
            try:
                elem = soup.select_one(selector)
                if elem and elem.get("content"):
                    dt = safe_datetime(elem["content"])
                    if dt:
                        return dt
            except:
                continue

        # Из текста страницы
        time_selectors = [
            "a.topic-header__time",
            ".article-date",
            ".published",
            "time",
        ]

        for selector in time_selectors:
            try:
                elem = soup.select_one(selector)
                if elem:
                    text = elem.get_text(strip=True)
                    dt = safe_datetime(text)
                    if dt:
                        return dt
            except:
                continue

        # Из URL (формат: /news/2024/01/15/slug/)
        date_match = re.search(r"/(\d{4})/(\d{2})/(\d{2})/", url)
        if date_match:
            year, month, day = map(int, date_match.groups())
            try:
                return datetime(year, month, day)
            except:
                pass

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
            published_at=article_data["published_time"] or rss_item["published"],
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
