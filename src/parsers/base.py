"""
Базовый класс для всех парсеров.
Определяет унифицированный интерфейс.
"""

import asyncio
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any

import aiohttp

from src.core.exceptions import ParserError
from src.core.models import ParsedItem, ParserConfig
from src.utils.logging import get_logger
from src.utils.retry import async_retry


class ParseResult:
    """
    Результат парсинга.
    Содержит элементы и информацию для пагинации.
    """

    def __init__(
        self,
        items: list[ParsedItem],
        cursor: str | None = None,
        has_more: bool = False,
    ):
        self.items = items
        self.cursor = cursor
        self.has_more = has_more

    def __len__(self) -> int:
        return len(self.items)

    def __bool__(self) -> bool:
        return len(self.items) > 0


class BaseParser(ABC):
    """
    Абстрактный базовый класс для всех парсеров.
    Унифицированный интерфейс с поддержкой фильтров через kwargs.
    """

    def __init__(self, config: ParserConfig):
        """
        Args:
            config: Конфигурация парсера
        """
        self.config = config
        self._session: aiohttp.ClientSession | None = None
        self.logger = get_logger(f"parsers.{config.source_name}")

    @property
    def source_id(self) -> int:
        return self.config.source_id

    @property
    def source_name(self) -> str:
        return self.config.source_name

    @abstractmethod
    async def parse(self, limit: int = 100, **filters) -> ParseResult:
        """
        Парсинг свежих постов/статей.

        Args:
            limit: Максимальное количество элементов
            **filters: Специфичные для парсера фильтры:
                - Для Lenta: categories=["Политика", "Экономика"]
                - Для TInvest: tickers=["SBER", "VTBR"], min_reactions=10

        Returns:
            ParseResult с элементами и курсором для пагинации
        """
        pass

    @abstractmethod
    async def parse_period(
        self, start_date: datetime, end_date: datetime, limit: int = 100, **filters
    ) -> ParseResult:
        """
        Архивный парсинг за период.

        Args:
            start_date: Начало периода (MSK naive)
            end_date: Конец периода (MSK naive)
            limit: Максимальное количество элементов
            **filters: Специфичные для парсера фильтры

        Returns:
            ParseResult с элементами
        """
        pass

    @abstractmethod
    def to_parsed_item(self, raw_data: dict[str, Any]) -> ParsedItem:
        """
        Конвертация сырых данных в ParsedItem.
        Каждый парсер реализует свою логику.

        Args:
            raw_data: Сырые данные из источника

        Returns:
            Унифицированный ParsedItem
        """
        pass

    async def __aenter__(self):
        """Контекстный менеджер для асинхронного использования."""
        await self._setup_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Закрытие сессии при выходе из контекста."""
        await self._close_session()

    async def _setup_session(self):
        """Создание HTTP сессии."""
        if self._session is None or self._session.closed:
            timeout = aiohttp.ClientTimeout(total=self.config.timeout)
            headers = {"User-Agent": self.config.user_agent}

            self._session = aiohttp.ClientSession(
                timeout=timeout,
                headers=headers,
            )
            self.logger.debug(f"HTTP сессия создана для {self.source_name}")

    async def _close_session(self):
        """Закрытие HTTP сессии."""
        if self._session and not self._session.closed:
            await self._session.close()
            self.logger.debug(f"HTTP сессия закрыта для {self.source_name}")

    @async_retry(
        exceptions=(aiohttp.ClientError, asyncio.TimeoutError),
        max_attempts=3,
        delay=2.0,
    )
    async def _fetch_url(self, url: str, **kwargs) -> str:
        """
        Безопасная загрузка URL с повторными попытками.

        Args:
            url: URL для загрузки
            **kwargs: Дополнительные параметры для aiohttp

        Returns:
            Текст ответа
        """
        await self._setup_session()

        try:
            self.logger.debug(f"Загрузка: {url}")

            async with self._session.get(url, **kwargs) as response:
                response.raise_for_status()
                text = await response.text()

                self.logger.debug(f"Загружено: {url} ({len(text)} байт)")
                return text

        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                self.logger.warning(f"Страница не найдена: {url}")
            elif e.status == 429:
                self.logger.warning(f"Rate limit для {url}, пауза 30 сек")
                await asyncio.sleep(30)
            raise ParserError(f"HTTP {e.status} для {url}: {e}") from e
        except Exception as e:
            raise ParserError(f"Ошибка загрузки {url}: {e}") from e

    async def _fetch_json(self, url: str, **kwargs) -> dict[str, Any]:
        """Загрузка JSON данных."""
        text = await self._fetch_url(url, **kwargs)

        try:
            import json

            return json.loads(text)
        except json.JSONDecodeError as e:
            raise ParserError(f"Ошибка парсинга JSON из {url}: {e}") from e

    async def _delay(self):
        """Задержка между запросами для избежания rate limiting."""
        if self.config.request_delay > 0:
            await asyncio.sleep(self.config.request_delay)

    def _validate_url(self, url: str) -> bool:
        """Простая валидация URL."""
        if not url or not isinstance(url, str):
            return False
        return url.startswith(("http://", "https://", "//"))

    def _validate_item(self, item: ParsedItem) -> bool:
        """
        Базовая валидация ParsedItem.
        Может быть переопределена в наследниках.
        """
        if not self._validate_url(item.url):
            self.logger.warning(f"Некорректный URL: {item.url}")
            return False

        if not item.title or len(item.title.strip()) < 3:
            self.logger.warning(f"Слишком короткий заголовок: {item.title}")
            return False

        # Минимальная длина контента зависит от источника
        min_length = 10 if self.source_name == "tinvest" else 50
        if not item.content or len(item.content.strip()) < min_length:
            self.logger.warning(f"Слишком короткий контент: {len(item.content)}")
            return False

        return True
