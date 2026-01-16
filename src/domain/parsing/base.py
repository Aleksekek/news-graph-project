"""
Базовые классы для парсеров и процессоров.
"""

import asyncio
from abc import ABC, abstractmethod
from datetime import datetime
from typing import Any, AsyncIterator, Dict, List, Optional

import aiohttp
from pydantic import BaseModel, HttpUrl

from src.core.exceptions import ParserError, ValidationError
from src.core.models import ParsedItem
from src.utils.data import generate_article_id, safe_datetime, safe_str, validate_url
from src.utils.logging import get_logger, log_async_execution_time
from src.utils.retry import async_retry, retry_network

logger = get_logger("parsing.base")


class ParserConfig(BaseModel):
    """Конфигурация парсера"""

    source_id: int
    source_name: str
    base_url: Optional[HttpUrl] = None
    request_delay: float = 1.0
    max_retries: int = 3
    timeout: int = 30
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    )
    headers: Dict[str, str] = {}

    class Config:
        from_attributes = True


class BaseParser(ABC):
    """
    Абстрактный базовый класс для всех парсеров.
    Все парсеры должны наследоваться от этого класса.
    """

    def __init__(self, config: ParserConfig):
        """
        Args:
            config: Конфигурация парсера
        """
        self.config = config
        self.session: Optional[aiohttp.ClientSession] = None
        self.logger = get_logger(f"parser.{config.source_name}")

    @property
    def source_id(self) -> int:
        """ID источника"""
        return self.config.source_id

    @property
    def source_name(self) -> str:
        """Имя источника"""
        return self.config.source_name

    @abstractmethod
    async def parse_recent(self, limit: int = 100) -> List[ParsedItem]:
        """
        Парсинг последних элементов.

        Args:
            limit: Максимальное количество элементов

        Returns:
            Список распарсенных элементов
        """
        pass

    async def parse_period(
        self, start_date: datetime, end_date: datetime, **kwargs
    ) -> List[ParsedItem]:
        """
        Парсинг за период.

        Args:
            start_date: Начальная дата
            end_date: Конечная дата
            **kwargs: Дополнительные параметры

        Returns:
            Список распарсенных элементов

        Raises:
            NotImplementedError: Если парсер не поддерживает архив
        """
        raise NotImplementedError(
            f"Парсер {self.source_name} не поддерживает архивный парсинг"
        )

    async def __aenter__(self):
        """Контекстный менеджер для асинхронного использования"""
        await self.setup_session()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Закрытие сессии при выходе из контекста"""
        await self.close_session()

    async def setup_session(self):
        """Создание HTTP сессии"""
        if self.session is None or self.session.closed:
            timeout = aiohttp.ClientTimeout(total=self.config.timeout)

            self.session = aiohttp.ClientSession(
                timeout=timeout,
                headers={"User-Agent": self.config.user_agent, **self.config.headers},
            )
            self.logger.debug(f"Создана HTTP сессия для {self.source_name}")

    async def close_session(self):
        """Закрытие HTTP сессии"""
        if self.session and not self.session.closed:
            await self.session.close()
            self.logger.debug(f"Закрыта HTTP сессия для {self.source_name}")

    @async_retry(
        exceptions=(aiohttp.ClientError, asyncio.TimeoutError),
        max_attempts=3,
        delay=2.0,
    )
    async def fetch_url(self, url: str, **kwargs) -> str:
        """
        Безопасная загрузка URL с повторными попытками.

        Args:
            url: URL для загрузки
            **kwargs: Дополнительные параметры для aiohttp

        Returns:
            Текст ответа

        Raises:
            ParserError: При ошибке загрузки
        """
        if not validate_url(url):
            raise ValidationError(f"Некорректный URL: {url}")

        await self.setup_session()

        try:
            self.logger.debug(f"Загрузка URL: {url}")

            async with self.session.get(url, **kwargs) as response:
                response.raise_for_status()
                text = await response.text()

                self.logger.debug(f"Успешно загружено: {url} ({len(text)} байт)")
                return text

        except aiohttp.ClientResponseError as e:
            if e.status == 404:
                self.logger.warning(f"Страница не найдена: {url}")
            elif e.status == 429:
                self.logger.warning(f"Rate limiting для {url}. Пауза 30 сек.")
                await asyncio.sleep(30)
                raise
            raise ParserError(f"Ошибка HTTP {e.status} для {url}: {e}")
        except Exception as e:
            raise ParserError(f"Ошибка загрузки {url}: {e}")

    async def fetch_json(self, url: str, **kwargs) -> Dict[str, Any]:
        """
        Загрузка JSON данных.

        Args:
            url: URL для загрузки
            **kwargs: Дополнительные параметры

        Returns:
            JSON данные как словарь
        """
        text = await self.fetch_url(url, **kwargs)

        try:
            import json

            return json.loads(text)
        except json.JSONDecodeError as e:
            raise ParserError(f"Ошибка парсинга JSON из {url}: {e}")

    def validate_item(self, item: ParsedItem) -> bool:
        """
        Валидация распарсенного элемента.

        Args:
            item: Элемент для валидации

        Returns:
            True если элемент валиден
        """
        # Проверяем обязательные поля
        if not item.url or not validate_url(item.url):
            self.logger.warning(f"Некорректный URL в элементе: {item.url}")
            return False

        if not item.title or len(item.title.strip()) < 3:
            self.logger.warning(f"Слишком короткий заголовок: {item.title}")
            return False

        if not item.content or len(item.content.strip()) < 50:
            self.logger.warning(
                f"Слишком короткий контент: {len(item.content)} символов"
            )
            return False

        return True

    def delay_between_requests(self):
        """Задержка между запросами для избежания rate limiting"""
        return asyncio.sleep(self.config.request_delay)


class BaseProcessor(ABC):
    """
    Базовый класс для процессоров/адаптеров.
    Преобразует сырые данные в унифицированные модели.
    """

    def __init__(self, source_id: int, source_name: str):
        """
        Args:
            source_id: ID источника
            source_name: Имя источника
        """
        self.source_id = source_id
        self.source_name = source_name
        self.logger = get_logger(f"processor.{source_name}")

    @abstractmethod
    def to_parsed_item(self, raw_data: Dict[str, Any]) -> ParsedItem:
        """
        Преобразование сырых данных в ParsedItem.

        Args:
            raw_data: Сырые данные из парсера

        Returns:
            Унифицированный ParsedItem
        """
        pass

    def batch_to_parsed_items(
        self, raw_data_list: List[Dict[str, Any]]
    ) -> List[ParsedItem]:
        """
        Пакетное преобразование сырых данных.

        Args:
            raw_data_list: Список сырых данных

        Returns:
            Список ParsedItem
        """
        result = []
        for raw_data in raw_data_list:
            try:
                item = self.to_parsed_item(raw_data)
                if item:
                    result.append(item)
            except Exception as e:
                self.logger.error(f"Ошибка преобразования данных: {e}")
                continue

        return result

    def generate_item_id(self, item: ParsedItem) -> str:
        """
        Генерация уникального ID для элемента.
        Может быть переопределен в дочерних классах.

        Args:
            item: Элемент

        Returns:
            Уникальный ID
        """
        return generate_article_id(
            url=item.url,
            published_at=item.published_at,
            title=item.title,
            source_prefix=self.source_name,
        )


class AsyncParserIterator:
    """
    Итератор для потокового парсинга с пагинацией.
    """

    def __init__(self, parser: BaseParser, batch_size: int = 20):
        """
        Args:
            parser: Парсер
            batch_size: Размер батча
        """
        self.parser = parser
        self.batch_size = batch_size
        self.current_page = 0
        self.has_more = True

    def __aiter__(self):
        return self

    async def __anext__(self) -> List[ParsedItem]:
        if not self.has_more:
            raise StopAsyncIteration

        try:
            items = await self.parser.parse_recent(
                limit=self.batch_size, offset=self.current_page * self.batch_size
            )

            if not items:
                self.has_more = False
                raise StopAsyncIteration

            self.current_page += 1
            return items

        except Exception as e:
            self.parser.logger.error(f"Ошибка в итераторе: {e}")
            self.has_more = False
            raise StopAsyncIteration
