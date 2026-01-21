"""
Парсер Тинькофф Пульса с унифицированным интерфейсом.
Полностью асинхронная реализация без pandas, с расширенным сбором данных.
"""

import asyncio
import hashlib
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Set

import aiohttp
from tpulse import TinkoffPulse

from src.core.exceptions import ParserError
from src.domain.parsing.base import BaseParser, ParsedItem, ParserConfig
from src.utils.data import safe_datetime, safe_int, safe_list, safe_str
from src.utils.logging import log_async_execution_time
from src.utils.retry import async_retry


class TInvestParser(BaseParser):
    """
    Парсер Тинькофф Пульса с полностью асинхронной реализацией.
    Обертка над библиотекой tpulse с расширенным сбором данных и поддержкой курсоров.
    """

    def __init__(self, config: ParserConfig):
        super().__init__(config)

        # Синхронный парсер tpulse (для async обертки)
        self.tp = TinkoffPulse()

        # Настройки
        self.tickers = getattr(config, "tickers", ["SBER", "VTBR", "MOEX"])

        # Настройки пагинации и буферов
        self.batch_size = 50  # Размер батча для запросов (можно адаптировать)
        self.max_batches_per_ticker = 100  # Ограничение для rate limiting

    @log_async_execution_time()
    async def parse(
        self,
        limit: int = 100,
        tickers: Optional[List[str]] = None,
        **kwargs,  # title_filters, author_filters, has_images etc.
    ) -> List[ParsedItem]:
        """
        Унифицированный метод парсинга свежих постов Тинькофф Пульса.

        Args:
            limit: Максимальное количество постов в сумме
            tickers: Список тикеров (если None, берем из конфига)
            **kwargs: Специфичные фильтры (например, min_reactions=10)

        Returns:
            Список ParsedItem
        """
        target_tickers = tickers or self.tickers

        self.logger.info(
            f"Парсинг свежих постов TInvest для тикеров {target_tickers}, лимит: {limit}"
        )

        all_items = []
        seen_ids = set()

        # Изменение: всегда обрабатываем все тикеры, дедуплицируем глобально в конце
        for ticker_idx, ticker in enumerate(target_tickers):
            try:
                self.logger.info(
                    f"Обработка тикера {ticker} ({ticker_idx + 1}/{len(target_tickers)})"
                )

                ticker_items = await self._fetch_fresh_posts(
                    ticker,
                    limit_per_ticker=limit,
                    filters=kwargs,  # Дать шанс набрать больше kandидатов
                )

                self.logger.debug(
                    f"Для тикера {ticker} получено {len(ticker_items)} кандидат-постов"
                )

                # Дедупликация по original_id (локальная для тикера, но seen_ids глобальный)
                new_unique = 0
                for item in ticker_items:
                    if item.original_id not in seen_ids:
                        all_items.append(item)
                        seen_ids.add(item.original_id)
                        new_unique += 1
                    else:
                        self.logger.debug(f"Дубликат поста {item.original_id} пропущен")

                self.logger.info(
                    f"Новых уникальных постов для {ticker}: {new_unique} (всего уникальных: {len(all_items)})"
                )

            except Exception as e:
                self.logger.error(f"Ошибка парсинга тикера {ticker}: {e}")
                continue

        # Сортировка по дате публикации (новые первые)
        all_items.sort(key=lambda x: x.published_at or datetime.min, reverse=True)

        self.logger.info(
            f"Всего собрано {len(all_items)} уникальных постов из {len(target_tickers)} тикеров"
        )

        return all_items

    @log_async_execution_time()
    async def parse_period(
        self,
        start_date: datetime,
        end_date: datetime,
        limit: int = 100,
        start_cursor: Optional[str] = None,  # Поддержка курсора
        tickers: Optional[List[str]] = None,
        **kwargs,
    ) -> List[ParsedItem]:
        """
        Архивный парсинг за период с поддержкой курсора для продолжаемого парсинга.

        Args:
            start_date: Начальная дата
            end_date: Конечная дата
            limit: Общий лимит
            start_cursor: Курсор для продолжения (если None, начинаем заново)
            tickers: Фильтр тикеров
            **kwargs: Дополнительные фильтры

        Returns:
            Список ParsedItem с курсором в metadata последней записи
        """
        target_tickers = tickers or self.tickers

        self.logger.info(
            f"Архивный парсинг TInvest: {start_date.date()} - {end_date.date()} для {target_tickers}"
        )

        all_items = []
        current_cursor = start_cursor

        for ticker in target_tickers:
            ticker_items, final_cursor = await self._fetch_historical_posts(
                ticker, start_date, end_date, limit, current_cursor, kwargs
            )
            all_items.extend(ticker_items)
            if final_cursor and not start_cursor:
                # Сохраняем курсор для продолжения в следующей сессии
                pass  # Будет в metadata

        # Добавляем курсор в последнюю запись для чекпоинта
        if all_items:
            all_items[-1].metadata["cursor"] = final_cursor

        return all_items

    async def _fetch_fresh_posts(
        self, ticker: str, limit_per_ticker: int, filters: Dict[str, Any]
    ) -> List[ParsedItem]:
        """Получение свежих постов для тикера с фильтрами"""
        items = []
        cursor = None
        batches_loaded = 0

        while (
            len(items) < limit_per_ticker
            and batches_loaded < self.max_batches_per_ticker
        ):
            batch_data = await self._make_async_request(ticker, cursor=cursor)

            if not batch_data or not batch_data.get("items"):
                break

            batch_items = self._process_batch(batch_data["items"], ticker, filters)
            items.extend(batch_items)

            # Ограничиваем по лимиту
            if len(items) > limit_per_ticker:
                items = items[:limit_per_ticker]

            cursor = batch_data.get("nextCursor")
            if not cursor or not batch_data.get("hasNext", False):
                break

            batches_loaded += 1
            # Задержка между батчами
            if batches_loaded > 0 and self.config.request_delay > 0:
                await asyncio.sleep(self.config.request_delay)

        return items

    async def _fetch_historical_posts(
        self,
        ticker: str,
        start_date: datetime,
        end_date: datetime,
        limit: int,
        start_cursor: str,
        filters: Dict[str, Any],
    ) -> tuple[List[ParsedItem], Optional[str]]:
        """Исторический парсинг с фильтром по дате и курсором"""
        self.logger.debug(
            f"Начало исторического парсинга тикера {ticker}: {start_date} - {end_date}"
        )
        self.logger.debug(f"Курсоры: стартовый {start_cursor}, лимит: {limit}")

        items = []
        cursor = start_cursor
        final_cursor = None
        batches_loaded = 0

        # Преобразуем даты для сравнения (наивные datetime), включая конец дня
        start_naive = start_date.replace(tzinfo=None)
        # Конец дня: добавляем день, ставим начало, минус микросекунда = 23:59:59.999
        end_naive = (end_date.replace(tzinfo=None) + timedelta(days=1)).replace(
            hour=0, minute=0, second=0, microsecond=0
        ) - timedelta(microseconds=1)

        self.logger.debug(
            f"Диапазон дат включительно: {start_naive} ({start_date.date()}) - {end_naive} ({end_date.date()}, конец дня)"
        )

        found_older_than_start = (
            False  # Флаг для остановки при выходе за начало диапазона
        )

        while len(items) < limit and not found_older_than_start:
            if found_older_than_start:
                self.logger.debug(
                    "Остановка парсинга: найдены посты старше начала диапазона"
                )
                break

            batch_version = batches_loaded + 1
            self.logger.debug(
                f"Загрузка батча {batch_version} для {ticker}, курсор: {cursor}"
            )

            batch_data = await self._make_async_request(ticker, cursor=cursor)

            if not batch_data or not batch_data.get("items"):
                self.logger.debug(
                    f"Пустой батч {batch_version} или конец данных для {ticker}"
                )
                break

            final_cursor = batch_data.get("nextCursor")
            posts_in_batch = len(batch_data["items"])
            self.logger.debug(
                f"Батч {batch_version}: получено {posts_in_batch} постов, следующий курсор: {final_cursor}"
            )

            posts_processed = 0
            posts_added = 0
            posts_filtered = 0
            seen_in_range = False

            for post in batch_data["items"]:
                if len(items) >= limit:
                    self.logger.debug(f"Достигнут глобальный лимит {limit} постов")
                    break

                post_date = self._extract_post_date(post)
                if not post_date:
                    self.logger.debug("Не удалось извлечь дату поста (пропущен)")
                    continue

                posts_processed += 1

                if post_date < start_naive:
                    self.logger.debug(
                        f"Пост от {post_date.date()} старше начала диапазона, оставшиеся в батче и все следующие пропущены"
                    )
                    found_older_than_start = True
                    break  # Прекращаем этот батч
                elif post_date > end_naive:
                    self.logger.debug(
                        f"Пост от {post_date.date()} слишком свежий (пропущен)"
                    )
                    pass  # Просто пропускаем
                else:
                    # Пост в диапазоне
                    seen_in_range = True
                    parsed_item = self._post_to_parsed_item(post, ticker)
                    if parsed_item and self._apply_filters(parsed_item, filters):
                        items.append(parsed_item)
                        posts_added += 1
                    else:
                        posts_filtered += 1
                        self.logger.debug(f"Фильтр отклонил пост от {post_date.date()}")

            self.logger.debug(
                f"Батч {batch_version}: обработано {posts_processed}, добавлено {posts_added}, фильтровано {posts_filtered}"
            )

            # Если в этом батче было слишком многие старые посты, останавливаемся совсем
            if found_older_than_start:
                break

            if not batch_data.get("hasNext", False):
                self.logger.debug(f"Конец данных для {ticker} (hasNext=False)")
                break

            # Условная остановка по батчам без результатов (тверже)
            if (
                not seen_in_range and batches_loaded > 10
            ):  # Увеличил с 5 до 10, чтобы не срабатывало преждевременно
                self.logger.warning(
                    f"Не найдено постов в диапазоне в последних {batches_loaded-5}-{batches_loaded} батчах для {ticker}, возможно данные исчерпаны или период слишком узкий"
                )
                break

            cursor = final_cursor
            batches_loaded += 1

            # Ограничение на количество батчей - повысь для больших наборов данных
            self.max_batches_per_ticker = (
                999  # Увеличил для тестов с историей (ранее 100)
            )
            if batches_loaded >= self.max_batches_per_ticker:
                self.logger.warning(
                    f"Превышен лимит батчей ({self.max_batches_per_ticker}) для {ticker}, остановка"
                )
                break

            if self.config.request_delay > 0:
                await asyncio.sleep(self.config.request_delay)

        self.logger.info(
            f"Исторический парсинг {ticker} завершен: {len(items)} постов, {batches_loaded} батчей, финальный курсор: {final_cursor}, остановка по старым постам: {found_older_than_start}"
        )

        return items, final_cursor

    async def _make_async_request(
        self, ticker: str, cursor: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Асинхронный запрос к tpulse через event loop executor"""
        loop = asyncio.get_event_loop()
        try:
            return await loop.run_in_executor(None, self._sync_request, ticker, cursor)
        except Exception as e:
            self.logger.error(f"Ошибка запроса для {ticker}: {e}")
            return None

    def _sync_request(
        self, ticker: str, cursor: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Синхронный запрос (обертка для tpulse)"""
        try:
            return self.tp.get_posts_by_ticker(ticker, cursor=cursor)
        except Exception as e:
            raise

    def _process_batch(
        self, posts: List[Dict[str, Any]], ticker: str, filters: Dict[str, Any]
    ) -> List[ParsedItem]:
        """Обработка батча постов с фильтрами"""
        items = []
        for post in posts:
            parsed_item = self._post_to_parsed_item(post, ticker)
            if parsed_item and self._apply_filters(parsed_item, filters):
                items.append(parsed_item)
        return items

    def _post_to_parsed_item(
        self, post: Dict[str, Any], ticker: str
    ) -> Optional[ParsedItem]:
        """Конвертация поста в ParsedItem"""
        try:
            # Извлекаем данные из поста
            post_data = self._extract_post_data(post)

            # Проверяем обязательные поля
            if not post_data["text"].strip():
                return None

            published_at = self._extract_post_date(post)
            original_id = self._generate_post_id(post)
            url = self._generate_post_url(post)
            title = self._generate_title(post_data["text"], post_data["author"])

            # Метаданные
            metadata = {
                "target_ticker": ticker,
                "mentioned_tickers": post_data["instruments"],
                "mentioned_tickers_count": len(post_data["instruments"]),
                "total_reactions": post_data["reactions"]["totalCount"],
                "comments_count": post_data["commentsCount"],
                "has_images": len(post_data["images"]) > 0,
                "images_count": len(post_data["images"]),
                "hashtags_count": len(post_data["hashtags"]),
                "profiles_count": len(post_data["profiles"]),
                "strategies_count": len(post_data["strategies"]),
                "reactions": post_data["reactions"]["counters"],
                "instruments": post_data["instruments"],  # Полные данные инструментов
                "hashtags": post_data["hashtags"],
                "profiles": post_data["profiles"],
                "strategies": post_data["strategies"],
                "owner": post_data["owner"],
                "service_tags": post_data["service_tags"],
                "is_editable": post_data["is_editable"],
                "base_tariff_category": post_data["base_tariff_category"],
                "cursor": post.get("cursor"),  # Если есть в данных
            }

            # Создаем ParsedItem
            return ParsedItem(
                source_id=self.source_id,
                source_name=self.source_name,
                original_id=original_id,
                url=url,
                title=title,
                content=post_data["text"],
                published_at=published_at,
                author=post_data["author"],
                metadata=metadata,
                raw_data={"original_post": post},  # Полный сырой пост
            )

        except Exception as e:
            self.logger.error(f"Ошибка конвертации поста: {e}")
            return None

    def _extract_post_data(self, post: Dict[str, Any]) -> Dict[str, Any]:
        """Извлечение всех данных из поста"""
        content = post.get("content", {})

        return {
            "id": post.get("id", ""),
            "text": content.get("text", ""),
            "author": self._extract_author(post.get("owner", {})),
            "owner": post.get("owner", {}),
            "instruments": content.get(
                "instruments", []
            ),  # Полный список с ценами/именами
            "images": content.get("images", []),
            "hashtags": content.get("hashtags", []),
            "profiles": content.get("profiles", []),
            "strategies": content.get("strategies", []),
            "reactions": {
                "totalCount": safe_int(post.get("reactions", {}).get("totalCount", 0)),
                "counters": post.get("reactions", {}).get("counters", []),
            },
            "commentsCount": safe_int(post.get("commentsCount", 0)),
            "service_tags": post.get("serviceTags", []),
            "is_editable": post.get("isEditable", False),
            "base_tariff_category": post.get("baseTariffCategory", ""),
            "is_bookmarked": post.get("isBookmarked", False),
            "status": post.get("status", ""),
        }

    def _extract_author(self, owner: Dict[str, Any]) -> str:
        """Извлечение автора"""
        return safe_str(owner.get("nickname", ""))

    def _extract_post_date(self, post: Dict[str, Any]) -> Optional[datetime]:
        """Извлечение и конвертация даты в naive datetime (MSK)"""
        try:
            utc_str = post.get("inserted", "")
            if not utc_str:
                return None
            utc_dt = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))  # aware UTC
            msk_dt = utc_dt + timedelta(hours=3)  # aware MSK
            return msk_dt.replace(
                tzinfo=None
            )  # делаем naive для совместимости с start/end из use_case
        except Exception:
            return None

    def _generate_post_id(self, post: Dict[str, Any]) -> str:
        """Генерация уникального ID поста"""
        post_id = post.get("id", "")
        inserted = post.get("inserted", "")
        content = post.get("content", {}).get("text", "")[:100]
        key_data = f"{post_id}|{inserted}|{content}"
        hash_obj = hashlib.sha256(key_data.encode()).hexdigest()
        return f"tinvest_{hash_obj[:16]}"

    def _generate_post_url(self, post: Dict[str, Any]) -> str:
        """Генерация реального URL поста"""
        owner = post.get("owner", {})
        nickname = owner.get("nickname", "")
        post_id = post.get("id", "")
        if nickname and post_id:
            return f"https://www.tbank.ru/invest/social/profile/{nickname}/{post_id}"
        return ""  # Пустой URL если данных недостаточно

    def _generate_title(self, text: str, author: str) -> str:
        """Генерация заголовка из текста"""
        if not text:
            return "Без заголовка"

        # Первая строка или первое предложение
        first_line = text.strip().split("\n")[0]
        first_sentence = text.split(".")[0]
        candidate = (
            first_line if len(first_line) > len(first_sentence) else first_sentence
        )

        if len(candidate) < 20:
            candidate = text[:100]

        title = candidate.strip()
        if author:
            title = f"{author}: {title}"

        return title[:200] + ("..." if len(title) > 200 else "")

    def _apply_filters(self, item: ParsedItem, filters: Dict[str, Any]) -> bool:
        """Применение фильтров к элементу"""
        if "min_reactions" in filters:
            if item.metadata.get("total_reactions", 0) < filters["min_reactions"]:
                return False
        if "has_images" in filters and filters["has_images"]:
            if not item.metadata.get("has_images", False):
                return False
        if "author" in filters:
            if item.author != filters["author"]:
                return False
        return True

    def validate_item(self, item: ParsedItem) -> bool:
        """
        Расширенная валидация для постов Тинькофф Пульса.
        Минимальные требования для социального контента.
        """
        # Базовые проверки
        if not item.url or not self.validate_url(item.url):
            self.logger.warning(f"Некорректный URL: {item.url}")
            return False

        if not item.content or len(item.content.strip()) < 10:
            self.logger.warning("Слишком короткий пост")
            return False

        # Специфично для TP: должен быть автор
        if not item.author or len(item.author.strip()) == 0:
            self.logger.warning("Пост без автора")
            return False

        # Хотя бы один упомянутый тикер
        mentioned_tickers = item.metadata.get("mentioned_tickers", [])
        if not mentioned_tickers:
            self.logger.warning("Пост без упомянутых тикеров")
            return False

        # Дополнительные проверки
        if len(item.content.strip()) > 10000:  # Абсурдно длинный пост
            self.logger.warning(f"Слишком длинный пост: {len(item.content)} символов")
            return False

        return True

    def validate_url(self, url: str) -> bool:
        """Простая валидация URL"""
        from urllib.parse import urlparse

        try:
            result = urlparse(url)
            return all([result.scheme, result.netloc])
        except Exception:
            return False
