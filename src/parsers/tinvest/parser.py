"""
Парсер Тинькофф Пульса.
Асинхронная обёртка над tpulse с поддержкой курсоров и фильтров.
"""

import asyncio
import hashlib
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from tpulse import TinkoffPulse

from src.core.exceptions import ParserError
from src.core.models import ParsedItem
from src.parsers.base import BaseParser, ParserConfig, ParseResult
from src.utils.datetime_utils import now_msk, utc_to_msk
from src.utils.logging import log_async_execution_time


class TInvestParser(BaseParser):
    """
    Парсер Тинькофф Пульса.

    Параметры через filters в parse():
        - tickers: List[str] - список тикеров (обязательный)
        - min_reactions: int - минимальное количество реакций
        - has_images: bool - только посты с картинками
        - author: str - фильтр по автору
    """

    def __init__(self, config: ParserConfig):
        super().__init__(config)

        # Синхронный клиент tpulse (работает через executor)
        self._tp = TinkoffPulse()

        # Тикеры из конфига
        self.default_tickers = getattr(config, "tickers", ["SBER", "VTBR", "MOEX"])

        # Ограничения
        self.max_batches_per_ticker = 100
        self.batch_size = 50

    @log_async_execution_time()
    async def parse(self, limit: int = 100, **filters) -> ParseResult:
        """
        Парсинг свежих постов.

        Filters:
            tickers: List[str] - тикеры для поиска
            min_reactions: int - минимальное количество реакций
            has_images: bool - только с картинками
            author: str - фильтр по автору
        """
        tickers = filters.get("tickers", self.default_tickers)

        if not tickers:
            raise ParserError("TInvest парсеру нужен список tickers")

        self.logger.info(f"Парсинг TInvest: лимит={limit}, тикеры={tickers}")

        all_items = []
        seen_ids = set()

        for ticker in tickers:
            try:
                ticker_items = await self._fetch_ticker_posts(
                    ticker=ticker,
                    limit=limit,
                    filters=filters,
                )

                # Дедупликация глобальная
                for item in ticker_items:
                    if item.original_id not in seen_ids:
                        all_items.append(item)
                        seen_ids.add(item.original_id)

                self.logger.debug(
                    f"Тикер {ticker}: {len(ticker_items)} постов, "
                    f"всего уникальных: {len(all_items)}"
                )

            except Exception as e:
                self.logger.error(f"Ошибка тикера {ticker}: {e}")
                continue

        # Сортировка по дате (новые первые)
        all_items.sort(key=lambda x: x.published_at or datetime.min, reverse=True)

        # Ограничиваем по лимиту
        if len(all_items) > limit:
            all_items = all_items[:limit]

        self.logger.info(f"Всего собрано {len(all_items)} уникальных постов")

        return ParseResult(all_items)

    @log_async_execution_time()
    async def parse_period(
        self, start_date: datetime, end_date: datetime, limit: int = 100, **filters
    ) -> ParseResult:
        """
        Архивный парсинг за период.

        Filters:
            tickers: List[str] - тикеры для поиска
            start_cursor: str - курсор для продолжения
        """
        tickers = filters.get("tickers", self.default_tickers)
        start_cursor = filters.get("start_cursor")

        if not tickers:
            raise ParserError("TInvest парсеру нужен список tickers")

        self.logger.info(f"Архивный парсинг TInvest: {start_date.date()} - {end_date.date()}")

        all_items = []
        final_cursor = start_cursor

        for ticker in tickers:
            ticker_items, cursor = await self._fetch_historical_posts(
                ticker=ticker,
                start_date=start_date,
                end_date=end_date,
                limit=limit,
                start_cursor=start_cursor,
                filters=filters,
            )

            all_items.extend(ticker_items)
            if cursor:
                final_cursor = cursor

        # Сохраняем курсор в метаданные последнего поста
        if all_items and final_cursor:
            all_items[-1].metadata["cursor"] = final_cursor

        self.logger.info(f"Архивный парсинг завершён: {len(all_items)} постов")

        return ParseResult(all_items)

    def to_parsed_item(self, raw_data: Dict[str, Any]) -> ParsedItem:
        """Конвертация сырых данных в ParsedItem."""
        post = raw_data["post"]
        ticker = raw_data["ticker"]
        content = post.get("content", {})

        # Извлекаем данные
        text = content.get("text", "")
        author = self._extract_author(post.get("owner", {}))
        published_at = self._extract_date(post)
        original_id = self._generate_id(post)
        url = self._generate_url(post)

        # Извлекаем метаданные
        instruments = content.get("instruments", [])
        mentioned_tickers = [i.get("ticker", "") for i in instruments if i.get("ticker")]

        reactions = post.get("reactions", {})
        total_reactions = reactions.get("totalCount", 0)
        reactions_counters = reactions.get("counters", [])

        images = content.get("images", [])
        hashtags = content.get("hashtags", [])

        # Заголовок (берём первую строку или первое предложение)
        title = self._make_title(text, author, mentioned_tickers)

        metadata = {
            "target_ticker": ticker,
            "mentioned_tickers": mentioned_tickers,
            "total_reactions": total_reactions,
            "comments_count": post.get("commentsCount", 0),
            "has_images": len(images) > 0,
            "images_count": len(images),
            "images": images,
            "hashtags": hashtags,
            "hashtags_count": len(hashtags),
            "reactions": reactions_counters,
            "author_nickname": author,
            "owner": post.get("owner", {}),
        }

        return ParsedItem(
            source_id=self.source_id,
            source_name=self.source_name,
            original_id=original_id,
            url=url,
            title=title,
            content=text,
            published_at=published_at,
            author=author,
            metadata=metadata,
            raw_data={"original_post": post},
        )

    # ==================== Внутренние методы ====================

    async def _fetch_ticker_posts(
        self,
        ticker: str,
        limit: int,
        filters: Dict[str, Any],
    ) -> List[ParsedItem]:
        """Получение постов для одного тикера."""
        items = []
        cursor = None
        batches_loaded = 0

        while len(items) < limit and batches_loaded < self.max_batches_per_ticker:
            # Загружаем батч
            batch_data = await self._request_posts(ticker, cursor)

            if not batch_data or not batch_data.get("items"):
                break

            # Обрабатываем батч
            batch_items = []
            for post in batch_data["items"]:
                parsed = self.to_parsed_item({"post": post, "ticker": ticker})

                if self._apply_filters(parsed, filters):
                    batch_items.append(parsed)

            items.extend(batch_items)

            # Ограничиваем по лимиту
            if len(items) > limit:
                items = items[:limit]

            # Следующий курсор
            cursor = batch_data.get("nextCursor")
            if not cursor or not batch_data.get("hasNext", False):
                break

            batches_loaded += 1
            await self._delay()

        return items

    async def _fetch_historical_posts(
        self,
        ticker: str,
        start_date: datetime,
        end_date: datetime,
        limit: int,
        start_cursor: Optional[str],
        filters: Dict[str, Any],
    ) -> tuple[List[ParsedItem], Optional[str]]:
        """Исторический парсинг с фильтром по дате."""
        items = []
        cursor = start_cursor
        final_cursor = None
        batches_loaded = 0

        # Преобразуем даты для сравнения (naive, без времени)
        start_naive = start_date.replace(hour=0, minute=0, second=0, microsecond=0)
        # Конец дня
        end_naive = end_date.replace(hour=23, minute=59, second=59, microsecond=999999)

        self.logger.debug(
            f"Исторический парсинг {ticker}: {start_naive.date()} - {end_naive.date()}"
        )

        found_older = False

        while len(items) < limit and not found_older:
            batch_data = await self._request_posts(ticker, cursor)

            if not batch_data or not batch_data.get("items"):
                break

            final_cursor = batch_data.get("nextCursor")

            for post in batch_data["items"]:
                if len(items) >= limit:
                    break

                post_date = self._extract_date(post)
                if not post_date:
                    continue

                # Если пост старше начала периода - останавливаемся
                if post_date < start_naive:
                    found_older = True
                    break

                # Если пост в периоде
                if start_naive <= post_date <= end_naive:
                    parsed = self.to_parsed_item({"post": post, "ticker": ticker})

                    if self._apply_filters(parsed, filters):
                        items.append(parsed)

            cursor = final_cursor
            if not cursor or not batch_data.get("hasNext", False):
                break

            batches_loaded += 1
            await self._delay()

        self.logger.info(
            f"Тикер {ticker}: {len(items)} постов за период, " f"батчей: {batches_loaded}"
        )

        return items, final_cursor

    async def _request_posts(
        self, ticker: str, cursor: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Асинхронный запрос к tpulse."""
        loop = asyncio.get_event_loop()

        try:
            # tpulse синхронный, запускаем в executor
            return await loop.run_in_executor(None, self._tp.get_posts_by_ticker, ticker, cursor)
        except Exception as e:
            self.logger.error(f"Ошибка запроса для {ticker}: {e}")
            return None

    # ==================== Хелперы ====================

    def _extract_author(self, owner: Dict[str, Any]) -> str:
        """Извлечение имени автора."""
        return owner.get("nickname", "") or owner.get("name", "") or "Аноним"

    def _extract_date(self, post: Dict[str, Any]) -> Optional[datetime]:
        """Извлечение и конвертация даты из UTC в MSK (TInvest API)."""
        try:
            inserted = post.get("inserted", "")
            if not inserted:
                return None

            # TInvest возвращает время в UTC
            # Всегда конвертируем в MSK
            if inserted.endswith("Z"):
                # С Z - точно UTC
                dt = datetime.fromisoformat(inserted.replace("Z", "+00:00"))
            else:
                # Без Z - пробуем как есть, но всё равно сдвигаем
                dt = datetime.fromisoformat(inserted)
                # Если нет tzinfo, считаем что UTC
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)

            return utc_to_msk(dt)

        except Exception as e:
            self.logger.debug(f"Ошибка парсинга даты: {e}")
            return None

        except Exception as e:
            self.logger.debug(f"Ошибка парсинга даты: {e}")
            return None

    def _generate_id(self, post: Dict[str, Any]) -> str:
        """Генерация уникального ID поста."""
        post_id = post.get("id", "")
        inserted = post.get("inserted", "")
        content = post.get("content", {}).get("text", "")[:100]

        key = f"{post_id}|{inserted}|{content}"
        hash_obj = hashlib.sha256(key.encode()).hexdigest()[:16]

        return f"tinvest_{hash_obj}"

    def _generate_url(self, post: Dict[str, Any]) -> str:
        """Генерация URL поста."""
        owner = post.get("owner", {})
        nickname = owner.get("nickname", "")
        post_id = post.get("id", "")

        if nickname and post_id:
            return f"https://www.tbank.ru/invest/social/profile/{nickname}/{post_id}"

        return ""

    def _make_title(self, text: str, author: str, tickers: List[str]) -> str:
        """Генерация заголовка из текста."""
        if not text:
            return "Без заголовка"

        # Первая строка или первое предложение
        first_line = text.strip().split("\n")[0]
        first_sentence = text.split(".")[0]

        candidate = first_line if len(first_line) > len(first_sentence) else first_sentence

        if len(candidate) < 20:
            candidate = text[:100]

        title = candidate.strip()

        # Добавляем автора
        if author and author != "Аноним":
            title = f"{author}: {title}"

        # Добавляем тикеры
        if tickers:
            tickers_str = ", ".join(tickers[:2])
            title = f"{title} [{tickers_str}]"

        return title[:200] + ("..." if len(title) > 200 else "")

    def _apply_filters(self, item: ParsedItem, filters: Dict[str, Any]) -> bool:
        """Применение фильтров к посту."""
        # Минимальное количество реакций
        if "min_reactions" in filters:
            if item.metadata.get("total_reactions", 0) < filters["min_reactions"]:
                return False

        # Только с картинками
        if filters.get("has_images", False):
            if not item.metadata.get("has_images", False):
                return False

        # Фильтр по автору
        if "author" in filters:
            if item.author != filters["author"]:
                return False

        return True

    # Переопределяем валидацию для TP
    def _validate_item(self, item: ParsedItem) -> bool:
        """Специфичная валидация для постов TP."""
        if not self._validate_url(item.url):
            return False

        if not item.content or len(item.content.strip()) < 10:
            return False

        if not item.author or len(item.author.strip()) == 0:
            return False

        # Хотя бы один тикер
        mentioned = item.metadata.get("mentioned_tickers", [])
        if not mentioned:
            return False

        return True
