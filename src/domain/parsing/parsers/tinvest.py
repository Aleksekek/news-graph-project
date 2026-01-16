"""
Парсер Тинькофф Пульса
"""

import asyncio
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import pandas as pd
from tpulse import TinkoffPulse

from src.core.exceptions import ParserError
from src.domain.parsing.base import BaseParser, ParsedItem, ParserConfig
from src.utils.data import safe_datetime, safe_int, safe_list, safe_str
from src.utils.retry import retry_network


class TInvestParser(BaseParser):
    """
    Парсер Тинькофф Пульса.
    Обертка над существующей библиотекой tpulse с async интерфейсом.
    """

    def __init__(self, config: ParserConfig):
        super().__init__(config)

        # Инициализация синхронного парсера
        self.tp = TinkoffPulse()

        # Настройки
        self.request_delay = self.config.request_delay
        self.sleep_interval = 2000
        self.sleep_duration = 30
        self.checkpoint_interval = 500

    async def parse_recent(
        self,
        limit: int = 100,
        tickers: Optional[List[str]] = None,
        **kwargs,  # Принимаем дополнительные параметры
    ) -> List[ParsedItem]:
        """
        Парсинг последних постов Тинькофф Пульса.

        Args:
            limit: Максимальное количество постов в сумме
            tickers: Список тикеров для парсинга (переопределяет дефолтные)
            **kwargs: Дополнительные параметры (игнорируются)

        Returns:
            Список ParsedItem
        """
        # Используем переданные тикеры или дефолтные
        target_tickers = tickers if tickers else ["SBER", "VTBR", "MOEX"]

        self.logger.info(f"Парсинг Тинькофф Пульса: {target_tickers} (лимит: {limit})")

        all_items = []

        # Распределяем лимит по тикерам
        limit_per_ticker = max(limit // len(target_tickers), 1)

        for ticker in target_tickers:
            try:
                ticker_items = await self._parse_ticker_posts(ticker, limit_per_ticker)
                all_items.extend(ticker_items)

                self.logger.info(f"Тикер {ticker}: {len(ticker_items)} постов")

                # Задержка между тикерами
                if ticker != target_tickers[-1]:
                    await asyncio.sleep(5)

            except Exception as e:
                self.logger.error(f"Ошибка парсинга тикера {ticker}: {e}")
                continue

        self.logger.info(f"Всего распарсено: {len(all_items)} постов")
        return all_items

    async def _parse_ticker_posts(self, ticker: str, limit: int) -> List[ParsedItem]:
        """Парсинг постов по конкретному тикеру"""
        self.logger.debug(f"Парсинг тикера: {ticker}")

        # Используем синхронный вызов в thread pool, чтобы не блокировать event loop
        loop = asyncio.get_event_loop()

        try:
            # Вызываем синхронный код в отдельном потоке
            df = await loop.run_in_executor(
                None, self._get_posts_dataframe_sync, ticker, limit
            )

            if df.empty:
                self.logger.warning(f"Нет постов для тикера {ticker}")
                return []

            # Конвертируем DataFrame в ParsedItem
            parsed_items = []
            for _, row in df.iterrows():
                try:
                    parsed_item = self._row_to_parsed_item(row, ticker)
                    if parsed_item and self.validate_item(parsed_item):
                        parsed_items.append(parsed_item)
                except Exception as e:
                    self.logger.error(f"Ошибка конвертации строки: {e}")
                    continue

            return parsed_items

        except Exception as e:
            raise ParserError(f"Ошибка парсинга тикера {ticker}: {e}")

    def _get_posts_dataframe_sync(self, ticker: str, num_posts: int) -> pd.DataFrame:
        """
        Синхронный метод получения постов.
        Оригинальная логика из PulseParser.
        """
        all_posts = []
        cursor = None
        posts_loaded = 0

        while posts_loaded < num_posts:
            # Пауза при достижении интервала
            if (
                posts_loaded > 0
                and posts_loaded > 35
                and posts_loaded % self.sleep_interval in range(30)
            ):
                self.logger.info(
                    f"Пауза {self.sleep_duration} сек после {posts_loaded} постов"
                )
                time.sleep(self.sleep_duration)

            # Получаем данные
            data = self._make_request_with_retry_sync(ticker, cursor)

            if not data or not data.get("items"):
                break

            # Обрабатываем посты
            for post in data["items"]:
                if posts_loaded >= num_posts:
                    break

                try:
                    post_data = self._extract_post_data(post, ticker)
                    all_posts.append(post_data)
                    posts_loaded += 1

                except Exception as e:
                    self.logger.error(f"Ошибка обработки поста: {e}")
                    continue

            # Пагинация
            if data.get("hasNext", False) and data.get("nextCursor"):
                cursor = data["nextCursor"]
                time.sleep(self.request_delay)
            else:
                break

        # Создаем DataFrame
        df = pd.DataFrame(all_posts)

        if not df.empty and "date" in df.columns and "time" in df.columns:
            df["datetime"] = pd.to_datetime(
                df["date"].astype(str) + " " + df["time"].astype(str)
            )
            df = df.sort_values("datetime", ascending=False).reset_index(drop=True)
            df = df.drop("datetime", axis=1)

        return df

    def _make_request_with_retry_sync(
        self, ticker: str, cursor: Optional[str] = None, max_retries: int = 3
    ) -> Optional[Dict[str, Any]]:
        """Синхронный запрос с повторными попытками"""
        for attempt in range(max_retries):
            try:
                if attempt > 0:
                    delay = self.request_delay * (2**attempt)
                    self.logger.warning(
                        f"Повторная попытка {attempt + 1}/{max_retries} через {delay} сек"
                    )
                    time.sleep(delay)
                else:
                    time.sleep(self.request_delay)

                return self.tp.get_posts_by_ticker(ticker, cursor=cursor)

            except Exception as e:
                self.logger.error(
                    f"Ошибка запроса (попытка {attempt + 1}/{max_retries}): {e}"
                )

                if attempt == max_retries - 1:
                    return None

        return None

    def _extract_post_data(self, post: Dict[str, Any], ticker: str) -> Dict[str, Any]:
        """Извлечение данных из поста"""
        content = post.get("content", {})
        owner = post.get("owner", {})
        reactions = post.get("reactions", {})

        # Конвертируем время
        date, time_val = self._convert_time_to_msk(post.get("inserted", ""))

        # Извлекаем тикеры
        mentioned_tickers = self._extract_mentioned_tickers(content)

        # Статистика реакций
        total_reactions, reaction_types = self._calculate_reactions_stats(reactions)

        # Формируем данные
        return {
            "date": date,
            "time": time_val,
            "username": owner.get("nickname", ""),
            "post_text": content.get("text", ""),
            "target_ticker": ticker,
            "mentioned_tickers": mentioned_tickers,
            "mentioned_tickers_count": len(mentioned_tickers),
            "total_reactions": total_reactions,
            "comments_count": post.get("commentsCount", 0),
            "has_images": len(content.get("images", [])) > 0,
            "images_count": len(content.get("images", [])),
            "hashtags_count": len(content.get("hashtags", [])),
            "reaction_like": reaction_types.get("like", 0),
            "reaction_rocket": reaction_types.get("rocket", 0),
            "reaction_dislike": reaction_types.get("dislike", 0),
            "reaction_buy_up": reaction_types.get("buy-up", 0),
            "reaction_get_rid": reaction_types.get("get-rid", 0),
            "original_data": post,  # Сохраняем оригинальные данные
        }

    def _convert_time_to_msk(self, utc_time_str: str) -> tuple:
        """Конвертация времени UTC в московское время"""
        try:
            utc_time = datetime.fromisoformat(utc_time_str.replace("Z", "+00:00"))
            msk_time = utc_time + timedelta(hours=3)
            date = msk_time.date()
            time_val = msk_time.time().replace(microsecond=0)
            return date, time_val
        except Exception as e:
            self.logger.warning(f"Ошибка конвертации времени: {e}")
            return None, None

    def _extract_mentioned_tickers(self, content: Dict) -> List[str]:
        """Извлечение упомянутых тикеров"""
        tickers = []
        if "instruments" in content:
            for instrument in content["instruments"]:
                if "ticker" in instrument:
                    tickers.append(instrument["ticker"])
        return tickers

    def _calculate_reactions_stats(self, reactions: Dict) -> tuple:
        """Подсчет статистики реакций"""
        total_reactions = reactions.get("totalCount", 0)

        reaction_types = {}
        if "counters" in reactions:
            for counter in reactions["counters"]:
                reaction_type = counter.get("type", "unknown")
                count = counter.get("count", 0)
                reaction_types[reaction_type] = count

        return total_reactions, reaction_types

    def _row_to_parsed_item(self, row: pd.Series, ticker: str) -> Optional[ParsedItem]:
        """Конвертация строки DataFrame в ParsedItem"""
        try:
            # Базовые данные
            post_text = safe_str(row.get("post_text", ""))
            username = safe_str(row.get("username", ""))

            if not post_text.strip():
                return None

            # Дата и время
            date_val = row.get("date")
            time_val = row.get("time")

            published_at = None
            if date_val is not None and time_val is not None:
                try:
                    if hasattr(date_val, "strftime"):
                        date_str = date_val.strftime("%Y-%m-%d")
                    else:
                        date_str = str(date_val)

                    if hasattr(time_val, "strftime"):
                        time_str = time_val.strftime("%H:%M:%S")
                    elif time_val is not None:
                        time_str = str(time_val)
                    else:
                        time_str = "00:00:00"

                    published_at = datetime.strptime(
                        f"{date_str} {time_str}", "%Y-%m-%d %H:%M:%S"
                    )
                except Exception:
                    published_at = safe_datetime(row.get("datetime"))

            # Генерация ID и URL
            original_id = self._generate_post_id(row, ticker)
            url = self._generate_post_url(row, ticker)

            # Упомянутые тикеры
            mentioned_tickers = safe_list(row.get("mentioned_tickers", []))

            # Метаданные
            metadata = {
                "target_ticker": ticker,
                "mentioned_tickers": mentioned_tickers,
                "mentioned_tickers_count": len(mentioned_tickers),
                "total_reactions": safe_int(row.get("total_reactions", 0)),
                "comments_count": safe_int(row.get("comments_count", 0)),
                "has_images": bool(row.get("has_images", False)),
                "images_count": safe_int(row.get("images_count", 0)),
                "hashtags_count": safe_int(row.get("hashtags_count", 0)),
                "reactions": {
                    "like": safe_int(row.get("reaction_like", 0)),
                    "rocket": safe_int(row.get("reaction_rocket", 0)),
                    "dislike": safe_int(row.get("reaction_dislike", 0)),
                    "buy_up": safe_int(row.get("reaction_buy_up", 0)),
                    "get_rid": safe_int(row.get("reaction_get_rid", 0)),
                },
            }

            # Создаем ParsedItem
            return ParsedItem(
                source_id=self.source_id,
                source_name=self.source_name,
                original_id=original_id,
                url=url,
                title=self._generate_title(post_text),
                content=post_text,
                published_at=published_at,
                author=username if username else None,
                metadata=metadata,
                raw_data=row.to_dict() if hasattr(row, "to_dict") else dict(row),
            )

        except Exception as e:
            self.logger.error(f"Ошибка создания ParsedItem: {e}")
            return None

    def _generate_post_id(self, row: pd.Series, ticker: str) -> str:
        """Генерация уникального ID поста"""
        import hashlib
        import json

        # Создаем строку для хэширования
        content_for_hash = json.dumps(
            {
                "username": safe_str(row.get("username", "")),
                "post_text": safe_str(row.get("post_text", "")),
                "date": str(row.get("date", "")),
                "time": str(row.get("time", "")),
                "ticker": ticker,
            },
            sort_keys=True,
            ensure_ascii=False,
        )

        # Хэшируем
        post_hash = hashlib.sha256(content_for_hash.encode()).hexdigest()[:16]
        return f"tinvest_{post_hash}"

    def _generate_post_url(self, row: pd.Series, ticker: str) -> str:
        """Генерация URL поста"""
        username = safe_str(row.get("username", ""))
        date_val = row.get("date")

        if hasattr(date_val, "strftime"):
            date_str = date_val.strftime("%Y%m%d")
        else:
            date_str = str(date_val).replace("-", "")

        # Очищаем username для URL
        username_clean = "".join(
            c for c in username if c.isalnum() or c in "-_"
        ).lower()

        return f"tinvest://{date_str}/{username_clean}/{ticker}"

    def _generate_title(self, text: str) -> str:
        """Генерация заголовка из текста"""
        if not text:
            return "Без заголовка"

        # Берем первую непустую строку
        lines = text.strip().split("\n")
        for line in lines:
            if line.strip() and len(line.strip()) > 5:
                title = line.strip()[:150]
                return title + "..." if len(line.strip()) > 150 else title

        # Если все строки пустые, берем начало текста
        return text[:100] + "..." if len(text) > 100 else text

    def validate_item(self, item: ParsedItem) -> bool:
        """
        Переопределенная валидация для постов Тинькофф Пульса.
        Требования менее строгие чем для новостных статей.
        """
        if not item.url:
            self.logger.warning("Пост без URL")
            return False

        if not item.content or len(item.content.strip()) < 10:
            self.logger.warning("Слишком короткий пост")
            return False

        return True
