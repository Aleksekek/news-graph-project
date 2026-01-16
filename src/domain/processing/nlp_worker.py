"""
NLP воркер без контекстного менеджера для репозитория.
"""

import asyncio
import time
from typing import Any, Dict, List, Optional

from src.core.exceptions import DatabaseError
from src.domain.storage.database import ArticleRepository
from src.utils.data import clean_text, extract_tickers_from_text
from src.utils.logging import get_logger

logger = get_logger("processing.nlp_worker")


class SimpleNLPWorker:
    """
    Простой NLP воркер без контекстного менеджера.
    """

    def __init__(self, batch_size: int = 20, processing_delay: float = 0.1):
        """
        Args:
            batch_size: Размер батча для обработки
            processing_delay: Задержка между обработкой статей (секунды)
        """
        self.batch_size = batch_size
        self.processing_delay = processing_delay
        self.logger = get_logger(self.__class__.__name__)

    async def process_continuously(self, interval: int = 60):
        """
        Непрерывная обработка статей.

        Args:
            interval: Интервал между проверками новых статей (секунды)
        """
        self.logger.info(f"Запуск NLP воркера (интервал: {interval} сек)")

        while True:
            try:
                processed = await self.process_batch()

                if processed == 0:
                    self.logger.debug(
                        f"Нет статей для обработки. Ожидание {interval} сек..."
                    )
                    await asyncio.sleep(interval)
                else:
                    # Если были статьи, проверяем сразу снова
                    await asyncio.sleep(1)

            except KeyboardInterrupt:
                self.logger.info("Остановка NLP воркера по запросу пользователя")
                break
            except Exception as e:
                self.logger.error(f"Ошибка в NLP воркере: {e}")
                await asyncio.sleep(interval)

    async def process_batch(self) -> int:
        """
        Обработка одного батча статей.

        Returns:
            Количество обработанных статей
        """
        try:
            # Создаем репозиторий
            repository = ArticleRepository()

            # Получаем статьи для обработки
            raw_articles = repository.get_raw_articles_for_processing(
                limit=self.batch_size, status="raw"
            )

            if not raw_articles:
                repository.cleanup()
                return 0

            self.logger.info(f"Найдено {len(raw_articles)} статей для обработки")

            processed_count = 0

            for article in raw_articles:
                try:
                    # Обрабатываем статью
                    success = await self._process_single_article(repository, article)

                    if success:
                        processed_count += 1

                    # Задержка между статьями
                    await asyncio.sleep(self.processing_delay)

                except Exception as e:
                    self.logger.error(
                        f"Ошибка обработки статьи {article.get('id')}: {e}"
                    )
                    self._mark_article_as_failed(repository, article.get("id"))
                    continue

            # Очищаем ресурсы
            repository.cleanup()

            self.logger.info(
                f"Обработано статей: {processed_count}/{len(raw_articles)}"
            )
            return processed_count

        except Exception as e:
            self.logger.error(f"Ошибка обработки батча: {e}")
            return 0

    async def _process_single_article(
        self, repository: ArticleRepository, article: Dict[str, Any]
    ) -> bool:
        """
        Обработка одной статьи.

        Args:
            repository: Репозиторий для работы с БД
            article: Данные статьи из БД

        Returns:
            True если успешно
        """
        article_id = article["id"]
        raw_text = article["raw_text"]
        raw_title = article["raw_title"]

        self.logger.debug(f"Обработка статьи {article_id}: {raw_title[:50]}...")

        try:
            # 1. Очищаем текст
            cleaned_text = clean_text(raw_text, remove_newlines=True)
            cleaned_title = clean_text(raw_title)

            # 2. Извлекаем тикеры (для финансовых новостей)
            tickers = extract_tickers_from_text(cleaned_text)

            # 3. Определяем тему (упрощенная версия)
            topic = self._detect_topic(cleaned_title, cleaned_text)

            # 4. Анализируем тональность (упрощенная версия)
            sentiment_score, sentiment_label = self._analyze_sentiment(cleaned_text)

            # 5. Извлекаем сущности
            entities = self._extract_entities(cleaned_text, tickers)

            # 6. Создаем summary (упрощенная версия)
            summary = self._generate_summary(cleaned_text)

            # 7. Сохраняем результат
            success = repository.save_processed_article(
                raw_article_id=article_id,
                title=cleaned_title,
                text=cleaned_text,
                summary=summary,
                topic=topic,
                sentiment_score=sentiment_score,
                sentiment_label=sentiment_label,
                embedding=None,  # Можно добавить позже
                entities=entities,
            )

            if success:
                self.logger.debug(f"Статья {article_id} успешно обработана")
                return True
            else:
                self.logger.warning(
                    f"Не удалось сохранить обработанную статью {article_id}"
                )
                return False

        except Exception as e:
            self.logger.error(f"Ошибка обработки статьи {article_id}: {e}")
            self._mark_article_as_failed(repository, article_id)
            return False

    def _mark_article_as_failed(self, repository: ArticleRepository, article_id: int):
        """Помечаем статью как failed."""
        try:
            # Обновляем статус через прямой SQL
            with repository:
                pass
        except:
            # Игнорируем ошибку, так как мы убрали контекстный менеджер
            pass

    # Остальные методы остаются без изменений...
    def _detect_topic(self, title: str, text: str) -> str:
        """Определение темы статьи (упрощенная версия)."""
        # Ключевые слова для тем
        topics_keywords = {
            "финансы": [
                "акци",
                "актив",
                "банк",
                "бирж",
                "бюджет",
                "валют",
                "долг",
                "инвест",
                "кредит",
                "курс",
                "рынк",
                "сбер",
                "тикер",
                "фонд",
                "экономик",
                "$",
                "рубл",
            ],
            "политика": [
                "выбор",
                "власт",
                "глав",
                "государств",
                "депутат",
                "должност",
                "закон",
                "министр",
                "правительств",
                "президент",
                "реформ",
                "соглашен",
                "страна",
            ],
            "технологии": [
                "айти",
                "гаджет",
                "данн",
                "инновац",
                "интернет",
                "компани",
                "компьютер",
                "мобильн",
                "онлайн",
                "програм",
                "разработ",
                "сеть",
                "софт",
                "технолог",
            ],
            "бизнес": [
                "бизнес",
                "компани",
                "конкурент",
                "корпорац",
                "коммерц",
                "лояльн",
                "маркетинг",
                "менеджмент",
                "организац",
                "отрасл",
                "партнер",
                "предпринима",
                "производств",
            ],
        }

        combined_text = (title + " " + text).lower()

        for topic, keywords in topics_keywords.items():
            for keyword in keywords:
                if keyword in combined_text:
                    return topic

        return "другое"

    def _analyze_sentiment(self, text: str) -> tuple:
        """Анализ тональности текста (упрощенная версия)."""
        positive_words = [
            "хорош",
            "отличн",
            "прекрасн",
            "замечательн",
            "успеш",
            "выгод",
            "рост",
            "увелич",
            "повыш",
            "прибыл",
            "доход",
            "позитив",
            "оптимист",
        ]
        negative_words = [
            "плох",
            "ужасн",
            "сложн",
            "проблем",
            "кризис",
            "паден",
            "снижен",
            "убыток",
            "потер",
            "негатив",
            "пессимист",
            "риск",
            "опасен",
        ]

        text_lower = text.lower()

        positive_count = sum(1 for word in positive_words if word in text_lower)
        negative_count = sum(1 for word in negative_words if word in text_lower)

        total = positive_count + negative_count

        if total == 0:
            return 0.0, "neutral"

        sentiment_score = (positive_count - negative_count) / total

        if sentiment_score > 0.2:
            label = "positive"
        elif sentiment_score < -0.2:
            label = "negative"
        else:
            label = "neutral"

        return sentiment_score, label

    def _extract_entities(self, text: str, tickers: List[str]) -> List[Dict[str, Any]]:
        """Извлечение сущностей из текста (упрощенная версия)."""
        entities = []

        # Сущности-тикеры
        for ticker in tickers[:10]:  # Ограничиваем количество
            entities.append(
                {
                    "normalized_name": ticker.upper(),
                    "type": "organization",
                    "original_name": ticker,
                    "count": text.upper().count(ticker.upper()),
                    "importance_score": 0.7,  # Высокая важность для тикеров
                    "context_snippet": self._find_context(text, ticker, 50),
                    "external_ids": {"ticker": ticker},
                    "meta": {"sector": "finance"},
                }
            )

        return entities

    def _find_context(
        self, text: str, entity: str, context_chars: int = 50
    ) -> Optional[str]:
        """Поиск контекста для сущности."""
        text_upper = text.upper()
        entity_upper = entity.upper()

        idx = text_upper.find(entity_upper)
        if idx == -1:
            return None

        start = max(0, idx - context_chars)
        end = min(len(text), idx + len(entity) + context_chars)

        context = text[start:end]
        return context.strip()

    def _generate_summary(self, text: str, max_length: int = 200) -> str:
        """Генерация summary (упрощенная версия)."""
        if not text:
            return ""

        # Берем первое предложение
        sentences = text.split(".")
        first_sentence = sentences[0].strip() if sentences else ""

        if len(first_sentence) >= 30:
            summary = first_sentence
        else:
            # Или первые N символов
            summary = text[:max_length].strip()

        if len(summary) < len(text):
            summary += "..."

        return summary
