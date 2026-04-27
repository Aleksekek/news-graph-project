import logging
from datetime import datetime, timedelta
from typing import Optional

from src.domain.processing.llm_analyzer import DeepSeekAnalyzer
from src.domain.storage.summarization_repository import SummarizationRepository

logger = logging.getLogger(__name__)


class SummarizationService:
    """Сервис для управления суммаризациями"""

    def __init__(self):
        self.llm = DeepSeekAnalyzer()
        self.repo = SummarizationRepository()

    async def generate_hourly_summary(self, hour: datetime) -> Optional[int]:
        """Генерирует часовую суммаризацию"""
        period_start = hour.replace(minute=0, second=0, microsecond=0)
        period_end = period_start + timedelta(hours=1)

        # Проверяем, есть ли уже суммаризация
        existing = await self.repo.get_summaries_for_period(
            period_start, period_end, "hour"
        )
        if existing:
            logger.info(f"Суммаризация за {period_start} уже существует")
            # Проверяем оба возможных варианта
            first_item = existing[0]
            if "id" in first_item:
                return first_item["id"]
            elif "id" in first_item:
                return first_item["id"]
            else:
                logger.warning(f"Неизвестная структура existing[0]: {first_item.keys()}")
                return None

        # Получаем сбалансированную выборку статей за час
        articles = await self.repo.get_smart_articles_for_summarization(
            period_start, period_end, total_limit=80
        )

        if len(articles) < 3:
            logger.info(f"Мало статей за час ({len(articles)}), пропускаем")
            return None

        # Получаем предыдущую суммаризацию для контекста
        prev_summary_data = await self.repo.get_last_summary("hour")
        prev_summary = None
        if prev_summary_data and isinstance(prev_summary_data, dict):
            content = prev_summary_data.get("content", {})
            if isinstance(content, dict):
                prev_summary = content.get("summary")

        # Генерируем суммаризацию через LLM
        result = await self.llm.generate_summary(
            posts=articles,
            period_start=period_start,
            period_end=period_end,
            prev_summary=prev_summary,
        )

        if not result:
            return None

        # Рассчитываем стоимость
        meta = result.get("_meta", {})
        prompt_tokens = meta.get("prompt_tokens", 0)
        completion_tokens = meta.get("completion_tokens", 0)

        cost = (prompt_tokens / 1_000_000) * 0.14 + (
            completion_tokens / 1_000_000
        ) * 0.28

        result["_meta"] = {
            **meta,
            "cost_usd": round(cost, 6),
            "articles_count": len(articles),
            "sources_count": len(set(a.get("source_name") for a in articles)),
        }

        # Сохраняем в БД
        summary_id = await self.repo.save_summary(
            period_start=period_start,
            period_end=period_end,
            period_type="hour",
            content=result,
        )

        logger.info(
            f"Часовая суммаризация сохранена: id={summary_id}, стоимость=${cost:.6f}"
        )
        return summary_id

    async def generate_daily_summary(self, date: datetime) -> Optional[int]:
        """Генерирует дневную суммаризацию на основе часовых"""
        day_start = date.replace(hour=0, minute=0, second=0, microsecond=0)
        day_end = day_start + timedelta(days=1)

        # Проверяем, есть ли уже
        existing = await self.repo.get_summaries_for_period(day_start, day_end, "day")
        if existing:
            logger.info(f"Дневная суммаризация за {day_start.date()} уже существует")
            return existing[0]["id"]

        # Собираем часовые суммаризации за день
        hourly_summaries = await self.repo.get_summaries_for_period(
            day_start, day_end, "hour"
        )

        if not hourly_summaries:
            logger.warning(f"Нет часовых суммаризаций за {day_start.date()}")
            return None

        # Агрегируем часовые данные в читаемый формат
        aggregated = self._aggregate_hourly_summaries(hourly_summaries)

        # Для дневной суммаризации используем ТОЛЬКО часовые сводки, без сырых постов
        # Формируем текстовое представление из часовых суммаризаций
        summaries_text = []
        for s in hourly_summaries:
            period_start = s["period_start"]
            content = s["content"]
            if isinstance(content, dict):
                summary = content.get("summary", "")
                topics = ", ".join(content.get("topics", [])[:3])
                summaries_text.append(
                    f"[{period_start.strftime('%H:%M')}] Темы: {topics}\n   {summary}\n"
                )

        if not summaries_text:
            logger.warning(f"Нет данных в часовых суммаризациях за {day_start.date()}")
            return None

        # Формируем промпт для дневной суммаризации
        prompt = f"""
    Ты — аналитик новостного агрегатора. Проанализируй сводки за {day_start.strftime('%d.%m.%Y')}.

    ЧАСОВЫЕ СВОДКИ:
    {chr(10).join(summaries_text)}

    На основе этих данных верни ТОЛЬКО JSON (без пояснений):
    {{
        "topics": ["главная тема дня 1", "тема 2", "тема 3"],
        "summary": "главная сюжетная линия дня (2-3 предложения)",
        "trend": "ключевой тренд дня (1 предложение)",
        "important_events": ["событие1", "событие2"]
    }}

    Выдели ТОЛЬКО самое важное за весь день.
    """

        try:
            result_content = await self.llm._raw_request(prompt)

            import json

            result = json.loads(result_content)

            result["_meta"] = {
                "source": "daily_from_hourly",
                "hourly_count": len(hourly_summaries),
                "model": "deepseek-v4-flash",
            }

            # Сохраняем
            summary_id = await self.repo.save_summary(
                period_start=day_start,
                period_end=day_end,
                period_type="day",
                content=result,
            )

            logger.info(f"Дневная суммаризация сохранена: id={summary_id}")
            return summary_id

        except Exception as e:
            logger.error(f"Ошибка генерации дневной суммаризации: {e}")
            return None

    def _aggregate_hourly_summaries(self, hourly_summaries: list) -> Optional[str]:
        """Агрегирует часовые суммаризации в текст для контекста"""
        if not hourly_summaries:
            return None

        lines = []
        for s in hourly_summaries[-6:]:  # последние 6 часов
            period_start = s["period_start"]
            content = s["content"]
            summary = (
                content.get("summary", "")[:150]
                if isinstance(content, dict)
                else str(content)[:150]
            )
            lines.append(f"{period_start.strftime('%H:%M')}: {summary}")
        return "\n".join(lines)
