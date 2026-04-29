"""
Клиент для работы с DeepSeek API.
"""

import json
import logging
import os
from datetime import datetime

from dotenv import load_dotenv
from openai import OpenAI

load_dotenv()
logger = logging.getLogger(__name__)


class DeepSeekAnalyzer:
    """Клиент для работы с DeepSeek API."""

    def __init__(self):
        self.client = OpenAI(
            api_key=os.getenv("DEEPSEEK_API_KEY"), base_url="https://api.deepseek.com"
        )
        self.model = "deepseek-v4-flash"

    async def generate_summary(
        self,
        posts: list[dict],
        period_start: datetime,
        period_end: datetime,
        prev_summary: str | None = None,
    ) -> dict | None:
        """
        Генерирует суммаризацию за период.

        Args:
            posts: Список постов [{title, text, published_at, url, source_name}]
            period_start: Начало периода
            period_end: Конец периода
            prev_summary: Суммаризация предыдущего периода (для контекста)

        Returns:
            dict с ключами: topics, summary, trend, important_events, _meta
        """
        if not posts:
            return None

        # Формируем текстовое представление постов
        posts_text = []
        for p in posts[:40]:  # Ограничиваем 40 постами
            time_str = (
                p["published_at"].strftime("%H:%M")
                if p.get("published_at")
                else "--:--"
            )
            source = p.get("source_name", "unknown")
            title = p.get("title", "")
            text = p.get("text", "")
            posts_text.append(f"[{time_str}] [{source}] {title}\n   {text[:300]}...\n")

        # Формируем промпт
        prompt = self._build_prompt(
            posts_text=posts_text,
            period_start=period_start,
            period_end=period_end,
            prev_summary=prev_summary,
        )

        logger.info(
            f"Запрос к DeepSeek: {len(posts)} постов, "
            f"период {period_start} - {period_end}"
        )

        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.5,
                max_tokens=800,
                timeout=30.0,
                extra_body={"thinking": {"type": "disabled"}},
            )

            result_text = response.choices[0].message.content
            usage = response.usage

            logger.info(
                f"DeepSeek ответ: {usage.prompt_tokens} вх, "
                f"{usage.completion_tokens} вых, "
                f"{usage.total_tokens} токенов"
            )

            return self._parse_response(result_text, usage)

        except Exception as e:
            logger.error(f"Ошибка запроса к DeepSeek: {e}")
            return None

    def _build_prompt(
        self,
        posts_text: list[str],
        period_start: datetime,
        period_end: datetime,
        prev_summary: str | None = None,
    ) -> str:
        """Собирает промпт для LLM."""
        start_str = period_start.strftime("%d.%m.%Y %H:%M")
        end_str = period_end.strftime("%d.%m.%Y %H:%M")

        prompt = f"""Ты — аналитик новостного агрегатора. Проанализируй новости за период {start_str} - {end_str}.

"""
        if prev_summary:
            prompt += f"КОНТЕКСТ ПРОШЛОГО ПЕРИОДА:\n{prev_summary}\n\n"

        prompt += "НОВОСТИ ЗА ПЕРИОД:\n"
        prompt += "\n".join(posts_text)

        prompt += """

На основе этих новостей верни ТОЛЬКО JSON (без пояснений в начале и конце) в следующем формате:
{
    "topics": ["тема1", "тема2", "тема3"],
    "summary": "главная сюжетная линия за период (2-3 предложения на русском)",
    "trend": "ключевой тренд или изменение (кратко, 1 предложение)",
    "important_events": ["событие1", "событие2"]
}

Важно: выдели ТОЛЬКО самое важное. Не более 3 тем, не более 2 событий.
"""
        return prompt

    def _parse_response(self, response_text: str, usage) -> dict:
        """Парсит ответ LLM в словарь."""
        try:
            # Очищаем от маркеров кода
            response_text = response_text.strip()
            if response_text.startswith("```json"):
                response_text = response_text[7:]
            if response_text.startswith("```"):
                response_text = response_text[3:]
            if response_text.endswith("```"):
                response_text = response_text[:-3]
            response_text = response_text.strip()

            result = json.loads(response_text)

            # Добавляем метаинформацию
            result["_meta"] = {
                "prompt_tokens": usage.prompt_tokens,
                "completion_tokens": usage.completion_tokens,
                "total_tokens": usage.total_tokens,
                "model": self.model,
            }

            return result

        except json.JSONDecodeError as e:
            logger.error(f"Ошибка парсинга JSON: {e}, текст: {response_text[:500]}")
            return {
                "topics": ["Ошибка анализа"],
                "summary": "Не удалось обработать ответ от анализатора",
                "trend": "Ошибка",
                "important_events": [],
                "_meta": {
                    "prompt_tokens": 0,
                    "completion_tokens": 0,
                    "total_tokens": 0,
                },
            }

    async def raw_request(self, prompt: str) -> str:
        """Простой запрос к LLM без парсинга."""
        try:
            response = self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.5,
                max_tokens=800,
                timeout=30.0,
                extra_body={"thinking": {"type": "disabled"}},
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Ошибка raw запроса к DeepSeek: {e}")
            return "Не удалось получить ответ от анализатора."
