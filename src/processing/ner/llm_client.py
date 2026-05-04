"""
LLM-based NER через DeepSeek API.

Альтернатива NatashaClient. Сильно лучше на:
  - Канонизация имён (Стубб + Александр Стубб → одна сущность)
  - Расшифровка аббревиатур (ВСУ → Вооружённые силы Украины)
  - Распознавание тикеров как ссылок на компанию ($SBER → Сбербанк)
  - Корректная типизация (Внуково → location, не organization)
  - Импорт importance из текста (subject/key/mention)
  - Извлечение событий (event)

Цена: API-вызовы. v4-flash ≈ $0.0001 / статья. С DeepSeek prompt caching ещё дешевле.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

from openai import AsyncOpenAI

from src.config.settings import settings
from src.core.models import ExtractedEntity
from src.utils.logging import get_logger

logger = get_logger("ner.llm_client")

# Маппинг категориальной важности в numeric (соответствует шкале Natasha)
_IMPORTANCE_TO_SCORE = {"subject": 1.0, "key": 0.7, "mention": 0.3}
_ALLOWED_TYPES = {"person", "organization", "location", "event"}
_ALLOWED_IMPORTANCES = set(_IMPORTANCE_TO_SCORE.keys())

_DEFAULT_MODEL = "deepseek-v4-flash"
_DEFAULT_CONCURRENCY = 5
_MAX_TEXT_CHARS = 8000  # защита от слишком длинных статей в промпт
_REQUEST_TIMEOUT = 60.0
_MAX_OUTPUT_TOKENS = 2000

PROMPT_TEMPLATE = """Ты — NER-система для русскоязычных новостей.

ТЕКСТ:
{text}

Извлеки именованные сущности и верни ТОЛЬКО JSON-массив:
[
  {{"mention": "как в тексте", "canonical": "именительный падеж, полная форма",
    "type": "person|organization|location|event", "importance": "subject|key|mention"}}
]

ТИПЫ:
- person: реальные люди по именам (текущие, исторические, литературные).
- organization: компании, ведомства, СМИ, военные формирования, политические партии,
  а также мессенджеры/сервисы/приложения с корпоративной identity.
  Примеры: Сбербанк, ВСУ, ООН, ТАСС, Telegram, Facebook, Сбербанк Онлайн, Windows 11, Notepad.
- location: страны, города, регионы, географические объекты, аэропорты, вокзалы, мосты,
  именованные здания. Примеры: Москва, Россия, Тверская улица, Внуково, Кремль.
- event: ИМЕНОВАННЫЕ time-bounded события.
  Примеры: ПМЭФ, IPO Сбербанка, Парад Победы 2026, Олимпиада 2024, Великая Отечественная война.

ВАЖНОСТЬ (importance) — насколько сущность центральна для статьи:
- subject: статья ПРО эту сущность. Заголовок про неё, лид про неё, действие крутится вокруг.
  Обычно 1-2 на статью, иногда 0 (если статья про событие/процесс без явного субъекта).
- key: активно участвует в сюжете, упомянута неоднократно, без неё статья теряет смысл.
  Обычно 2-5 на статью.
- mention: фон, контекст, перечисление, упомянута 1-2 раза мельком.
  Большинство сущностей именно такие.

ОСОБЫЕ ПРАВИЛА:
- Тикеры ($SBER, {{$GMKN}}, $YNDX) — это ССЫЛКА на компанию.
  type=organization, canonical=полное имя компании (Сбербанк, Норникель, Яндекс).
- Сервисы/приложения/мессенджеры с корпоративной identity — всегда type=organization.
  Telegram, WhatsApp, Сбербанк Онлайн, Windows, Copilot → organization, НЕ отдельный тип.
- Физические товары и конкретные модели (iPhone 15, Lada Granta, Sukhoi Superjet 100) —
  НЕ извлекай как сущности. Если статья ПРО них, извлекай производителя.
- Должности без имени (Президент, Министр), национальности (украинец, россиянин),
  профессии, generic-слова — НЕ извлекай.
- Generic события без названия ("встреча", "переговоры", "санкции", "атака") — НЕ извлекай.
  Только конкретные именованные события (ПМЭФ, не "форум").
- Дубли в одной статье недопустимы: одна сущность = одна запись с самой полной canonical.
- canonical: всегда именительный падеж. Расшифровывай аббревиатуры если уверен
  (ЕС → "Европейский союз"; РФ → "Россия"; ВСУ → "Вооружённые силы Украины"; ПВО → "Противовоздушная оборона").
- Если нет именованных сущностей — верни []

Верни ТОЛЬКО JSON-массив, без пояснений и markdown-обрамления."""


class LLMNERClient:
    """NER через DeepSeek. Async. Совместим по интерфейсу с NatashaClient."""

    def __init__(
        self,
        model: str = _DEFAULT_MODEL,
        concurrency: int = _DEFAULT_CONCURRENCY,
    ) -> None:
        if not settings.DEEPSEEK_API_KEY:
            raise RuntimeError(
                "DEEPSEEK_API_KEY не задан в settings. LLMNERClient требует ключа."
            )
        self.client = AsyncOpenAI(
            api_key=settings.DEEPSEEK_API_KEY, base_url="https://api.deepseek.com"
        )
        self.model = model
        # Защита от пиковых параллельных вызовов; в текущем pipeline статьи
        # обрабатываются последовательно, но семафор готов к параллелизму.
        self._sem = asyncio.Semaphore(concurrency)

    async def extract(self, title: str, text: str) -> list[ExtractedEntity]:
        """Извлекает сущности через DeepSeek. Возвращает [] при ошибке (без exception)."""
        combined = f"{title}\n\n{text}" if text else title
        if not combined.strip():
            return []
        # Срез на случай очень длинных статей. Промпт устанет, output превысит max_tokens.
        if len(combined) > _MAX_TEXT_CHARS:
            combined = combined[:_MAX_TEXT_CHARS]

        async with self._sem:
            content = await self._call_deepseek(combined)

        if content is None:
            return []
        return self._parse_response(content, combined)

    async def _call_deepseek(self, text: str) -> str | None:
        try:
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[{"role": "user", "content": PROMPT_TEMPLATE.format(text=text)}],
                temperature=0.0,
                max_tokens=_MAX_OUTPUT_TOKENS,
                timeout=_REQUEST_TIMEOUT,
                extra_body={"thinking": {"type": "disabled"}},
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"DeepSeek NER call error: {e}")
            return None

    @staticmethod
    def _strip_markdown(content: str) -> str:
        content = content.strip()
        for prefix in ("```json", "```"):
            if content.startswith(prefix):
                content = content[len(prefix):]
                break
        if content.endswith("```"):
            content = content[:-3]
        return content.strip()

    @staticmethod
    def _validate_item(item: Any) -> dict | None:
        if not isinstance(item, dict):
            return None
        canonical = item.get("canonical")
        type_ = item.get("type")
        if not canonical or not isinstance(canonical, str):
            return None
        if type_ not in _ALLOWED_TYPES:
            return None
        mention = item.get("mention") or canonical
        importance = item.get("importance", "mention")
        if importance not in _ALLOWED_IMPORTANCES:
            importance = "mention"
        return {
            "mention": str(mention).strip(),
            "canonical": canonical.strip(),
            "type": type_,
            "importance": importance,
        }

    @staticmethod
    def _extract_context(text: str, mention: str, window: int = 150) -> str | None:
        if not mention:
            return None
        idx = text.lower().find(mention.lower())
        if idx == -1:
            return None
        start = max(0, idx - window // 2)
        end = min(len(text), idx + len(mention) + window // 2)
        return text[start:end].strip() or None

    def _parse_response(self, content: str, source_text: str) -> list[ExtractedEntity]:
        cleaned = self._strip_markdown(content)
        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError as e:
            logger.warning(f"JSON parse error: {e}; preview: {cleaned[:200]!r}")
            return []
        if not isinstance(data, list):
            return []

        entities: list[ExtractedEntity] = []
        seen: set[tuple[str, str]] = set()
        text_lower = source_text.lower()

        for raw_item in data:
            item = self._validate_item(raw_item)
            if item is None:
                continue
            key = (item["canonical"].lower(), item["type"])
            if key in seen:
                continue
            seen.add(key)

            count = max(1, text_lower.count(item["mention"].lower()))
            context = self._extract_context(source_text, item["mention"])

            entities.append(
                ExtractedEntity(
                    original_name=item["mention"],
                    normalized_name=item["canonical"],
                    entity_type=item["type"],
                    count=count,
                    importance_score=_IMPORTANCE_TO_SCORE[item["importance"]],
                    context_snippet=context,
                )
            )
        return entities
