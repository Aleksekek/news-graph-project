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

# SYSTEM_PROMPT — стабильный префикс. Идёт system-сообщением, чтобы DeepSeek
# prompt caching мог его кешировать (cache hit стоит ×10 дешевле cache miss:
# $0.014/M vs $0.14/M на v3-flash). Переменная часть (текст статьи) идёт
# отдельным user-сообщением — см. _call_deepseek.
SYSTEM_PROMPT = """Ты — NER-система для русскоязычных новостей.

Из присланного текста извлеки именованные сущности и верни ТОЛЬКО JSON-массив:
[
  {"mention": "как в тексте", "canonical": "именительный падеж, полная форма",
   "type": "person|organization|location|event", "importance": "subject|key|mention"}
]

ТИПЫ:
- person: реальные люди по именам (текущие, исторические, литературные).
- organization: компании, ведомства, СМИ, военные формирования, политические партии,
  а также мессенджеры/сервисы/приложения с корпоративной identity.
  Примеры: Сбербанк, ВСУ, ООН, ТАСС, Telegram, Facebook, Сбербанк Онлайн, Windows 11.
- location: страны, города, регионы, географические объекты, аэропорты, вокзалы, мосты,
  именованные здания. Примеры: Москва, Россия, Тверская улица, Внуково, Кремль.
- event: ИМЕНОВАННЫЕ time-bounded события с собственным названием.
  Примеры: ПМЭФ, IPO Сбербанка, Парад Победы 2026, Олимпиада 2024.
  НЕ event: generic удары/атаки/обстрелы без названия, военные операции без имени собственного.

ВАЖНОСТЬ (importance) — насколько сущность центральна для статьи:
- subject: статья ПРО эту сущность. 1-2 на статью, иногда 0.
- key: активно участвует в сюжете, упомянута неоднократно. 2-5 на статью.
- mention: фон, контекст, перечисление. Большинство сущностей именно такие.

КАНОНИЧЕСКИЕ ФОРМЫ — строго использовать ИМЕННО эти названия для канона:
- Страны: Россия (не РФ/Российская Федерация), США (не Соединённые Штаты),
  Китай (не КНР), Северная Корея (не КНДР), Великобритания, Германия, Иран,
  Турция, Индия, Япония, Франция, Италия, Испания, Польша, Украина, Беларусь.
- Объединения: ЕС → Европейский союз, СНГ → Содружество Независимых Государств,
  НАТО (так и оставить), ООН (так и оставить).
- Военные: ВСУ → Вооружённые силы Украины, ВС РФ → Вооружённые силы России,
  ПВО (так и оставить, без расшифровки).
- Для остальных аббревиатур — расшифровывай если уверен.

НЕ ИЗВЛЕКАЙ:
- Военная техника, оружие, БПЛА, ракеты, дроны, бронетехника, истребители, корабли.
  Примеры что НЕ извлекать: Герань-2, Шахед, Калибр, Искандер, HIMARS, Patriot,
  Bayraktar, Leopard 2, F-16. Это инструменты, не сущности.
  Если статья про атаку с использованием БПЛА — извлекай ЦЕЛЬ удара
  (location/organization), а не само оружие.
- Физические товары и конкретные модели: iPhone 15, Lada Granta, Sukhoi Superjet 100.
  Если статья ПРО них — извлекай производителя.
- Должности без имени (Президент, Министр), национальности (украинец, россиянин),
  профессии, generic-слова.
- Generic события без названия: "встреча", "переговоры", "санкции", "атака",
  "обстрел", "удар". Только конкретные именованные события.

ОСОБЫЕ ПРАВИЛА:
- Тикеры ($SBER, $GMKN, $YNDX) → type=organization, canonical=полное имя компании
  (Сбербанк, Норникель, Яндекс).
- Сервисы/приложения/мессенджеры — всегда type=organization.
- Для людей: canonical = полное имя, известное публично. Если в тексте только
  фамилия "Стубб" — canonical "Александр Стубб". Только если уверен.
- Дубли недопустимы: разные написания одной сущности (с ё/е, разные падежи,
  с/без сокращения, разные формы названия) = ОДНА запись с самой полной canonical.
  В canonical всегда писать через "ё" если буква там есть (Соёмов, не Соемов).
- Если нет именованных сущностей — верни [].

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
            # SYSTEM_PROMPT идёт отдельным сообщением — он стабильный, кешируется
            # на стороне DeepSeek; переменная часть в user, она и так уникальна.
            response = await self.client.chat.completions.create(
                model=self.model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": f"ТЕКСТ:\n{text}"},
                ],
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
