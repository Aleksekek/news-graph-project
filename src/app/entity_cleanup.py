"""
Модуль LLM-очистки сущностей.

Используется планировщиком NER-сервиса (автоматический еженедельный запуск)
и CLI-скриптом scripts/legacy/llm_entity_cleanup.py (ручной запуск с JSON-ревью).
"""

import asyncio
import json

from openai import AsyncOpenAI

from src.config.settings import settings
from src.database.pool import DatabasePoolManager
from src.utils.logging import get_logger

logger = get_logger("app.entity_cleanup")

# ─── Параметры ────────────────────────────────────────────────────────────────

REFERENCE_TOP_N = 50
BATCH_SIZE = 80
REFERENCE_WINDOW_STEP = 50
TAIL_WINDOW_STEP = 50
MIN_COUNT_DEFAULT = 3
LLM_MAX_TOKENS = 4096
LLM_TEMPERATURE = 0.2
MAX_CONCURRENT = 8
CHUNK_SIZE = 20
ENTITY_TYPES = ("person", "organization", "location")

_UPSERT_SQL = """
INSERT INTO entity_aliases (alias_name, alias_type, canonical_name, canonical_type)
VALUES ($1, $2, $3, $4)
ON CONFLICT (alias_name, COALESCE(alias_type, '')) DO NOTHING;
"""


# ─── БД ───────────────────────────────────────────────────────────────────────


async def fetch_entities(min_count: int = MIN_COUNT_DEFAULT, limit: int | None = None) -> list[dict]:
    async with DatabasePoolManager.connection() as conn:
        rows = await conn.fetch(
            """
            SELECT e.id,
                   e.normalized_name AS name,
                   e.type,
                   COUNT(DISTINCT ae.processed_article_id) AS count
            FROM   entities e
            JOIN   article_entities ae ON ae.entity_id = e.id
            GROUP  BY e.id, e.normalized_name, e.type
            HAVING COUNT(DISTINCT ae.processed_article_id) >= $1
            ORDER  BY e.type, 4 DESC
            """,
            min_count,
        )
    entities = [dict(r) for r in rows]
    if limit:
        entities = entities[:limit]
    return entities


async def count_unaliased_entities(min_count: int = MIN_COUNT_DEFAULT) -> int:
    """Количество сущностей, не охваченных entity_aliases — индикатор необходимости очистки."""
    async with DatabasePoolManager.connection() as conn:
        result = await conn.fetchval(
            """
            SELECT COUNT(*) FROM (
                SELECT e.id
                FROM   entities e
                JOIN   article_entities ae ON ae.entity_id = e.id
                WHERE  NOT EXISTS (
                    SELECT 1 FROM entity_aliases ea
                    WHERE lower(ea.alias_name) = lower(e.normalized_name)
                      AND (ea.alias_type = e.type OR ea.alias_type IS NULL)
                )
                GROUP  BY e.id
                HAVING COUNT(DISTINCT ae.processed_article_id) >= $1
            ) sub
            """,
            min_count,
        )
    return result or 0


async def apply_aliases_to_db(aliases: list[dict], type_fixes: list[dict]) -> int:
    """
    Применяет алиасы и исправления типов к entity_aliases.
    aliases имеют приоритет над type_fixes для одного и того же ключа.
    Не добавляет строки, где alias_name уже является canonical_name (защита от циклов).
    Не перезаписывает существующие алиасы.
    Возвращает количество применённых записей.
    """
    merged: dict[tuple, dict] = {}
    for fix in type_fixes:
        key = (fix["name"], fix["current_type"])
        merged[key] = {
            "alias_name":     fix["name"],
            "alias_type":     fix["current_type"],
            "canonical_name": fix["name"],
            "canonical_type": fix["correct_type"],
        }
    for a in aliases:
        key = (a["alias_name"], a["alias_type"])
        merged[key] = a

    if not merged:
        return 0

    async with DatabasePoolManager.connection() as conn:
        existing = await conn.fetch(
            "SELECT lower(canonical_name) AS cn, canonical_type AS ct "
            "FROM entity_aliases WHERE canonical_type != 'discard'"
        )
        existing_canonicals = {(r["cn"], r["ct"]) for r in existing}

        rows = [
            (v["alias_name"], v["alias_type"], v["canonical_name"], v["canonical_type"])
            for v in merged.values()
            if (v["alias_name"].lower(), v["alias_type"]) not in existing_canonicals
        ]
        if not rows:
            return 0
        await conn.executemany(_UPSERT_SQL, rows)
    return len(rows)


async def merge_entity_aliases() -> tuple[int, int]:
    """
    Объединяет существующие дубли в entities по таблице entity_aliases.
    Возвращает (merged_entities, links_moved).
    """
    async with DatabasePoolManager.connection() as conn:
        aliases = await conn.fetch(
            "SELECT alias_name, alias_type, canonical_name, canonical_type FROM entity_aliases"
        )
        total_merged = 0
        total_links = 0

        for alias in aliases:
            alias_name     = alias["alias_name"]
            alias_type     = alias["alias_type"]
            canonical_name = alias["canonical_name"]
            canonical_type = alias["canonical_type"]

            if canonical_type == "discard":
                continue

            if alias_type:
                alias_entities = await conn.fetch(
                    "SELECT id FROM entities WHERE normalized_name = $1 AND type = $2",
                    alias_name, alias_type,
                )
            else:
                alias_entities = await conn.fetch(
                    "SELECT id FROM entities WHERE normalized_name = $1",
                    alias_name,
                )

            for ae in alias_entities:
                alias_id = ae["id"]
                canonical = await conn.fetchrow(
                    "SELECT id FROM entities WHERE normalized_name = $1 AND type = $2",
                    canonical_name, canonical_type,
                )
                if canonical is None:
                    canonical = await conn.fetchrow(
                        """
                        INSERT INTO entities (normalized_name, type, original_name)
                        VALUES ($1, $2, $1)
                        ON CONFLICT (normalized_name, type) DO UPDATE
                            SET normalized_name = entities.normalized_name
                        RETURNING id
                        """,
                        canonical_name, canonical_type,
                    )

                canonical_id = canonical["id"]
                if canonical_id == alias_id:
                    continue

                link_count = await conn.fetchval(
                    "SELECT COUNT(*) FROM article_entities WHERE entity_id = $1", alias_id
                )
                await conn.execute(
                    """
                    INSERT INTO article_entities
                           (processed_article_id, entity_id, count, importance_score, context_snippet)
                    SELECT  processed_article_id, $2,        count, importance_score, context_snippet
                    FROM    article_entities
                    WHERE   entity_id = $1
                    ON CONFLICT (processed_article_id, entity_id) DO NOTHING
                    """,
                    alias_id, canonical_id,
                )
                await conn.execute("DELETE FROM article_entities WHERE entity_id = $1", alias_id)
                await conn.execute("DELETE FROM entities WHERE id = $1", alias_id)

                total_merged += 1
                total_links += link_count

    return total_merged, total_links


# ─── Батчинг ──────────────────────────────────────────────────────────────────


def split_reference_and_tail(entities: list[dict]) -> tuple[list[dict], list[dict]]:
    reference: list[dict] = []
    for etype in ENTITY_TYPES:
        subset = [e for e in entities if e["type"] == etype]
        reference.extend(subset[:REFERENCE_TOP_N])
    ref_ids = {e["id"] for e in reference}
    tail = [e for e in entities if e["id"] not in ref_ids]
    return reference, tail


def sort_tail_by_last_token(tail: list[dict]) -> list[dict]:
    def key(e: dict) -> tuple[str, str]:
        tokens = e["name"].strip().split()
        last = tokens[-1].lower() if tokens else e["name"].lower()
        return (last, e["name"].lower())
    return sorted(tail, key=key)


def sliding_window_batches(items: list[dict], size: int, step: int) -> list[list[dict]]:
    if len(items) <= size:
        return [items]
    batches, i = [], 0
    while i < len(items):
        batches.append(items[i : i + size])
        if i + size >= len(items):
            break
        i += step
    return batches


# ─── Промпт ───────────────────────────────────────────────────────────────────


def _entities_json(entities: list[dict]) -> str:
    lines = [
        f'  {{"name": {e["name"]!r}, "type": "{e["type"]}", "count": {e["count"]}}}'
        for e in entities
    ]
    return "[\n" + ",\n".join(lines) + "\n]"


def build_prompt(batch: list[dict], reference: list[dict] | None) -> str:
    ref_block = ""
    if reference:
        ref_block = (
            "КАНОНИЧЕСКИЕ ЭТАЛОННЫЕ СУЩНОСТИ"
            " (нормализуй к ним, если подходит; выбирай полное официальное имя):\n"
            + _entities_json(reference)
            + "\n\n"
        )

    return f"""Ты анализируешь именованные сущности, извлечённые NER-системой из российских новостей.

{ref_block}СУЩНОСТИ ДЛЯ АНАЛИЗА:
{_entities_json(batch)}

Найди проблемы и верни ТОЛЬКО JSON без пояснений и markdown.

Правила:
- aliases: сущность является вариантом или формой другой сущности.
  Предпочитай полное официальное название как каноническое.
  НЕ создавай алиасы для финансовых терминов (Лонг, Шорт, Позиция, Рост, Падение и т.п.) —
  это не имена людей и не организации, их тип будет исправлен через type_fixes или discard.
- type_fixes: тип явно неверный (компания помечена как location, финансовый термин помечен
  как person и т.п.). Сюда же — финансовые термины с неверным типом.
- discard: явный мусор — цифры, URL, обрывки текста,
  случайные символы, отдельные буквы.
- Если сущность в порядке — не включай её в ответ вообще.

Доступные типы сущностей: person, location, organization

{{
  "aliases": [
    {{"alias_name": "...", "alias_type": "person|organization|location",
      "canonical_name": "...", "canonical_type": "person|organization|location"}}
  ],
  "type_fixes": [
    {{"name": "...", "current_type": "...", "correct_type": "..."}}
  ],
  "discard": [
    {{"name": "...", "type": "..."}}
  ]
}}"""


# ─── LLM ──────────────────────────────────────────────────────────────────────


def _parse_llm_response(text: str) -> dict:
    text = text.strip()
    for prefix in ("```json", "```"):
        if text.startswith(prefix):
            text = text[len(prefix):]
    if text.endswith("```"):
        text = text[:-3]
    return json.loads(text.strip())


async def call_llm(
    client: AsyncOpenAI, sem: asyncio.Semaphore, model: str, prompt: str
) -> tuple[dict | None, int]:
    async with sem:
        total_tokens = 0
        for attempt in range(2):
            try:
                response = await client.chat.completions.create(
                    model=model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=LLM_TEMPERATURE,
                    max_tokens=LLM_MAX_TOKENS,
                    timeout=120.0,
                    extra_body={"thinking": {"type": "disabled"}},
                )
                text = response.choices[0].message.content
                total_tokens += response.usage.total_tokens if response.usage else 0
                return _parse_llm_response(text), total_tokens
            except json.JSONDecodeError as e:
                logger.warning(f"JSON parse error (попытка {attempt + 1}/2): {e}")
                if attempt == 0:
                    await asyncio.sleep(1.0)
            except Exception as e:
                logger.error(f"LLM error: {e}")
                return None, total_tokens
        return None, total_tokens


def _make_client() -> AsyncOpenAI:
    return AsyncOpenAI(
        api_key=settings.DEEPSEEK_API_KEY,
        base_url="https://api.deepseek.com",
    )


# ─── Основная функция очистки ─────────────────────────────────────────────────


async def run_full_cleanup(
    min_count: int = MIN_COUNT_DEFAULT,
    limit: int | None = None,
    max_batches: int | None = None,
    auto_merge: bool = True,
    model: str = "deepseek-v4-pro",
    on_chunk_done: object = None,  # callable(done, total) | None
) -> dict:
    """
    Полный цикл LLM-очистки: fetch → LLM-батчинг → apply → merge.

    Используется планировщиком NER-сервиса. Для ручного запуска с JSON-ревью
    используйте scripts/legacy/llm_entity_cleanup.py.

    Возвращает словарь статистики.
    """
    entities = await fetch_entities(min_count=min_count, limit=limit)
    logger.info(f"[cleanup] Загружено {len(entities)} сущностей")

    if not entities:
        logger.info("[cleanup] Нет сущностей для обработки, пропускаем")
        return {"skipped": True, "reason": "no_entities"}

    reference, tail = split_reference_and_tail(entities)
    ref_batches = sliding_window_batches(reference, BATCH_SIZE, REFERENCE_WINDOW_STEP)
    tail_batches = sliding_window_batches(sort_tail_by_last_token(tail), BATCH_SIZE, TAIL_WINDOW_STEP)

    if max_batches is not None:
        ref_batches  = ref_batches[:max_batches]
        tail_batches = tail_batches[:max(0, max_batches - len(ref_batches))]

    all_batches: list[tuple[list[dict], list[dict] | None]] = (
        [(b, None) for b in ref_batches] + [(b, reference) for b in tail_batches]
    )
    total = len(all_batches)
    logger.info(f"[cleanup] Батчей: ref={len(ref_batches)}, tail={len(tail_batches)}")

    client = _make_client()
    sem = asyncio.Semaphore(MAX_CONCURRENT)

    all_aliases:    list[dict] = []
    all_type_fixes: list[dict] = []
    all_discards:   list[dict] = []
    total_tokens = 0
    errors = 0

    for chunk_start in range(0, total, CHUNK_SIZE):
        chunk = all_batches[chunk_start : chunk_start + CHUNK_SIZE]
        tasks = [
            call_llm(client, sem, model, build_prompt(batch, ref))
            for batch, ref in chunk
        ]
        chunk_results = await asyncio.gather(*tasks)

        chunk_ok = 0
        for result, tokens in chunk_results:
            total_tokens += tokens
            if result:
                all_aliases.extend(result.get("aliases", []))
                all_type_fixes.extend(result.get("type_fixes", []))
                all_discards.extend(result.get("discard", []))
                chunk_ok += 1
            else:
                errors += 1

        done = chunk_start + len(chunk)
        logger.info(f"[cleanup] [{done}/{total}] чанк: {chunk_ok}/{len(chunk)} успешно")
        if callable(on_chunk_done):
            on_chunk_done(done, total)

    # Дедупликация
    dedup_aliases = list({(a["alias_name"], a["alias_type"]): a for a in all_aliases}.values())
    dedup_fixes   = list({(f["name"], f["current_type"]): f for f in all_type_fixes}.values())
    dedup_discards = list(
        {(d.get("name", ""), d.get("type", "")): d for d in all_discards if d.get("name")}.values()
    )

    applied = await apply_aliases_to_db(dedup_aliases, dedup_fixes)
    logger.info(f"[cleanup] Применено алиасов: {applied}, ошибок LLM: {errors}")

    merged, links = 0, 0
    if auto_merge:
        merged, links = await merge_entity_aliases()
        logger.info(f"[cleanup] Слито сущностей: {merged}, связей перенесено: {links}")

    return {
        "entities_processed": len(entities),
        "aliases":    len(dedup_aliases),
        "type_fixes": len(dedup_fixes),
        "discards":   len(dedup_discards),
        "applied":    applied,
        "tokens":     total_tokens,
        "errors":     errors,
        "merged":     merged,
        "links_moved": links,
    }
