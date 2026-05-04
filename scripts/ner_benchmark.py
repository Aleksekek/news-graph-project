"""
Бенчмарк качества NER: Natasha vs DeepSeek.

Берёт N статей, прогоняет через оба движка, выводит side-by-side для визуальной
оценки качества. Не имеет побочных эффектов на БД.

Использование:
    python scripts/ner_benchmark.py                 # 4 статьи × 5 источников = 20
    python scripts/ner_benchmark.py --sample 6      # 6 на источник
    python scripts/ner_benchmark.py --source tinvest --sample 10
    python scripts/ner_benchmark.py --json out.json # дамп для последующего анализа
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from openai import AsyncOpenAI

from src.config.settings import settings
from src.database.pool import DatabasePoolManager
from src.processing.ner.natasha_client import NatashaClient
from src.processing.ner.text_cleaner import clean_article_text
from src.utils.logging import get_logger

logger = get_logger("ner_benchmark")

DEEPSEEK_MODEL = "deepseek-v4-flash"
DEEPSEEK_CONCURRENCY = 5
MAX_TEXT_CHARS = 8000  # защита от слишком длинных статей в промпт

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

# Mapping для backward compat с importance_score (Natasha shкала)
IMPORTANCE_TO_SCORE = {"subject": 1.0, "key": 0.7, "mention": 0.3}
VALID_IMPORTANCES = set(IMPORTANCE_TO_SCORE.keys())


# ── DeepSeek NER ──────────────────────────────────────────────────────────────


def _parse_json_response(text: str) -> list[dict]:
    text = text.strip()
    for prefix in ("```json", "```"):
        if text.startswith(prefix):
            text = text[len(prefix):]
    if text.endswith("```"):
        text = text[:-3]
    text = text.strip()
    try:
        data = json.loads(text)
        if not isinstance(data, list):
            return []
        out = []
        for e in data:
            if not isinstance(e, dict):
                continue
            if "canonical" not in e or "type" not in e:
                continue
            if e["type"] not in {"person", "organization", "location", "event"}:
                continue
            # mention опционален; если не вернули — берём canonical
            e.setdefault("mention", e["canonical"])
            # importance опционален; невалидное значение → mention (самый "безопасный" дефолт)
            imp = e.get("importance")
            if imp not in VALID_IMPORTANCES:
                e["importance"] = "mention"
            out.append(e)
        return out
    except json.JSONDecodeError as e:
        logger.warning(f"JSON parse error: {e}; preview: {text[:200]!r}")
        return []


async def call_deepseek(
    client: AsyncOpenAI, sem: asyncio.Semaphore, text: str
) -> tuple[list[dict], int]:
    """Возвращает (entities, total_tokens). При ошибке — ([], 0)."""
    text = text[:MAX_TEXT_CHARS]
    prompt = PROMPT_TEMPLATE.format(text=text)
    async with sem:
        try:
            response = await client.chat.completions.create(
                model=DEEPSEEK_MODEL,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.0,
                max_tokens=2000,
                timeout=60.0,
                extra_body={"thinking": {"type": "disabled"}},
            )
            content = response.choices[0].message.content
            tokens = response.usage.total_tokens if response.usage else 0
            return _parse_json_response(content), tokens
        except Exception as e:
            logger.error(f"DeepSeek call error: {e}")
            return [], 0


# ── Natasha (адаптер под общий формат) ────────────────────────────────────────


def _score_to_importance(score: float) -> str:
    """1.0→subject, 0.7→key, 0.3→mention (Natasha shкала)."""
    if score >= 0.9:
        return "subject"
    if score >= 0.5:
        return "key"
    return "mention"


def natasha_extract(natasha: NatashaClient, title: str, text: str) -> list[dict]:
    entities = natasha.extract(title, text)
    return [
        {
            "mention": e.original_name,
            "canonical": e.normalized_name,
            "type": e.entity_type,
            "importance": _score_to_importance(e.importance_score),
        }
        for e in entities
    ]


# ── Получение выборки ─────────────────────────────────────────────────────────


async def fetch_sample(n_per_source: int, source_filter: str | None) -> list[dict]:
    sql = """
        SELECT id, raw_title, raw_text, source_name
        FROM (
            SELECT
                ra.id,
                ra.raw_title,
                ra.raw_text,
                s.name AS source_name,
                row_number() OVER (PARTITION BY ra.source_id ORDER BY random()) AS rn
            FROM raw_articles ra
            JOIN sources s ON s.id = ra.source_id
            WHERE ra.raw_text IS NOT NULL
              AND length(ra.raw_text) >= 200
              {source_clause}
        ) sub
        WHERE rn <= $1
        ORDER BY source_name, id
    """
    params: list = [n_per_source]
    if source_filter:
        sql = sql.format(source_clause="AND s.name = $2")
        params.append(source_filter)
    else:
        sql = sql.format(source_clause="")

    async with DatabasePoolManager.connection() as conn:
        rows = await conn.fetch(sql, *params)
    return [dict(r) for r in rows]


# ── Surface-noise детектор (тот же что в diagnostic) ──────────────────────────

_TICKER_RE = re.compile(r"\$[A-Z][A-Z0-9]{1,}|\{\$[^}]+\}|\$\s*$")
_EMOJI_RE = re.compile(
    r"[\U0001F000-\U0001FAFF"
    r"\U00002600-\U000027BF"
    r"\U0001F300-\U0001F9FF"
    r"\U0001FA70-\U0001FAFF]"
)
_HTML_ARTIFACT_RE = re.compile(r"[<>{}]")
_PUNCT_EDGE_RE = re.compile(r"^[^\w«]|[^\w»]$")


def is_surface_noise(name: str) -> str | None:
    if _TICKER_RE.search(name):
        return "ticker"
    if _EMOJI_RE.search(name):
        return "emoji"
    if _HTML_ARTIFACT_RE.search(name):
        return "html_artifact"
    if len(name.replace(" ", "")) < 3:
        return "too_short"
    if _PUNCT_EDGE_RE.search(name):
        return "punct_edge"
    return None


# ── Side-by-side вывод ────────────────────────────────────────────────────────


def _key(e: dict) -> tuple[str, str]:
    return (e["canonical"].strip().lower(), e["type"])


_IMP_GLYPH = {"subject": "★", "key": "▲", "mention": "·"}


def _format_entity_line(e: dict, match: str) -> str:
    glyph = _IMP_GLYPH.get(e.get("importance", "mention"), "·")
    noise = is_surface_noise(e["canonical"])
    noise_tag = f"  ⚠ {noise}" if noise else ""
    mention = e["mention"]
    if mention.lower() != e["canonical"].lower():
        return f"  {match} {glyph} {e['canonical']:40s} ({e['type']:12s}) ← {mention!r}{noise_tag}"
    return f"  {match} {glyph} {e['canonical']:40s} ({e['type']:12s}){noise_tag}"


def print_article_diff(article: dict, n_ents: list[dict], d_ents: list[dict]) -> None:
    print()
    print("=" * 90)
    print(f"#{article['id']}  [{article['source_name']}]")
    title = (article.get("raw_title") or "").strip()
    print(f"Title: {title[:120]}")
    print("=" * 90)
    print("Glyphs: ★ subject  ▲ key  · mention")

    n_keys = {_key(e) for e in n_ents}
    d_keys = {_key(e) for e in d_ents}

    # Сортировка: сначала по importance (subject первыми), потом по типу/canonical
    imp_order = {"subject": 0, "key": 1, "mention": 2}

    def sort_key(x: dict) -> tuple:
        return (imp_order.get(x.get("importance", "mention"), 3), x["type"], x["canonical"].lower())

    print(f"\nNATASHA ({len(n_ents)}):")
    if not n_ents:
        print("  (пусто)")
    for e in sorted(n_ents, key=sort_key):
        match = "🟰" if _key(e) in d_keys else "  "
        print(_format_entity_line(e, match))

    print(f"\nDEEPSEEK ({len(d_ents)}):")
    if not d_ents:
        print("  (пусто)")
    for e in sorted(d_ents, key=sort_key):
        match = "🟰" if _key(e) in n_keys else "  "
        print(_format_entity_line(e, match))


# ── Агрегатные метрики ────────────────────────────────────────────────────────


def aggregate_stats(
    articles: list[dict], n_results: list[list[dict]], d_results: list[list[dict]]
) -> dict:
    n_total = sum(len(r) for r in n_results)
    d_total = sum(len(r) for r in d_results)

    n_types = Counter(e["type"] for r in n_results for e in r)
    d_types = Counter(e["type"] for r in d_results for e in r)

    n_imp = Counter(e.get("importance", "mention") for r in n_results for e in r)
    d_imp = Counter(e.get("importance", "mention") for r in d_results for e in r)

    n_noise = sum(1 for r in n_results for e in r if is_surface_noise(e["canonical"]))
    d_noise = sum(1 for r in d_results for e in r if is_surface_noise(e["canonical"]))

    # Согласие: какая доля сущностей у Natasha совпадает с DeepSeek (по canonical+type)
    same_total = 0
    only_natasha = 0
    only_deepseek = 0
    for n_ents, d_ents in zip(n_results, d_results):
        n_keys = {_key(e) for e in n_ents}
        d_keys = {_key(e) for e in d_ents}
        same_total += len(n_keys & d_keys)
        only_natasha += len(n_keys - d_keys)
        only_deepseek += len(d_keys - n_keys)

    return {
        "articles": len(articles),
        "natasha_total": n_total,
        "deepseek_total": d_total,
        "natasha_per_article": n_total / len(articles) if articles else 0,
        "deepseek_per_article": d_total / len(articles) if articles else 0,
        "natasha_types": dict(n_types),
        "deepseek_types": dict(d_types),
        "natasha_importance": dict(n_imp),
        "deepseek_importance": dict(d_imp),
        "natasha_surface_noise": n_noise,
        "deepseek_surface_noise": d_noise,
        "agreement": {
            "same": same_total,
            "only_natasha": only_natasha,
            "only_deepseek": only_deepseek,
        },
    }


def print_aggregate(stats: dict, tokens: int, time_natasha: float, time_deepseek: float) -> None:
    print()
    print("=" * 90)
    print("AGGREGATE")
    print("=" * 90)
    print(f"\nArticles: {stats['articles']}")
    print(f"\nEntities extracted (per article avg):")
    print(f"  Natasha:  {stats['natasha_total']:>5}  ({stats['natasha_per_article']:.1f}/art)")
    print(f"  DeepSeek: {stats['deepseek_total']:>5}  ({stats['deepseek_per_article']:.1f}/art)")

    print("\nType distribution:")
    print(f"  {'type':14s} {'natasha':>10s} {'deepseek':>10s}")
    all_types = set(stats["natasha_types"]) | set(stats["deepseek_types"])
    for t in sorted(all_types):
        print(
            f"  {t:14s} "
            f"{stats['natasha_types'].get(t, 0):>10} "
            f"{stats['deepseek_types'].get(t, 0):>10}"
        )

    print("\nImportance distribution:")
    print(f"  {'importance':14s} {'natasha':>10s} {'deepseek':>10s}")
    for imp in ("subject", "key", "mention"):
        n_count = stats["natasha_importance"].get(imp, 0)
        d_count = stats["deepseek_importance"].get(imp, 0)
        n_pct = (n_count / stats["natasha_total"] * 100) if stats["natasha_total"] else 0
        d_pct = (d_count / stats["deepseek_total"] * 100) if stats["deepseek_total"] else 0
        print(f"  {imp:14s} {n_count:>5} ({n_pct:>4.1f}%) {d_count:>5} ({d_pct:>4.1f}%)")
    # sanity-check: avg subjects per article (>2 — подозрительно)
    n_subj_per = stats["natasha_importance"].get("subject", 0) / stats["articles"]
    d_subj_per = stats["deepseek_importance"].get("subject", 0) / stats["articles"]
    print(f"  Avg subjects/article — Natasha: {n_subj_per:.1f}  DeepSeek: {d_subj_per:.1f}")
    if d_subj_per > 2.5:
        print(f"  ⚠ DeepSeek объявляет слишком много subjects — стоит ужесточить промпт")

    print("\nSurface-noise (regex-detectable junk):")
    print(f"  Natasha:  {stats['natasha_surface_noise']}")
    print(f"  DeepSeek: {stats['deepseek_surface_noise']}")

    print("\nAgreement (canonical + type):")
    a = stats["agreement"]
    print(f"  Same in both:    {a['same']}")
    print(f"  Only in Natasha: {a['only_natasha']}")
    print(f"  Only in DeepSeek:{a['only_deepseek']}")

    print(f"\nTiming:")
    print(f"  Natasha:  {time_natasha:.1f}s")
    print(f"  DeepSeek: {time_deepseek:.1f}s ({DEEPSEEK_CONCURRENCY} concurrent)")

    # Цена приблизительная: для v4-flash около $0.07/M вход + $0.28/M выход;
    # средневзвешенно ~$0.10-0.15 per M total. Точную цену проверь у DeepSeek.
    cost_low = tokens * 0.07 / 1_000_000
    cost_high = tokens * 0.20 / 1_000_000
    print(f"\nDeepSeek tokens: {tokens}")
    print(f"  Approx cost: ${cost_low:.4f} - ${cost_high:.4f}")
    if stats["articles"]:
        print(f"  Per article: ~${(cost_low+cost_high)/2/stats['articles']:.5f}")


# ── Точка входа ───────────────────────────────────────────────────────────────


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--sample", type=int, default=4, help="Articles per source (default: 4)")
    parser.add_argument("--source", type=str, default=None)
    parser.add_argument("--json", type=str, default=None)
    args = parser.parse_args()

    if not settings.DEEPSEEK_API_KEY:
        print("ERROR: DEEPSEEK_API_KEY not set in settings / .env")
        return

    print("Initializing Natasha (~250MB embeddings)...")
    natasha = NatashaClient()

    print(f"Fetching {args.sample} articles per source...")
    articles = await fetch_sample(args.sample, args.source)
    print(f"Got {len(articles)} articles")
    if not articles:
        print("Nothing to benchmark.")
        return

    # Cleanup всех текстов один раз — с source-aware дейтлайн-стриппингом
    cleaned = []
    for a in articles:
        title, text = clean_article_text(
            a.get("raw_title", ""),
            a.get("raw_text", ""),
            source=a.get("source_name"),
        )
        cleaned.append((title, text))

    # Natasha (sync)
    print("\nRunning Natasha...")
    t0 = time.time()
    n_results = [natasha_extract(natasha, t, x) for t, x in cleaned]
    time_natasha = time.time() - t0

    # DeepSeek (async, parallel)
    print("Running DeepSeek...")
    deepseek_client = AsyncOpenAI(
        api_key=settings.DEEPSEEK_API_KEY,
        base_url="https://api.deepseek.com",
    )
    sem = asyncio.Semaphore(DEEPSEEK_CONCURRENCY)
    t0 = time.time()
    tasks = [
        call_deepseek(deepseek_client, sem, f"{title}\n\n{text}" if text else title)
        for title, text in cleaned
    ]
    raw_results = await asyncio.gather(*tasks)
    time_deepseek = time.time() - t0
    d_results = [r[0] for r in raw_results]
    total_tokens = sum(r[1] for r in raw_results)

    # Per-article diff
    for article, n_ents, d_ents in zip(articles, n_results, d_results):
        print_article_diff(article, n_ents, d_ents)

    # Aggregate
    stats = aggregate_stats(articles, n_results, d_results)
    print_aggregate(stats, total_tokens, time_natasha, time_deepseek)

    # JSON dump
    if args.json:
        out = {
            "stats": stats,
            "tokens": total_tokens,
            "time_natasha_sec": time_natasha,
            "time_deepseek_sec": time_deepseek,
            "articles": [
                {
                    "id": a["id"],
                    "source": a["source_name"],
                    "title": a.get("raw_title", ""),
                    "natasha": n,
                    "deepseek": d,
                }
                for a, n, d in zip(articles, n_results, d_results)
            ],
        }
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
        print(f"\nJSON dump → {args.json}")


if __name__ == "__main__":
    asyncio.run(main())
