"""
Diagnostic-скрипт качества NER.

Берёт случайную выборку из raw_articles, прогоняет через Natasha-пайплайн
и категоризирует извлечённые сущности по бакетам типичных ошибок:

  A_ticker        — `$SBER`, `{$XYZ}`, `Сбер $`           (TInvest-noise)
  A_emoji         — эмодзи в имени
  A_html_artifact — `<>`, `{}` в имени
  A_caps_latin    — `ETF_PRO`, `MAX` (латиница капсом — обычно канал/тег)
  A_punct_edge    — пунктуация на границе ('Сбер.', '«Газпром')
  B_too_short     — длина <3 (без пробелов)
  B_non_noun_pos  — span целиком VERB/ADV/CONJ/PART (не должен быть сущностью)
  B_lower_case    — PER/ORG в нижнем регистре (подозрительно)
  C_oblique_case  — Cyrillic токен в косвенном падеже, span.normalize не справился
  D_partial_match — короткое имя ⊂ длинного ('Илия' ⊂ 'Илия Муромец')

Не имеет побочных эффектов на БД. Re-runnable: можно гонять до и после правок,
сравнивать JSON-дампы.

Использование:
    python scripts/legacy/ner_diagnostic.py                 # 100 статей × 5 источников
    python scripts/legacy/ner_diagnostic.py --sample 200    # 200 статей с источника
    python scripts/legacy/ner_diagnostic.py --source tinvest --sample 300
    python scripts/legacy/ner_diagnostic.py --json out.json # дамп в JSON для diff
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from natasha import (
    Doc,
    MorphVocab,
    NewsEmbedding,
    NewsMorphTagger,
    NewsNERTagger,
    Segmenter,
)

from src.database.pool import DatabasePoolManager
from src.processing.ner.text_cleaner import clean_article_text
from src.utils.logging import get_logger

logger = get_logger("ner_diagnostic")

# ── Категоризация ─────────────────────────────────────────────────────────────

_TYPE_MAP = {"PER": "person", "LOC": "location", "ORG": "organization"}

_TICKER_RE = re.compile(r"\$[A-Z][A-Z0-9]{1,}|\{\$[^}]+\}|\$\s*$")
_EMOJI_RE = re.compile(
    r"[\U0001F000-\U0001FAFF"
    r"\U00002600-\U000027BF"
    r"\U0001F300-\U0001F9FF"
    r"\U0001FA70-\U0001FAFF]"
)
_HTML_ARTIFACT_RE = re.compile(r"[<>{}]")
_ALL_CAPS_LATIN_RE = re.compile(r"^[A-Z][A-Z0-9_]+$")  # ETF_PRO, MAX
_PUNCT_EDGE_RE = re.compile(r"^[^\w«]|[^\w»]$")  # punct в начале/конце (кроме « »)
_HAS_CYRILLIC_RE = re.compile(r"[А-Яа-яЁё]")

# POS, которые точно не могут быть основой именованной сущности
_NON_ENTITY_POS = {"VERB", "ADV", "ADP", "CONJ", "PART", "SCONJ", "CCONJ", "INTJ"}


@dataclass
class Example:
    name: str
    type: str
    note: str = ""


@dataclass
class Stats:
    total_articles: int = 0
    total_entities: int = 0
    by_type: Counter = field(default_factory=Counter)
    by_source: Counter = field(default_factory=Counter)
    bucket_counts: Counter = field(default_factory=Counter)
    bucket_examples: dict[str, list[Example]] = field(
        default_factory=lambda: defaultdict(list)
    )
    per_source_buckets: dict[str, Counter] = field(
        default_factory=lambda: defaultdict(Counter)
    )

    def add_bucket(
        self, bucket: str, source: str, name: str, type_: str, note: str = ""
    ) -> None:
        self.bucket_counts[bucket] += 1
        self.per_source_buckets[source][bucket] += 1
        if len(self.bucket_examples[bucket]) < 10:
            self.bucket_examples[bucket].append(Example(name, type_, note))


# ── Natasha (lazy init) ───────────────────────────────────────────────────────

_segmenter: Segmenter | None = None
_morph_vocab: MorphVocab | None = None
_morph_tagger: NewsMorphTagger | None = None
_ner_tagger: NewsNERTagger | None = None


def init_natasha() -> None:
    global _segmenter, _morph_vocab, _morph_tagger, _ner_tagger
    if _segmenter is not None:
        return
    emb = NewsEmbedding()
    _segmenter = Segmenter()
    _morph_vocab = MorphVocab()
    _morph_tagger = NewsMorphTagger(emb)
    _ner_tagger = NewsNERTagger(emb)


# ── Извлечение и категоризация ────────────────────────────────────────────────


def _span_tokens(doc: Doc, span) -> list:
    return [t for t in doc.tokens if span.start <= t.start < span.stop]


def categorize_article(
    title: str, text: str, source: str, stats: Stats, all_entities: set
) -> None:
    combined = f"{title}\n\n{text}" if text else title
    if not combined.strip():
        return

    doc = Doc(combined)
    doc.segment(_segmenter)
    doc.tag_morph(_morph_tagger)
    for token in doc.tokens:
        token.lemmatize(_morph_vocab)
    doc.tag_ner(_ner_tagger)
    for span in doc.spans:
        span.normalize(_morph_vocab)

    seen_local: set[tuple[str, str]] = set()

    for span in doc.spans:
        if span.type not in _TYPE_MAP:
            continue
        ent_type = _TYPE_MAP[span.type]
        normalized = (span.normal or span.text).strip()
        if not normalized:
            continue

        key = (normalized, ent_type)
        if key in seen_local:
            continue
        seen_local.add(key)
        all_entities.add(key)

        stats.total_entities += 1
        stats.by_type[ent_type] += 1
        stats.by_source[source] += 1

        # Bucket A — source-noise / поверхностный мусор
        if _TICKER_RE.search(normalized):
            stats.add_bucket("A_ticker", source, normalized, ent_type)
        if _EMOJI_RE.search(normalized):
            stats.add_bucket("A_emoji", source, normalized, ent_type)
        if _HTML_ARTIFACT_RE.search(normalized):
            stats.add_bucket("A_html_artifact", source, normalized, ent_type)
        if _ALL_CAPS_LATIN_RE.match(normalized):
            stats.add_bucket("A_caps_latin", source, normalized, ent_type)
        if _PUNCT_EDGE_RE.search(normalized):
            stats.add_bucket("A_punct_edge", source, normalized, ent_type)

        # Bucket B — структурная аномалия
        compact = normalized.replace(" ", "")
        if len(compact) < 3:
            stats.add_bucket("B_too_short", source, normalized, ent_type)

        if normalized.islower() and _HAS_CYRILLIC_RE.search(normalized):
            stats.add_bucket("B_lower_case", source, normalized, ent_type)

        span_toks = _span_tokens(doc, span)
        if span_toks:
            pos_set = {t.pos for t in span_toks if getattr(t, "pos", None)}
            if pos_set and pos_set.issubset(_NON_ENTITY_POS):
                stats.add_bucket(
                    "B_non_noun_pos",
                    source,
                    normalized,
                    ent_type,
                    note=f"POS={sorted(pos_set)}",
                )

        # Bucket C — span.normalize не сработал, остался в косвенном падеже
        if normalized == span.text and span_toks:
            for tok in span_toks:
                feats = getattr(tok, "feats", None) or {}
                case = feats.get("Case")
                if case and case != "Nom":
                    lemma = getattr(tok, "lemma", "") or ""
                    if lemma and lemma.lower() != tok.text.lower():
                        stats.add_bucket(
                            "C_oblique_case",
                            source,
                            normalized,
                            ent_type,
                            note=f"Case={case}, lemma='{lemma}'",
                        )
                        break


def find_partial_matches(all_entities: set) -> list[tuple[str, str, str]]:
    """Bucket D: короткое имя ⊂ длинного с границей слова (внутри одного типа)."""
    by_type: dict[str, list[str]] = defaultdict(list)
    for name, etype in all_entities:
        by_type[etype].append(name)

    matches: list[tuple[str, str, str]] = []
    for etype, names in by_type.items():
        names = sorted(set(names), key=len)
        # Сравнение каждое-с-каждым O(n²); ограничим O(больших) для скорости
        if len(names) > 2000:
            names = names[:2000]
        names_set = set(names)
        for short in names:
            if " " in short:  # ищем только односложные подстроки
                continue
            for long in names_set:
                if short == long:
                    continue
                if (
                    long.startswith(short + " ")
                    or long.endswith(" " + short)
                    or f" {short} " in long
                ):
                    matches.append((short, long, etype))
                    if len(matches) >= 200:
                        return matches
    return matches


# ── Получение выборки ─────────────────────────────────────────────────────────


async def fetch_sample(n_per_source: int, source_filter: str | None) -> list[dict]:
    """
    Случайная выборка статей с raw_text >= 200 символов.
    Возвращает в формате [{id, raw_title, raw_text, source_name}, ...].
    """
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


# ── Отчёт ─────────────────────────────────────────────────────────────────────


def print_report(stats: Stats, partials: list[tuple[str, str, str]]) -> None:
    print()
    print("=" * 70)
    print("  NER QUALITY DIAGNOSTIC")
    print("=" * 70)
    print(f"\nArticles processed: {stats.total_articles}")
    print(f"Total unique entities (per-article dedup): {stats.total_entities}")

    print("\nBy type:")
    for t, c in stats.by_type.most_common():
        print(f"  {t:14s} {c:>6}")

    print("\nBy source:")
    for s, c in stats.by_source.most_common():
        print(f"  {s:14s} {c:>6}")

    print("\n" + "=" * 70)
    print("BUCKETS (one entity may fall into multiple)")
    print("=" * 70)
    for bucket, count in stats.bucket_counts.most_common():
        pct = (count / stats.total_entities * 100) if stats.total_entities else 0.0
        print(f"\n[{bucket}] {count} entities ({pct:.1f}%)")
        for ex in stats.bucket_examples[bucket]:
            extra = f"  — {ex.note}" if ex.note else ""
            print(f"    {ex.name!r:42s} ({ex.type}){extra}")

    print("\n" + "=" * 70)
    print("PER-SOURCE × BUCKET")
    print("=" * 70)
    for source in sorted(stats.per_source_buckets):
        total_src = stats.by_source[source]
        print(f"\n{source} (total: {total_src}):")
        for bucket, count in stats.per_source_buckets[source].most_common():
            pct = (count / total_src * 100) if total_src else 0.0
            print(f"  {bucket:25s} {count:>5}  ({pct:.1f}%)")

    print("\n" + "=" * 70)
    print(f"BUCKET D: PARTIAL-NAME MATCHES (sample of {len(partials)})")
    print("=" * 70)
    for short, long, t in partials[:25]:
        print(f"  {short!r:30s} ⊂ {long!r:50s} ({t})")


def stats_to_json(stats: Stats, partials: list[tuple[str, str, str]]) -> dict:
    return {
        "total_articles": stats.total_articles,
        "total_entities": stats.total_entities,
        "by_type": dict(stats.by_type),
        "by_source": dict(stats.by_source),
        "bucket_counts": dict(stats.bucket_counts),
        "per_source_buckets": {
            s: dict(c) for s, c in stats.per_source_buckets.items()
        },
        "bucket_examples": {
            b: [{"name": e.name, "type": e.type, "note": e.note} for e in exs]
            for b, exs in stats.bucket_examples.items()
        },
        "partial_matches_count": len(partials),
        "partial_matches_sample": [
            {"short": s, "long": l, "type": t} for s, l, t in partials[:50]
        ],
    }


# ── Точка входа ───────────────────────────────────────────────────────────────


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sample", type=int, default=100, help="Articles per source (default: 100)"
    )
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        help="Only this source (lenta|tinvest|interfax|tass|rbc)",
    )
    parser.add_argument(
        "--json", type=str, default=None, help="Dump report to JSON file"
    )
    args = parser.parse_args()

    print("Initializing Natasha (this loads ~250MB of embeddings)...")
    init_natasha()

    print(f"Fetching up to {args.sample} articles per source...")
    articles = await fetch_sample(args.sample, args.source)
    print(f"Got {len(articles)} articles")

    if not articles:
        print("No articles to process. Exiting.")
        return

    stats = Stats()
    stats.total_articles = len(articles)
    all_entities: set[tuple[str, str]] = set()

    for i, a in enumerate(articles, 1):
        if i % 50 == 0:
            print(f"  ... processed {i}/{len(articles)}")
        try:
            title, text = clean_article_text(a.get("raw_title", ""), a.get("raw_text", ""))
            categorize_article(title, text, a["source_name"], stats, all_entities)
        except Exception as e:
            logger.error(f"Error on article id={a.get('id')}: {e}")

    print("Looking for partial-name matches...")
    partials = find_partial_matches(all_entities)

    print_report(stats, partials)

    if args.json:
        with open(args.json, "w", encoding="utf-8") as f:
            json.dump(stats_to_json(stats, partials), f, ensure_ascii=False, indent=2)
        print(f"\nJSON dump written to {args.json}")


if __name__ == "__main__":
    asyncio.run(main())
