"""
Smoke test для LLM NER через DeepSeek.

Делает реальные API-вызовы. Не пишет в БД, только читает raw_articles.
Скипается, если DEEPSEEK_API_KEY не задан.

Один большой тест (не несколько) — у asyncpg-пула проблемы с переключением
event loop между function-scoped pytest-asyncio тестами.

Запуск:
    pytest tests/integration/test_ner_llm_smoke.py -v -m ""
"""

from collections import Counter

import pytest

from src.config.settings import settings
from src.database.pool import DatabasePoolManager
from src.processing.ner.text_cleaner import clean_article_text

_VALID_TYPES = {"person", "organization", "location", "event"}
_VALID_SCORES = {1.0, 0.7, 0.3}


async def _fetch_sample(n: int = 5) -> list[dict]:
    sql = """
        SELECT ra.id AS article_id, ra.raw_title, ra.raw_text, s.name AS source_name
        FROM raw_articles ra
        JOIN sources s ON s.id = ra.source_id
        WHERE ra.raw_text IS NOT NULL AND length(ra.raw_text) >= 200
        ORDER BY random()
        LIMIT $1
    """
    async with DatabasePoolManager.connection() as conn:
        rows = await conn.fetch(sql, n)
    return [dict(r) for r in rows]


@pytest.mark.integration
@pytest.mark.asyncio
@pytest.mark.skipif(not settings.DEEPSEEK_API_KEY, reason="DEEPSEEK_API_KEY not set in settings")
async def test_llm_ner_smoke():
    """
    End-to-end smoke check для LLMNERClient на 5 случайных статьях.
    Объединены проверки: shape, non-empty, no-dups, importance distribution.
    """
    from src.processing.ner.llm_client import LLMNERClient

    articles = await _fetch_sample(5)
    if not articles:
        pytest.skip("Нет статей в БД с текстом")

    client = LLMNERClient()
    importance_counts: Counter = Counter()
    total_entities = 0
    articles_with_entities = 0

    for art in articles:
        title, text = clean_article_text(
            art["raw_title"], art["raw_text"], source=art["source_name"]
        )
        entities = await client.extract(title, text)
        assert isinstance(entities, list)

        if entities:
            articles_with_entities += 1

        # Каждая сущность валидна
        keys = []
        for e in entities:
            assert e.normalized_name, f"Пустой canonical в #{art['article_id']}"
            assert e.entity_type in _VALID_TYPES, (
                f"Неизвестный type={e.entity_type!r} в #{art['article_id']}"
            )
            assert e.importance_score in _VALID_SCORES, (
                f"Неожиданный importance_score={e.importance_score}"
            )
            assert e.count >= 1
            keys.append((e.normalized_name.lower(), e.entity_type))
            importance_counts[e.importance_score] += 1
            total_entities += 1

        # Никаких дублей внутри статьи
        assert len(keys) == len(set(keys)), (
            f"Дубли в #{art['article_id']}: {[k for k in keys if keys.count(k) > 1]}"
        )

    # Sanity: хотя бы одна статья отдала сущности
    assert articles_with_entities > 0, (
        "DeepSeek не вернул сущности ни на одной из 5 статей"
    )

    # Sanity: распределение важности разумное
    if total_entities > 0:
        subj = importance_counts.get(1.0, 0)
        men = importance_counts.get(0.3, 0)
        assert men > 0, (
            f"Ни одной mention. importance counts: {dict(importance_counts)}"
        )
        assert subj / total_entities < 0.6, (
            f"Слишком много subjects ({subj}/{total_entities})"
        )

    # Print summary для дебаг-вывода (--capture=no / -s)
    print(
        f"\nSmoke: {len(articles)} articles, "
        f"{total_entities} entities, "
        f"importance: subject={importance_counts.get(1.0, 0)} "
        f"key={importance_counts.get(0.7, 0)} mention={importance_counts.get(0.3, 0)}"
    )
