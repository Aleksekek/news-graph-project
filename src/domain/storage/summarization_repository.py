import json
import logging
import random
from datetime import datetime
from typing import Dict, List, Optional

from src.domain.storage.database import DatabasePoolManager

logger = logging.getLogger(__name__)


class SummarizationRepository:
    """Репозиторий для работы с суммаризациями"""

    @staticmethod
    async def save_summary(
        period_start: datetime, period_end: datetime, period_type: str, content: Dict
    ) -> int:
        """Сохраняет суммаризацию в БД"""
        meta = content.pop("_meta", {})

        try:
            async with DatabasePoolManager.connection() as conn:
                result = await conn.fetchrow(
                    """
                    INSERT INTO summarizations (
                        period_start, period_end, period_type, content,
                        model_used, prompt_tokens, completion_tokens, cost_usd
                    ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                    ON CONFLICT (period_start, period_type) DO UPDATE SET
                        content = EXCLUDED.content,
                        model_used = EXCLUDED.model_used,
                        prompt_tokens = EXCLUDED.prompt_tokens,
                        completion_tokens = EXCLUDED.completion_tokens,
                        cost_usd = EXCLUDED.cost_usd
                    RETURNING id
                """,
                    period_start,
                    period_end,
                    period_type,
                    json.dumps(content),
                    meta.get("model"),
                    meta.get("prompt_tokens", 0),
                    meta.get("completion_tokens", 0),
                    meta.get("cost_usd", 0),
                )
                logger.info(
                    f"Суммаризация сохранена: id={result['id']}, тип={period_type}"
                )
                return result["id"]
        except Exception as e:
            logger.error(f"Ошибка сохранения суммаризации: {e}")
            return 0

    @staticmethod
    async def get_summaries_for_period(
        start: datetime, end: datetime, period_type: Optional[str] = None
    ) -> List[Dict]:
        """Получает суммаризации за период"""
        try:
            async with DatabasePoolManager.connection() as conn:
                query = """
                    SELECT id, period_start, period_end, period_type, content,
                        created_at, model_used, prompt_tokens, completion_tokens
                    FROM summarizations
                    WHERE period_start >= $1 AND period_end <= $2
                """
                params = [start, end]

                if period_type:
                    query += " AND period_type = $3"
                    params.append(period_type)

                query += " ORDER BY period_start ASC"

                rows = await conn.fetch(query, *params)

                import json

                results = []
                for row in rows:
                    r = dict(row)
                    if isinstance(r.get("content"), str):
                        r["content"] = json.loads(r["content"])
                    results.append(r)
                return results
        except Exception as e:
            logger.error(f"Ошибка получения суммаризаций: {e}")
            return []

    @staticmethod
    async def get_last_summary(period_type: str = "hour") -> Optional[Dict]:
        """Получает последнюю суммаризацию заданного типа"""
        try:
            async with DatabasePoolManager.connection() as conn:
                row = await conn.fetchrow(
                    """
                    SELECT id, period_start, period_end, content
                    FROM summarizations
                    WHERE period_type = $1
                    ORDER BY period_start DESC
                    LIMIT 1
                """,
                    period_type,
                )

                if row:
                    result = dict(row)
                    if isinstance(result.get("content"), str):
                        import json

                        result["content"] = json.loads(result["content"])
                    return result
                return None
        except Exception as e:
            logger.error(f"Ошибка получения последней суммаризации: {e}")
            return None

    @staticmethod
    async def get_smart_articles_for_summarization(
        start: datetime,
        end: datetime,
        total_limit: int = 40,
        sources: Optional[List[str]] = None,
    ) -> List[Dict]:
        """
        Умная выборка для суммаризации:
        1. Сбалансированно по источникам
        2. Без слишком коротких/длинных постов
        3. Без явной рекламы
        4. Равномерно по времени
        """
        try:
            async with DatabasePoolManager.connection() as conn:
                # 1. Получаем список активных источников
                if sources:
                    source_rows = await conn.fetch(
                        "SELECT id, name FROM sources WHERE name = ANY($1) AND is_active = TRUE",
                        sources,
                    )
                else:
                    source_rows = await conn.fetch(
                        "SELECT id, name FROM sources WHERE is_active = TRUE"
                    )

                if not source_rows:
                    logger.warning("Нет активных источников")
                    return []

                sources_info = [dict(row) for row in source_rows]

                # 2. Разбиваем период на сегменты (4 сегмента = каждые 15 минут при часе)
                num_segments = 4
                delta = (end - start) / num_segments
                per_segment_per_source = max(
                    1, total_limit // (num_segments * len(sources_info))
                )

                all_articles = []

                for segment in range(num_segments):
                    seg_start = start + delta * segment
                    seg_end = start + delta * (segment + 1)

                    for source in sources_info:
                        rows = await conn.fetch(
                            """
                            SELECT 
                                ra.raw_title as title, 
                                ra.raw_text as text, 
                                ra.published_at, 
                                ra.url, 
                                ra.source_id,
                                s.name as source_name
                            FROM raw_articles ra
                            JOIN sources s ON ra.source_id = s.id
                            WHERE ra.published_at >= $1 
                                AND ra.published_at <= $2
                                AND ra.status != 'failed'
                                AND ra.source_id = $3
                                AND LENGTH(ra.raw_text) BETWEEN 200 AND 5000
                                AND ra.raw_title NOT ILIKE '%реклама%'
                                AND ra.raw_title NOT ILIKE '%партнёр%'
                                AND ra.raw_title NOT ILIKE '🔥%'
                                AND ra.raw_title NOT ILIKE '💰%'
                            ORDER BY ra.published_at ASC
                            LIMIT $4
                        """,
                            seg_start,
                            seg_end,
                            source["id"],
                            per_segment_per_source,
                        )

                        for row in rows:
                            all_articles.append(dict(row))

                # 3. Перемешиваем для разнообразия
                random.shuffle(all_articles)

                # 4. Ограничиваем до лимита
                all_articles = all_articles[:total_limit]

                # 5. Логируем итоговое распределение
                source_counts = {}
                for a in all_articles:
                    name = a.get("source_name", "unknown")
                    source_counts[name] = source_counts.get(name, 0) + 1
                logger.info(
                    f"Умная выборка: {len(all_articles)} статей из {len(sources_info)} источников. "
                    f"Распределение: {source_counts}"
                )

                return all_articles

        except Exception as e:
            logger.error(f"Ошибка умной выборки: {e}")
            return []
