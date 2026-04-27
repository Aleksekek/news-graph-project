"""
Репозиторий для работы с суммаризациями.
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Optional

from src.database.pool import DatabasePoolManager

logger = logging.getLogger(__name__)


class SummaryRepository:
    """Репозиторий для суммаризаций."""

    @staticmethod
    async def save(
        period_start: datetime,
        period_end: datetime,
        period_type: str,
        content: Dict,
        model_used: Optional[str] = None,
        prompt_tokens: int = 0,
        completion_tokens: int = 0,
        cost_usd: float = 0.0,
    ) -> int:
        """Сохраняет суммаризацию в БД."""
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
                    model_used,
                    prompt_tokens,
                    completion_tokens,
                    cost_usd,
                )
                logger.info(
                    f"Суммаризация сохранена: id={result['id']}, тип={period_type}"
                )
                return result["id"]
        except Exception as e:
            logger.error(f"Ошибка сохранения суммаризации: {e}")
            return 0

    @staticmethod
    async def get_for_period(
        start: datetime,
        end: datetime,
        period_type: Optional[str] = None,
    ) -> List[Dict]:
        """Получает суммаризации за период."""
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
    async def get_last(period_type: str = "hour") -> Optional[Dict]:
        """Получает последнюю суммаризацию заданного типа."""
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
                        result["content"] = json.loads(result["content"])
                    return result
                return None
        except Exception as e:
            logger.error(f"Ошибка получения последней суммаризации: {e}")
            return None

    @staticmethod
    async def get_smart_articles(
        start: datetime,
        end: datetime,
        total_limit: int = 40,
    ) -> List[Dict]:
        """
        Умная выборка статей для суммаризации.
        """
        try:
            async with DatabasePoolManager.connection() as conn:
                # Получаем статьи, сбалансированные по времени
                rows = await conn.fetch(
                    """
                    SELECT 
                        ra.raw_title as title, 
                        ra.raw_text as text, 
                        ra.published_at, 
                        ra.url, 
                        s.name as source_name
                    FROM raw_articles ra
                    JOIN sources s ON ra.source_id = s.id
                    WHERE ra.published_at >= $1 
                        AND ra.published_at <= $2
                        AND ra.status != 'failed'
                        AND LENGTH(ra.raw_text) BETWEEN 200 AND 5000
                    ORDER BY ra.published_at DESC
                    LIMIT $3
                    """,
                    start,
                    end,
                    total_limit,
                )

                return [dict(row) for row in rows]
        except Exception as e:
            logger.error(f"Ошибка умной выборки: {e}")
            return []
