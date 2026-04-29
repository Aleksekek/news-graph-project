"""
Тесты для DeepSeek анализатора.
"""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestDeepSeekAnalyzer:
    """Тесты DeepSeekAnalyzer."""

    @pytest.mark.asyncio
    async def test_generate_summary_empty_posts(self):
        """Генерация суммаризации с пустыми постами."""
        from src.processing.llm.deepseek import DeepSeekAnalyzer

        analyzer = DeepSeekAnalyzer()
        result = await analyzer.generate_summary(
            posts=[], period_start=datetime.now(), period_end=datetime.now()
        )

        assert result is None

    @pytest.mark.asyncio
    async def test_raw_request_success(self):
        """Прямой запрос к LLM (с моком)."""
        from src.processing.llm.deepseek import DeepSeekAnalyzer

        analyzer = DeepSeekAnalyzer()

        mock_response = MagicMock()
        mock_response.choices = [MagicMock()]
        mock_response.choices[0].message.content = "Test response"

        with patch.object(analyzer.client.chat.completions, "create", return_value=mock_response):
            result = await analyzer.raw_request("Test prompt")

            assert result == "Test response"

    @pytest.mark.asyncio
    async def test_raw_request_error(self):
        """Ошибка при запросе к LLM."""
        from src.processing.llm.deepseek import DeepSeekAnalyzer

        analyzer = DeepSeekAnalyzer()

        with patch.object(
            analyzer.client.chat.completions, "create", side_effect=Exception("API Error")
        ):
            result = await analyzer.raw_request("Test prompt")

            assert result == "Не удалось получить ответ от анализатора."
