"""
Интеграционные тесты для use case парсинга.
"""

from datetime import datetime
from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.mark.asyncio
async def test_parse_source_success():
    """Успешный парсинг источника."""
    from src.app.parse_source import ParseSourceUseCase
    from src.core.models import ParsedItem, ProcessingStats

    # Создаём мок-парсер
    mock_parser = AsyncMock()
    mock_parser.parse = AsyncMock(
        return_value=MagicMock(
            items=[
                ParsedItem(
                    source_id=1,
                    source_name="lenta",  # Исправлено: реальное имя
                    original_id="test_1",
                    url="https://test.com/1",
                    title="Test 1",
                    content="Content 1",
                    published_at=datetime.now(),
                )
            ]
        )
    )
    mock_parser.__aenter__ = AsyncMock(return_value=mock_parser)
    mock_parser.__aexit__ = AsyncMock()

    # Создаём мок-конвертер
    mock_converter = MagicMock()
    mock_converter.convert = MagicMock(return_value=MagicMock())

    # Мокаем фабрики
    with patch("src.app.parse_source.ParserFactory") as MockParserFactory, patch(
        "src.app.parse_source.ConverterFactory"
    ) as MockConverterFactory, patch(
        "src.app.parse_source.ArticleRepository"
    ) as MockRepo:

        MockParserFactory.create.return_value = mock_parser
        MockConverterFactory.create.return_value = mock_converter

        mock_repo = AsyncMock()
        mock_repo.save_batch.return_value = ProcessingStats(total_rows=1, saved=1)
        MockRepo.return_value = mock_repo

        # Выполняем use case с реальным source_name
        use_case = ParseSourceUseCase()
        stats = await use_case.execute(
            source_name="lenta",  # Исправлено: реальное имя
            limit=10,
            categories=["Тест"],
        )

        assert stats.saved == 1


@pytest.mark.asyncio
async def test_parse_source_empty_result():
    """Парсинг без результатов."""
    from src.app.parse_source import ParseSourceUseCase

    mock_parser = AsyncMock()
    mock_parser.parse = AsyncMock(return_value=MagicMock(items=[]))
    mock_parser.__aenter__ = AsyncMock(return_value=mock_parser)
    mock_parser.__aexit__ = AsyncMock()

    with patch("src.app.parse_source.ParserFactory") as MockParserFactory, patch(
        "src.app.parse_source.ConverterFactory"
    ) as MockConverterFactory, patch(
        "src.app.parse_source.ArticleRepository"
    ) as MockRepo:

        MockParserFactory.create.return_value = mock_parser

        # Создаём мок-конвертер (даже если не будет использован)
        mock_converter = MagicMock()
        MockConverterFactory.create.return_value = mock_converter

        use_case = ParseSourceUseCase()
        stats = await use_case.execute(source_name="lenta", limit=10)

        assert stats.total_rows == 0
        assert stats.saved == 0
