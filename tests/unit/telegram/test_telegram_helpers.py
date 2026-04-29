"""
Тесты для Telegram хелперов.
"""

import pytest

from src.utils.telegram_helpers import escape_markdown, safe_markdown_text, truncate_with_ellipsis


class TestTelegramHelpers:
    """Тесты для telegram_helpers."""

    def test_escape_markdown_v2(self):
        """Экранирование спецсимволов для MarkdownV2."""
        text = "Hello *world* [test] (url)"
        escaped = escape_markdown(text, version=2)

        assert "Hello \\*world\\* \\[test\\] \\(url\\)" == escaped

    def test_escape_markdown_v1(self):
        """Экранирование для MarkdownV1."""
        text = "Hello *world*"
        escaped = escape_markdown(text, version=1)

        assert "Hello \\*world\\*" == escaped

    def test_escape_markdown_empty(self):
        """Пустая строка."""
        assert escape_markdown("") == ""

    def test_safe_markdown_text(self):
        """Безопасный текст с экранированием."""
        text = "Hello *world*"
        safe = safe_markdown_text(text)

        assert "Hello \\*world\\*" == safe

    def test_safe_markdown_text_with_bold(self):
        """Безопасный текст с обёрткой в жирный."""
        text = "Hello world"
        safe = safe_markdown_text(text, wrap_bold=True)

        assert "*Hello world*" == safe

    def test_truncate_with_ellipsis_short(self):
        """Короткий текст не обрезается."""
        text = "Short text"
        result = truncate_with_ellipsis(text, max_length=20)

        assert result == "Short text"

    def test_truncate_with_ellipsis_long(self):
        """Длинный текст обрезается."""
        text = "This is a very long text that should be truncated"
        result = truncate_with_ellipsis(text, max_length=20)

        assert len(result) <= 20
        assert result.endswith("...")

    def test_truncate_with_ellipsis_empty(self):
        """Пустая строка."""
        assert truncate_with_ellipsis("") == ""
