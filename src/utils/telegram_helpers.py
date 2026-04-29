"""
Вспомогательные функции для Telegram бота.
"""

import re


def escape_markdown(text: str, version: int = 2) -> str:
    """
    Экранирует спецсимволы для Telegram Markdown (v2).

    Args:
        text: Исходный текст
        version: 1 для MarkdownV1, 2 для MarkdownV2 (по умолчанию)

    Returns:
        Экранированный текст
    """
    if not text:
        return ""

    special_chars = r"_*[]()~`>#+-=|{}.!" if version == 1 else r"_*[]()~`>#+-=|{}.!\\"

    return re.sub(f"([{re.escape(special_chars)}])", r"\\\1", text)


def safe_markdown_text(content: str, wrap_bold: bool = False) -> str:
    """
    Подготавливает текст для безопасной отправки с Markdown разметкой.

    Args:
        content: Исходный текст
        wrap_bold: Обернуть ли весь текст в **жирный**

    Returns:
        Безопасный текст с разметкой
    """
    if not content:
        return ""

    # Сначала экранируем всё
    escaped = escape_markdown(content)

    if wrap_bold:
        return f"*{escaped}*"

    return escaped


def truncate_with_ellipsis(text: str, max_length: int = 80) -> str:
    """
    Обрезает текст до максимальной длины с многоточием.

    Args:
        text: Исходный текст
        max_length: Максимальная длина

    Returns:
        Обрезанный текст
    """
    if not text:
        return ""

    if len(text) <= max_length:
        return text

    return text[: max_length - 3].strip() + "..."
