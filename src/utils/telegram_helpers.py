import re
from typing import Optional


def escape_markdown(text: str, version: int = 2) -> str:
    """
    Экранирует спецсимволы для Telegram Markdown (v2).
    Оставляет только намеренную разметку.

    Args:
        text: Исходный текст
        version: 1 для MarkdownV1, 2 для MarkdownV2 (по умолчанию)

    Returns:
        Экранированный текст
    """
    if version == 1:
        # MarkdownV1: _ * [ ] ( ) ~ ` > # + - = | { } . !
        special_chars = r"_*[]()~`>#+-=|{}.!"
    else:
        # MarkdownV2: _ * [ ] ( ) ~ ` > # + - = | { } . ! \\
        special_chars = r"_*[]()~`>#+-=|{}.!\\"

    return re.sub(f"([{re.escape(special_chars)}])", r"\\\1", text)


def safe_markdown_text(content: str, wrap_bold: bool = False) -> str:
    """
    Подготавливает текст для безопасной отправки с Markdown разметкой.
    Экранирует всё, кроме специальных маркеров, которые мы явно добавим.

    Args:
        content: Исходный текст
        wrap_bold: Обернуть ли весь текст в **жирный**

    Returns:
        Безопасный текст с разметкой
    """
    # Сначала экранируем всё
    escaped = escape_markdown(content)

    if wrap_bold:
        return f"*{escaped}*"

    return escaped


def truncate_with_ellipsis(text: str, max_length: int = 80) -> str:
    """Обрезает текст до максимальной длины с многоточием"""
    if len(text) <= max_length:
        return text
    return text[: max_length - 3].strip() + "..."
