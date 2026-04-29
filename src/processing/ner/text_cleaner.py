"""
Очистка текста статей перед NER-обработкой.
"""

import re

from bs4 import BeautifulSoup


def clean_html(text: str) -> str:
    """HTML → plain text через BeautifulSoup."""
    if not text:
        return ""
    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text(separator=" ", strip=True)


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def clean_article_text(raw_title: str, raw_text: str) -> tuple[str, str]:
    """Возвращает (clean_title, clean_text) — без HTML, нормализованные пробелы."""
    title = normalize_whitespace(clean_html(raw_title or ""))
    text = normalize_whitespace(clean_html(raw_text or ""))
    return title, text
