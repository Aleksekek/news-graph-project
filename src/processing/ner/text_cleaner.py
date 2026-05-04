"""
Очистка текста статей перед NER-обработкой.
"""

import re

from bs4 import BeautifulSoup

# Журналистские дейтлайны в начале статьи / абзаца. Засоряют entities именами
# источника (ТАСС, Интерфакс) и городом-публикации, который не имеет отношения
# к содержанию статьи.
#   ТАСС:     "МОСКВА, 3 мая. /ТАСС/. Тегеран намерен..."
#   Интерфакс: "Москва. 3 мая. INTERFAX.RU - Губернатор..."
_TASS_DATELINE_RE = re.compile(
    r"^[А-ЯЁA-Z][А-ЯЁA-Z\- ]{1,40},\s*"   # CITY (caps, hyphens)
    r"\d{1,2}\s+[а-яё]+\.?\s*"            # "3 мая." / "12 января"
    r"/ТАСС/\.?\s*",                      # /ТАСС/.
    re.MULTILINE,
)
_INTERFAX_DATELINE_RE = re.compile(
    r"^[А-ЯЁ][А-ЯЁа-яё\- ]{1,40}\.\s*"    # City. (titlecase; multi-word like "Нижний Новгород")
    r"\d{1,2}\s+[а-яё]+\.?\s+"            # 3 мая.
    r"INTERFAX\.RU\s*[-–—]\s*",           # INTERFAX.RU - / – / —
    re.MULTILINE,
)


def clean_html(text: str) -> str:
    """HTML → plain text через BeautifulSoup."""
    if not text:
        return ""
    soup = BeautifulSoup(text, "html.parser")
    return soup.get_text(separator=" ", strip=True)


def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _strip_dateline(text: str, source: str | None) -> str:
    """Убирает source-специфичные дейтлайны. Применяется ДО normalize_whitespace,
    чтобы re.MULTILINE поймал начало абзаца (а не только начало текста)."""
    if not text or not source:
        return text
    s = source.lower()
    if "tass" in s or "тасс" in s:
        return _TASS_DATELINE_RE.sub("", text)
    if "interfax" in s or "интерфакс" in s:
        return _INTERFAX_DATELINE_RE.sub("", text)
    return text


def clean_article_text(
    raw_title: str, raw_text: str, source: str | None = None
) -> tuple[str, str]:
    """Возвращает (clean_title, clean_text) — без HTML, нормализованные пробелы.

    Если передан source — дополнительно убирает source-специфичные дейтлайны
    из тела статьи (но не из заголовка).
    """
    title = normalize_whitespace(clean_html(raw_title or ""))
    text = clean_html(raw_text or "")
    text = _strip_dateline(text, source)
    text = normalize_whitespace(text)
    return title, text
