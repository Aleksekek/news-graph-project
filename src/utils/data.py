"""
Универсальные утилиты для работы с данными
Объединяет логику из helpers.py, lenta_processor.py, tinvest_processor.py
"""

import hashlib
import json
import re
from datetime import datetime
from typing import Any, Dict, List, Optional, Set, Union
from urllib.parse import urlparse

import pandas as pd

from src.config.settings import settings
from src.core.exceptions import DatabaseError


def safe_str(value: Any, default: str = "") -> str:
    """
    Безопасное преобразование любого значения в строку.

    Args:
        value: Любое значение
        default: Значение по умолчанию если None или NaN

    Returns:
        Очищенная строка
    """
    if value is None:
        return default

    if isinstance(value, float) and pd.isna(value):
        return default

    if isinstance(value, pd._libs.missing.NAType):
        return default

    # Преобразуем в строку и чистим
    result = str(value).strip()
    return result if result else default


def safe_list(value: Any) -> List[str]:
    """
    Безопасное преобразование любого значения в список строк.

    Args:
        value: Любое значение

    Returns:
        Список строк
    """
    if value is None:
        return []

    if isinstance(value, list):
        return [safe_str(item) for item in value]

    if isinstance(value, str):
        # Пробуем парсить как Python список
        if value.startswith("[") and value.endswith("]"):
            try:
                import ast

                parsed = ast.literal_eval(value)
                if isinstance(parsed, list):
                    return [safe_str(item) for item in parsed]
            except (SyntaxError, ValueError):
                pass

        # Если не список, делаем список из одного элемента
        value_str = safe_str(value)
        return [value_str] if value_str else []

    # Для pandas Series и других итерируемых
    try:
        return [safe_str(item) for item in value]
    except (TypeError, ValueError):
        return [safe_str(value)]


def safe_int(value: Any, default: int = 0) -> int:
    """
    Безопасное преобразование в целое число.

    Args:
        value: Любое значение
        default: Значение по умолчанию

    Returns:
        Целое число
    """
    try:
        if value is None or pd.isna(value):
            return default
        return int(float(value))
    except (ValueError, TypeError):
        return default


def safe_datetime(value: Any, default: Optional[datetime] = None) -> Optional[datetime]:
    """
    Безопасное преобразование в datetime.

    Args:
        value: Любое значение
        default: Значение по умолчанию

    Returns:
        Объект datetime или None
    """
    if value is None or pd.isna(value):
        return default

    if isinstance(value, datetime):
        return value

    if isinstance(value, str):
        # Очищаем строку
        value = value.strip()

        # Пробуем разные форматы
        formats = [
            # RSS формат с часовым поясом
            "%a, %d %b %Y %H:%M:%S %z",  # Sat, 17 Jan 2026 19:09:24 +0300
            "%a, %d %b %Y %H:%M:%S %Z",  # С текстовым часовым поясом
            # ISO форматы
            "%Y-%m-%dT%H:%M:%S%z",  # 2026-01-17T19:09:24+0300
            "%Y-%m-%d %H:%M:%S%z",  # 2026-01-17 19:09:24+0300
            "%Y-%m-%dT%H:%M:%S",  # 2026-01-17T19:09:24
            "%Y-%m-%d %H:%M:%S",  # 2026-01-17 19:09:24
            # Другие форматы
            "%d.%m.%Y %H:%M:%S",  # 17.01.2026 19:09:24
            "%d/%m/%Y %H:%M:%S",  # 17/01/2026 19:09:24
            "%Y-%m-%d",  # 2026-01-17
            "%d.%m.%Y %H:%M",  # 17.01.2026 19:09
            "%d/%m/%Y %H:%M",  # 17/01/2026 19:09
            # Для Lenta.ru мета-тегов
            "%Y-%m-%dT%H:%M:%S.%f%z",  # 2026-01-17T19:09:24.123+0300
        ]

        for fmt in formats:
            try:
                if fmt.endswith("%z"):
                    # Для форматов с часовым поясом
                    dt = datetime.strptime(value, fmt)
                    # Приводим к наивному datetime (убираем часовой пояс)
                    if dt.tzinfo:
                        dt = dt.astimezone().replace(tzinfo=None)
                    return dt
                else:
                    return datetime.strptime(value, fmt)
            except ValueError:
                continue

    return default


def generate_content_hash(content: str, algorithm: str = "md5") -> str:
    """
    Генерация хэша контента для дедупликации.

    Args:
        content: Текст для хэширования
        algorithm: Алгоритм хэширования (md5, sha256)

    Returns:
        Хэш в hex формате
    """
    if not content:
        return ""

    content_clean = content.strip().lower()

    if algorithm == "md5":
        return hashlib.md5(content_clean.encode()).hexdigest()
    elif algorithm == "sha256":
        return hashlib.sha256(content_clean.encode()).hexdigest()
    else:
        raise ValueError(f"Unsupported hash algorithm: {algorithm}")


def generate_article_id(
    url: str,
    published_at: Optional[datetime] = None,
    title: Optional[str] = None,
    source_prefix: str = "article",
) -> str:
    """
    Генерация уникального ID статьи.

    Args:
        url: URL статьи
        published_at: Дата публикации
        title: Заголовок
        source_prefix: Префикс источника

    Returns:
        Уникальный ID
    """
    # Используем URL как основу
    url_hash = hashlib.md5(url.encode()).hexdigest()[:12]

    # Добавляем дату если есть
    date_part = ""
    if published_at:
        date_part = published_at.strftime("%Y%m%d")

    # Собираем ID
    parts = [source_prefix]
    if date_part:
        parts.append(date_part)
    parts.append(url_hash)

    return "_".join(parts)


def extract_domain(url: str) -> str:
    """
    Извлечение домена из URL.

    Args:
        url: URL

    Returns:
        Доменное имя
    """
    if not url:
        return ""

    try:
        parsed = urlparse(url)
        domain = parsed.netloc

        # Убираем www.
        if domain.startswith("www."):
            domain = domain[4:]

        return domain
    except Exception:
        return ""


def clean_text(text: str, remove_newlines: bool = False) -> str:
    """
    Очистка текста: удаление лишних пробелов, нормализация.

    Args:
        text: Исходный текст
        remove_newlines: Удалять переносы строк

    Returns:
        Очищенный текст
    """
    if not text:
        return ""

    # Заменяем множественные пробелы и табы
    text = re.sub(r"\s+", " ", text)

    if remove_newlines:
        text = text.replace("\n", " ").replace("\r", " ")

    # Убираем пробелы в начале и конце
    text = text.strip()

    return text


def extract_tickers_from_text(text: str) -> List[str]:
    """
    Извлечение тикеров из текста.
    Поддерживает форматы: {$TICKER}, TICKER, TICKER.ME, TICKER-RM и т.д.

    Args:
        text: Текст для анализа

    Returns:
        Список найденных тикеров
    """
    if not text:
        return []

    tickers = set()

    # Паттерн {$TICKER} (Тинькофф Пульс)
    pattern_dollar = r"\{\$([A-Z0-9]+)\}"
    matches = re.findall(pattern_dollar, text, re.IGNORECASE)
    tickers.update([m.upper() for m in matches])

    # Паттерн для тикеров в скобках или отдельно
    # Ищет слова из 2-6 заглавных букв/цифр
    pattern_standalone = r"\b([A-Z0-9]{2,6})\b"

    # Исключаем общие слова
    exclude_words = {
        "THE",
        "AND",
        "FOR",
        "NOT",
        "ARE",
        "BUT",
        "YOU",
        "ALL",
        "ANY",
        "CAN",
        "HAS",
        "HER",
        "HIS",
        "HOW",
        "ITS",
        "LET",
        "OUR",
        "OUT",
        "SAY",
        "SEE",
        "SHE",
        "THE",
        "TOO",
        "USE",
        "WAY",
        "WHO",
        "WHY",
        "YES",
        "YET",
        "USD",
        "RUB",
        "EUR",
        "GPT",
        "AI",
        "CEO",
        "CFO",
    }

    matches = re.findall(pattern_standalone, text.upper())
    for match in matches:
        # Проверяем, что это похоже на тикер
        if (
            len(match) >= 2
            and match.isupper()
            and not match.isdigit()
            and match not in exclude_words
        ):
            tickers.add(match)

    return list(tickers)


def get_existing_urls(source_id: int) -> Set[str]:
    """
    Получение существующих URL из БД для дедупликации.
    Обновленная версия из helpers.py.

    Args:
        source_id: ID источника

    Returns:
        Множество существующих URL
    """
    import psycopg2

    from src.core.exceptions import DatabaseError
    from src.utils.retry import retry

    existing_urls = set()

    @retry(exceptions=(psycopg2.OperationalError,), max_attempts=3, delay=1.0)
    def fetch_urls():
        nonlocal existing_urls

        try:
            conn = psycopg2.connect(**settings.database_dict)
            with conn.cursor() as cursor:
                cursor.execute(
                    "SELECT url FROM raw_articles WHERE source_id = %s", (source_id,)
                )

                for row in cursor.fetchall():
                    existing_urls.add(row[0])

            conn.close()

            print(f"✅ Загружено {len(existing_urls)} существующих URL из БД")
            return existing_urls

        except Exception as e:
            print(f"⚠️ Не удалось загрузить URL из БД: {e}")
            # Возвращаем пустое множество, продолжаем без дедупликации
            return set()

    try:
        return fetch_urls()
    except Exception as e:
        print(f"❌ Все попытки загрузки URL провалились: {e}")
        return set()


def truncate_text(text: str, max_length: int = 500, ellipsis: str = "...") -> str:
    """
    Обрезка текста до максимальной длины с сохранением целостности слов.

    Args:
        text: Исходный текст
        max_length: Максимальная длина
        ellipsis: Строка для обозначения обрезки

    Returns:
        Обрезанный текст
    """
    if not text or len(text) <= max_length:
        return text

    # Обрезаем до max_length и ищем последний пробел
    truncated = text[:max_length]
    last_space = truncated.rfind(" ")

    if last_space > max_length * 0.7:  # Если нашли хорошее место для обрезки
        truncated = truncated[:last_space]

    return truncated.rstrip() + ellipsis


def validate_url(url: str) -> bool:
    """
    Простая валидация URL для Lenta.ru и подобных.
    """
    if not url or not isinstance(url, str):
        return False

    # Базовые проверки
    if len(url) < 10:
        return False

    # Проверяем что это похоже на URL
    url_lower = url.lower()

    # Должен содержать протокол или начинаться с //
    if not (
        url_lower.startswith("http://")
        or url_lower.startswith("https://")
        or url_lower.startswith("//")
    ):
        # Для Lenta.ru это нормально - добавляем https://
        if url.startswith("/"):
            return True  # Относительные URL ок
        if "lenta.ru" in url_lower or "tinvest://" in url_lower:
            return True

    # Проверяем наличие точки (домен)
    if "." not in url and "://" not in url:
        return False

    return True


def dict_to_json_safe(data: Dict[str, Any]) -> str:
    """
    Безопасная конвертация словаря в JSON строку.

    Args:
        data: Словарь для конвертации

    Returns:
        JSON строка или пустая строка при ошибке
    """
    if not data:
        return ""

    try:
        return json.dumps(data, ensure_ascii=False, default=str)
    except (TypeError, ValueError):
        # Пробуем сериализовать только простые типы
        simple_data = {}
        for key, value in data.items():
            if isinstance(value, (str, int, float, bool, type(None))):
                simple_data[key] = value
            else:
                simple_data[key] = str(value)

        try:
            return json.dumps(simple_data, ensure_ascii=False)
        except Exception:
            return ""


def json_to_dict_safe(json_str: str) -> Dict[str, Any]:
    """
    Безопасная конвертация JSON строки в словарь.

    Args:
        json_str: JSON строка

    Returns:
        Словарь или пустой словарь при ошибке
    """
    if not json_str:
        return {}

    try:
        return json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return {}
