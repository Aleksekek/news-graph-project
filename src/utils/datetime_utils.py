"""
Единая политика работы с часовыми поясами.
Все даты в проекте хранятся в MSK (UTC+3) без tzinfo.
"""

import re
from datetime import datetime, timedelta, timezone
from typing import Optional

# Константы
MSK_OFFSET = timedelta(hours=3)
MSK_TZ = timezone(MSK_OFFSET)

# Форматы дат
RSS_DATE_FORMATS = [
    "%a, %d %b %Y %H:%M:%S %z",  # Sat, 17 Jan 2026 19:09:24 +0300
    "%a, %d %b %Y %H:%M:%S %Z",  # С текстовым часовым поясом
]

HTML_DATE_FORMATS = [
    "%Y-%m-%dT%H:%M:%S%z",  # 2026-01-17T19:09:24+0300
    "%Y-%m-%d %H:%M:%S%z",  # 2026-01-17 19:09:24+0300
    "%Y-%m-%dT%H:%M:%S",  # 2026-01-17T19:09:24
    "%Y-%m-%d %H:%M:%S",  # 2026-01-17 19:09:24
    "%d.%m.%Y %H:%M:%S",  # 17.01.2026 19:09:24
    "%d/%m/%Y %H:%M:%S",  # 17/01/2026 19:09:24
    "%Y-%m-%d",  # 2026-01-17
]

# Месяцы на русском для Lenta.ru
RUSSIAN_MONTHS = {
    "января": 1,
    "янв": 1,
    "февраля": 2,
    "фев": 2,
    "марта": 3,
    "мар": 3,
    "апреля": 4,
    "апр": 4,
    "мая": 5,
    "май": 5,
    "июня": 6,
    "июн": 6,
    "июля": 7,
    "июл": 7,
    "августа": 8,
    "авг": 8,
    "сентября": 9,
    "сен": 9,
    "октября": 10,
    "окт": 10,
    "ноября": 11,
    "ноя": 11,
    "декабря": 12,
    "дек": 12,
}


def now_msk() -> datetime:
    """
    Текущее время в MSK (naive).
    Использовать для всех `datetime.now()` в проекте.
    """
    return datetime.now(MSK_TZ).replace(tzinfo=None)


def utc_to_msk(utc_dt: datetime) -> datetime:
    """
    Конвертирует UTC datetime в MSK (naive).

    Args:
        utc_dt: datetime с tzinfo=UTC или naive (считаем что UTC)

    Returns:
        MSK datetime без tzinfo
    """
    if utc_dt.tzinfo is None:
        # Считаем что это UTC
        utc_dt = utc_dt.replace(tzinfo=timezone.utc)
    return utc_dt.astimezone(MSK_TZ).replace(tzinfo=None)


def msk_to_utc(msk_dt: datetime) -> datetime:
    """
    Конвертирует MSK datetime в UTC (aware).

    Args:
        msk_dt: naive datetime в MSK

    Returns:
        UTC datetime с tzinfo
    """
    if msk_dt.tzinfo is not None:
        msk_dt = msk_dt.replace(tzinfo=None)
    msk_aware = msk_dt.replace(tzinfo=MSK_TZ)
    return msk_aware.astimezone(timezone.utc)


def parse_rfc2822_date(date_str: str) -> Optional[datetime]:
    """
    Парсит RFC 2822 дату (RSS формат) в MSK naive.
    RSS всегда в UTC, поэтому конвертируем.

    Args:
        date_str: строка с датой из RSS

    Returns:
        MSK datetime или None
    """
    for fmt in RSS_DATE_FORMATS:
        try:
            dt = datetime.strptime(date_str, fmt)
            # RSS всегда UTC, конвертируем в MSK
            return utc_to_msk(dt)
        except ValueError:
            continue
    return None


def parse_russian_date(date_str: str) -> Optional[datetime]:
    """
    Парсит русскоязычную дату формата Lenta.ru.
    Пример: "19:09, 17 января 2026"

    Args:
        date_str: строка с датой на русском

    Returns:
        MSK datetime или None
    """
    # Паттерн: "ЧЧ:ММ, ДД МЕСЯЦ ГГГГ"
    pattern = r"(\d{1,2}):(\d{2})\s*,\s*(\d{1,2})\s+([а-яё]+)\s+(\d{4})"
    match = re.search(pattern, date_str, re.IGNORECASE)

    if not match:
        return None

    hour, minute, day, month_str, year = match.groups()
    month = RUSSIAN_MONTHS.get(month_str.lower())

    if not month:
        return None

    try:
        # Lenta.ru отдаёт время уже в MSK
        return datetime(int(year), month, int(day), int(hour), int(minute))
    except ValueError:
        return None


def parse_html_date(date_str: str, source: str = "unknown") -> Optional[datetime]:
    """
    Парсит дату из HTML страницы.

    Args:
        date_str: строка с датой
        source: источник ("lenta", "tinvest", etc.)

    Returns:
        MSK datetime или None
    """
    # Сначала пробуем русский формат (Lenta.ru)
    if source == "lenta":
        russian_dt = parse_russian_date(date_str)
        if russian_dt:
            return russian_dt

    # Пробуем стандартные форматы
    for fmt in HTML_DATE_FORMATS:
        try:
            dt = datetime.strptime(date_str, fmt)

            # Если источник отдаёт в MSK (как Lenta), не конвертируем
            if source == "lenta":
                # Lenta.ru уже в MSK
                if dt.tzinfo:
                    return dt.astimezone(MSK_TZ).replace(tzinfo=None)
                return dt
            else:
                # Для других источников считаем UTC и конвертируем
                return utc_to_msk(dt)
        except ValueError:
            continue

    return None


def naive_msk_dt(dt: datetime) -> datetime:
    """
    Подготавливает datetime.
    Просто убеждается, что naive и в MSK.
    """
    if dt is None:
        return None
    if dt.tzinfo is not None:
        return dt.astimezone(MSK_TZ).replace(tzinfo=None)
    return dt


def format_for_db(msk_naive: datetime) -> datetime:
    """
    Конвертирует MSK naive datetime в UTC naive для БД.
    Это правильный подход — БД должна хранить UTC.
    """
    if msk_naive is None:
        return None
    if msk_naive.tzinfo is not None:
        msk_naive = msk_naive.replace(tzinfo=None)

    # Помечаем как MSK и конвертируем в UTC
    msk_aware = msk_naive.replace(tzinfo=MSK_TZ)
    utc_naive = msk_aware.astimezone(timezone.utc).replace(tzinfo=None)
    return utc_naive


def format_for_display(dt: datetime, include_time: bool = True) -> str:
    """
    Форматирует datetime для отображения пользователю.

    Args:
        dt: datetime (должен быть MSK naive)
        include_time: показывать время

    Returns:
        Отформатированная строка
    """
    if dt is None:
        return "Дата неизвестна"

    if include_time:
        return dt.strftime("%d.%m.%Y %H:%M")
    return dt.strftime("%d.%m.%Y")
