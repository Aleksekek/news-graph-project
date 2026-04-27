"""
Дополнительные тесты для datetime_utils.
"""

from datetime import datetime, timedelta, timezone

import pytest

from src.utils.datetime_utils import (
    format_for_db,
    format_for_display,
    msk_to_utc,
    now_msk,
    parse_html_date,
    parse_rfc2822_date,
    parse_russian_date,
    utc_to_msk,
)


class TestDateTimeUtilsExtended:
    """Расширенные тесты datetime_utils."""

    def test_now_msk_type(self):
        """now_msk возвращает datetime."""
        now = now_msk()
        assert isinstance(now, datetime)

    def test_msk_to_utc_conversion(self):
        """Конвертация MSK в UTC."""
        msk_dt = datetime(2026, 1, 17, 19, 9)
        utc_dt = msk_to_utc(msk_dt)

        assert utc_dt.tzinfo is not None
        assert utc_dt.hour == 16  # MSK 19:09 -> UTC 16:09

    def test_utc_to_msk_roundtrip(self):
        """Прямая и обратная конвертация."""
        original = datetime(2026, 1, 17, 19, 9)
        utc = msk_to_utc(original)
        back = utc_to_msk(utc)

        assert original == back

    def test_format_for_db_with_timezone(self):
        """format_for_db убирает timezone."""
        dt_with_tz = datetime(2026, 1, 17, 19, 9, tzinfo=timezone(timedelta(hours=3)))
        result = format_for_db(dt_with_tz)

        assert result.tzinfo is None
        assert result.hour == 19

    def test_format_for_display_none(self):
        """format_for_display с None."""
        result = format_for_display(None)
        assert result == "Дата неизвестна"

    def test_parse_russian_date_various(self):
        """Парсинг разных русских дат."""
        test_cases = [
            ("19:09, 17 января 2026", (19, 9, 17, 1, 2026)),
            ("09:05, 3 марта 2025", (9, 5, 3, 3, 2025)),
            ("23:59, 31 декабря 2024", (23, 59, 31, 12, 2024)),
        ]

        for date_str, expected in test_cases:
            dt = parse_russian_date(date_str)
            assert dt is not None
            assert dt.hour == expected[0]
            assert dt.minute == expected[1]
            assert dt.day == expected[2]
            assert dt.month == expected[3]
            assert dt.year == expected[4]

    def test_parse_html_date_unknown_source(self):
        """Парсинг даты из неизвестного источника (считаем UTC)."""
        # ISO с +0000
        dt = parse_html_date("2026-01-17T16:09:24+0000", source="unknown")
        assert dt is not None
        assert dt.hour == 19  # UTC 16:09 -> MSK 19:09
