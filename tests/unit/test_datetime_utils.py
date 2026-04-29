"""
Тесты для модуля работы с часовыми поясами.
Запуск: pytest tests/unit/test_datetime_utils.py -v
"""

from datetime import datetime, timedelta, timezone

import pytest

from src.utils.datetime_utils import (
    MSK_OFFSET,
    MSK_TZ,
    format_for_display,
    msk_naive_to_aware,
    now_msk,
    now_msk_aware,
    parse_html_date,
    parse_rfc2822_date,
    utc_to_msk,
)


class TestDateTimeUtils:
    """Тесты для datetime_utils"""

    def test_now_msk_returns_naive(self):
        """now_msk() должен возвращать naive datetime"""
        now = now_msk()
        assert now.tzinfo is None
        assert isinstance(now, datetime)

    def test_now_msk_correct_offset(self):
        """Проверяем, что now_msk действительно MSK"""
        now = now_msk()
        utc_now = datetime.now(timezone.utc)

        # Разница должна быть примерно 3 часа
        diff = abs((now - utc_now.replace(tzinfo=None)).total_seconds() / 3600 - 3)
        assert diff < 0.1  # Погрешность меньше 0.1 часа

    def test_now_msk_aware_returns_aware(self):
        """now_msk_aware() должен возвращать aware datetime с MSK таймзоной"""
        now = now_msk_aware()
        assert now.tzinfo is not None
        assert now.tzinfo == MSK_TZ
        assert isinstance(now, datetime)

    def test_now_msk_aware_correct_offset(self):
        """Проверяем, что now_msk_aware действительно MSK"""
        now = now_msk_aware()
        utc_now = datetime.now(timezone.utc)

        # Разница offset'ов должна быть 3 часа
        diff = (now.utcoffset().total_seconds() - MSK_OFFSET.total_seconds()) / 3600
        assert abs(diff) < 0.1

    def test_msk_naive_to_aware_with_naive(self):
        """Преобразование naive MSK datetime в aware"""
        naive_dt = datetime(2026, 1, 17, 19, 9, 24)
        aware_dt = msk_naive_to_aware(naive_dt)

        assert aware_dt.tzinfo == MSK_TZ
        assert aware_dt.hour == 19
        assert aware_dt.minute == 9
        # UTC offset должен быть +3 часа
        assert aware_dt.utcoffset().total_seconds() == 3 * 3600

    def test_msk_naive_to_aware_with_aware(self):
        """Преобразование уже aware datetime (должно остаться в MSK)"""
        aware_dt = datetime(2026, 1, 17, 19, 9, 24, tzinfo=MSK_TZ)
        result = msk_naive_to_aware(aware_dt)

        assert result.tzinfo == MSK_TZ
        assert result.hour == 19

    def test_msk_naive_to_aware_with_none(self):
        """Преобразование None должно вернуть None"""
        assert msk_naive_to_aware(None) is None

    def test_msk_naive_to_aware_with_other_tz(self):
        """Преобразование datetime с другой таймзоной должно конвертироваться в MSK"""
        utc_dt = datetime(2026, 1, 17, 16, 9, 24, tzinfo=timezone.utc)
        result = msk_naive_to_aware(utc_dt)

        assert result.tzinfo == MSK_TZ
        assert result.hour == 19  # UTC 16:09 -> MSK 19:09

    def test_utc_to_msk_with_aware(self):
        """Конвертация aware UTC datetime в MSK naive"""
        utc_dt = datetime(2026, 1, 17, 16, 9, 24, tzinfo=timezone.utc)
        msk_dt = utc_to_msk(utc_dt)

        assert msk_dt.tzinfo is None
        assert msk_dt.hour == 19  # 16 UTC + 3 = 19 MSK
        assert msk_dt.day == 17

    def test_utc_to_msk_with_naive(self):
        """Конвертация naive UTC datetime в MSK naive"""
        utc_dt = datetime(2026, 1, 17, 16, 9, 24)  # Считаем что UTC
        msk_dt = utc_to_msk(utc_dt)

        assert msk_dt.tzinfo is None
        assert msk_dt.hour == 19

    def test_parse_rfc2822_date(self):
        """Парсинг RSS дат"""
        # Пример из реального RSS
        rss_date = "Sat, 17 Jan 2026 19:09:24 +0300"
        dt = parse_rfc2822_date(rss_date)

        assert dt is not None
        assert dt.tzinfo is None
        # +0300 уже MSK, поэтому час должен быть 19
        assert dt.hour == 19
        assert dt.year == 2026
        assert dt.month == 1
        assert dt.day == 17

    def test_parse_rfc2822_date_utc(self):
        """Парсинг UTC даты из RSS"""
        rss_date = "Sat, 17 Jan 2026 16:09:24 +0000"
        dt = parse_rfc2822_date(rss_date)

        assert dt is not None
        # UTC 16:09 -> MSK 19:09
        assert dt.hour == 19
        assert dt.minute == 9

    def test_parse_rfc2822_date_invalid(self):
        """Некорректная дата должна вернуть None"""
        assert parse_rfc2822_date("not a date") is None

    def test_parse_html_date_lenta(self):
        """Парсинг даты с Lenta.ru (уже MSK)"""
        # Lenta отдаёт в MSK
        lenta_date = "19:09, 17 января 2026"
        dt = parse_html_date(lenta_date, source="lenta")

        # Должна вернуть MSK без изменений
        assert dt is not None
        assert dt.hour == 19
        assert dt.minute == 9

    def test_parse_html_date_iso_with_tz(self):
        """ISO формат с часовым поясом"""
        iso_date = "2026-01-17T19:09:24+0300"
        dt = parse_html_date(iso_date, source="unknown")

        # Считаем что это UTC +3, значит уже MSK
        assert dt is not None
        assert dt.hour == 19

    def test_parse_html_date_iso_utc(self):
        """ISO формат UTC"""
        iso_date = "2026-01-17T16:09:24Z"
        dt = parse_html_date(iso_date, source="unknown")

        # UTC -> MSK
        assert dt is not None
        assert dt.hour == 19

    def test_format_for_display(self):
        """Форматирование для отображения"""
        dt = datetime(2026, 1, 17, 19, 9)

        with_time = format_for_display(dt, include_time=True)
        assert with_time == "17.01.2026 19:09"

        without_time = format_for_display(dt, include_time=False)
        assert without_time == "17.01.2026"

    def test_format_for_display_none(self):
        """Форматирование None"""
        assert format_for_display(None) == "Дата неизвестна"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
