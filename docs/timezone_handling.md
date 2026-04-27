# Работа с часовыми поясами

## Единое правило

**ВСЕ даты в проекте хранятся в MSK (UTC+3) и НЕ содержат timezone (naive datetime).**

```
Хранение в БД          → MSK naive
Парсинг из RSS (UTC)   → конвертируем в MSK
Парсинг из HTML (MSK)  → оставляем как есть
Отображение в Telegram → MSK (без изменений)
```

## Почему MSK?

- Lenta.ru отдаёт время уже в MSK
- TInvest отдаёт в UTC, но мы конвертируем
- Большинство пользователей из UTC+3
- Упрощает отладку и запросы к БД

## Использование

```python
from src.utils.datetime_utils import now_msk, utc_to_msk, format_for_display

# Получить текущее MSK время
now = now_msk()

# Конвертировать UTC в MSK
utc_time = datetime(2026, 1, 17, 16, 9, tzinfo=timezone.utc)
msk_time = utc_to_msk(utc_time)  # 19:09

# Форматировать для пользователя
display = format_for_display(msk_time, include_time=True)  # "17.01.2026 19:09"
```

## Добавление нового источника

Если новый источник отдаёт время в другом формате:

1. Добавьте формат в `HTML_DATE_FORMATS` или `RSS_DATE_FORMATS`
2. Если источник отдаёт в MSK, укажите `source="newsource"` в `parse_html_date()`
3. Если в UTC — конвертация произойдёт автоматически

## Проверка

```bash
pytest tests/unit/test_datetime_utils.py -v
```

Все 11 тестов должны проходить.