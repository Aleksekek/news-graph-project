"""
Константы проекта - ID источников, категории, тикеры.
"""

# Идентификаторы источников в БД
SOURCE_IDS = {
    "tinvest": 1,
    "lenta": 2,
}

# Категории Lenta.ru по умолчанию
LENTA_CATEGORIES = [
    "Россия",
    "Бывший СССР",
    "Политика",
    "Экономика",
    "Мир",
    "Наука и техника",
    "Интернет и СМИ",
]

# Тикеры TInvest по умолчанию
TINVEST_TICKERS = [
    "SBER",
    "VTBR",
    "GAZP",
    "LKOH",
    "GMKN",
    "NVTK",
    "TATN",
    "ROSN",
    "ALRS",
    "MOEX",
    "YDEX",
    "OZON",
    "AFKS",
    "PHOR",
    "MGNT",
]

# Статусы статей
ARTICLE_STATUS = {
    "RAW": "raw",  # Не обработана NLP
    "PROCESSED": "processed",  # Обработана
    "FAILED": "failed",  # Ошибка обработки
}

# Типы суммаризаций
SUMMARY_TYPES = {
    "HOUR": "hour",
    "DAY": "day",
}
