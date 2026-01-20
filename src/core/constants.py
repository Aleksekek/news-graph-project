"""Константы проекта"""

# Идентификаторы источников
SOURCE_IDS = {
    "tinvest": 1,
    "lenta": 2,
}

# Типы сущностей
ENTITY_TYPES = {
    "organization": "organization",
    "person": "person",
    "location": "location",
    "product": "product",
    "event": "event",
}

# Статусы статей
ARTICLE_STATUSES = {
    "raw": "raw",  # Необработанная
    "processed": "processed",  # Обработанная NLP
    "failed": "failed",  # Ошибка обработки
    "skipped": "skipped",  # Пропущена (пустая и т.д.)
}

# Категории Lenta.ru
LENTA_CATEGORIES = [
    "Россия",
    "Бывший СССР",
    "Политика",
    "Экономика",
    "Мир",
    "Наука и техника",
    "Интернет и СМИ",
]

# Тикеры ТПульса
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
