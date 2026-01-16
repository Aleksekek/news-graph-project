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

# Популярные тикеры для простого NLP
COMMON_TICKERS = [
    "SBER",
    "VTBR",
    "GAZP",
    "LKOH",
    "GMKN",
    "NVTK",
    "TATN",
    "ROSN",
    "ALRS",
    "POLY",
    "MOEX",
    "YNDX",
    "OZON",
    "TCSG",
    "VKCO",
    "AFKS",
    "PHOR",
    "MGNT",
    "DSKY",
    "QIWI",
]
