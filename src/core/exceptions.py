"""Исключения проекта"""


class NewsGraphError(Exception):
    """Базовое исключение проекта"""

    pass


class ParserError(NewsGraphError):
    """Ошибка парсера"""

    pass


class DatabaseError(NewsGraphError):
    """Ошибка базы данных"""

    pass


class ConfigurationError(NewsGraphError):
    """Ошибка конфигурации"""

    pass


class ValidationError(NewsGraphError):
    """Ошибка валидации данных"""

    pass


class RetryExhaustedError(NewsGraphError):
    """Исчерпаны попытки повтора"""

    pass


class SourceNotFoundError(NewsGraphError):
    """Источник не найден"""

    pass
