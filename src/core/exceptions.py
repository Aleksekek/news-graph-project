"""
Исключения проекта.
"""


class NewsGraphError(Exception):
    """Базовое исключение проекта."""

    pass


class ParserError(NewsGraphError):
    """Ошибка парсинга."""

    pass


class DatabaseError(NewsGraphError):
    """Ошибка базы данных."""

    pass


class ConfigurationError(NewsGraphError):
    """Ошибка конфигурации."""

    pass


class ValidationError(NewsGraphError):
    """Ошибка валидации данных."""

    pass


class SourceNotFoundError(NewsGraphError):
    """Источник не найден."""

    pass


class RetryExhaustedError(NewsGraphError):
    """Исчерпаны попытки повтора."""

    pass
