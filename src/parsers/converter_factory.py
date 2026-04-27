"""
Фабрика для создания конвертеров по имени источника.
"""

from typing import Dict, Type

from src.core.exceptions import SourceNotFoundError
from src.parsers.lenta.converter import LentaConverter
from src.parsers.tinvest.converter import TInvestConverter


class ConverterFactory:
    """Фабрика конвертеров."""

    _converters: Dict[str, Type] = {
        "lenta": LentaConverter,
        "tinvest": TInvestConverter,
    }

    @classmethod
    def create(cls, source_name: str):
        """Создание конвертера для источника."""
        if source_name not in cls._converters:
            available = list(cls._converters.keys())
            raise SourceNotFoundError(
                f"Конвертер для '{source_name}' не найден. " f"Доступные: {available}"
            )

        converter_class = cls._converters[source_name]
        return converter_class()

    @classmethod
    def register(cls, source_name: str, converter_class: Type):
        """Регистрация нового конвертера."""
        cls._converters[source_name] = converter_class
