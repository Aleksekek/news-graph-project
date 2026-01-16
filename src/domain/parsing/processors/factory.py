"""
Фабрика для создания процессоров.
"""

from typing import Any, Dict

from src.core.constants import SOURCE_IDS
from src.core.exceptions import SourceNotFoundError
from src.domain.parsing.processors.base import BaseProcessor
from src.domain.parsing.processors.lenta import LentaProcessor
from src.domain.parsing.processors.tinvest import TInvestProcessor


class ProcessorFactory:
    """
    Фабрика для создания процессоров по имени источника.
    """

    # Реестр процессоров
    _processors_registry: Dict[str, type] = {
        "lenta": LentaProcessor,
        "tinvest": TInvestProcessor,
    }

    @classmethod
    def create(cls, source_name: str) -> BaseProcessor:
        """
        Создание процессора по имени источника.

        Args:
            source_name: Имя источника

        Returns:
            Экземпляр процессора

        Raises:
            SourceNotFoundError: Если источник не найден
        """
        if source_name not in cls._processors_registry:
            available = list(cls._processors_registry.keys())
            raise SourceNotFoundError(
                f"Процессор для источника '{source_name}' не найден. "
                f"Доступные источники: {available}"
            )

        # Получаем ID источника
        source_id = SOURCE_IDS.get(source_name)
        if source_id is None:
            raise SourceNotFoundError(f"Не найден ID для источника '{source_name}'")

        # Создаем процессор
        processor_class = cls._processors_registry[source_name]
        return processor_class(source_id, source_name)

    @classmethod
    def register_processor(cls, source_name: str, processor_class: type):
        """
        Регистрация нового процессора.

        Args:
            source_name: Имя источника
            processor_class: Класс процессора
        """
        if not issubclass(processor_class, BaseProcessor):
            raise TypeError(
                f"Процессор должен наследоваться от BaseProcessor, "
                f"получен {processor_class}"
            )

        cls._processors_registry[source_name] = processor_class

    @classmethod
    def list_available_processors(cls) -> Dict[str, str]:
        """
        Список доступных процессоров.

        Returns:
            Словарь {имя_источника: имя_класса}
        """
        return {
            source_name: processor_class.__name__
            for source_name, processor_class in cls._processors_registry.items()
        }
