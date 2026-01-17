"""
Фабрика для создания парсеров с поддержкой передачи параметров в конструктор.
"""

from typing import Any, Dict, Optional

from src.core.constants import SOURCE_IDS
from src.core.exceptions import ConfigurationError, SourceNotFoundError
from src.domain.parsing.base import BaseParser, ParserConfig
from src.domain.parsing.parsers.lenta import LentaParser
from src.domain.parsing.parsers.tinvest import TInvestParser


class ParserFactory:
    """
    Фабрика для создания парсеров по имени источника.
    Поддерживает передачу специфичных параметров в конструктор.
    """

    # Реестр парсеров
    _parsers_registry: Dict[str, type] = {
        "lenta": LentaParser,
        "tinvest": TInvestParser,
    }

    # Конфигурации по умолчанию для источников
    _default_configs: Dict[str, Dict[str, Any]] = {
        "lenta": {
            "source_name": "lenta",
            "base_url": "https://lenta.ru",
            "request_delay": 2.0,
            "max_retries": 3,
            "timeout": 30,
            "user_agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
        },
        "tinvest": {
            "source_name": "tinvest",
            "request_delay": 1.5,
            "max_retries": 3,
            "timeout": 30,
            "user_agent": "TInvestParser/1.0",
            "tickers": ["SBER", "VTBR", "MOEX"],  # Дефолтные тикеры
        },
    }

    @classmethod
    def create(
        cls, source_name: str, config_overrides: Optional[Dict[str, Any]] = None
    ) -> BaseParser:
        """
        Создание парсера по имени источника с поддержкой специфичных параметров.

        Args:
            source_name: Имя источника (lenta, tinvest)
            config_overrides: Переопределения конфигурации
                - Для TInvest: tickers=["SBER", "VTBR"]
                - Для Lenta: categories=["Политика", "Экономика"]

        Returns:
            Экземпляр парсера

        Raises:
            SourceNotFoundError: Если источник не найден
            ConfigurationError: Если ошибка конфигурации
        """
        # Проверяем существование источника
        if source_name not in cls._parsers_registry:
            available = list(cls._parsers_registry.keys())
            raise SourceNotFoundError(
                f"Источник '{source_name}' не найден. "
                f"Доступные источники: {available}"
            )

        # Получаем ID источника
        source_id = SOURCE_IDS.get(source_name)
        if source_id is None:
            raise ConfigurationError(f"Не найден ID для источника '{source_name}'")

        # Формируем конфигурацию
        default_config = cls._default_configs.get(source_name, {}).copy()

        # Базовые параметры
        base_config = {
            "source_id": source_id,
            "source_name": source_name,
        }

        # Объединяем конфигурации
        config_dict = {**base_config, **default_config}

        # Добавляем переопределения
        if config_overrides:
            config_dict.update(config_overrides)

        # Для TInvest сохраняем тикеры в конфиг
        if source_name == "tinvest" and config_overrides:
            if "tickers" in config_overrides:
                config_dict["tickers"] = config_overrides["tickers"]

        # Для Lenta сохраняем категории в конфиг
        if source_name == "lenta" and config_overrides:
            if "categories" in config_overrides:
                config_dict["categories"] = config_overrides["categories"]

        # Создаем конфигурацию (ParserConfig разрешает дополнительные поля)
        try:
            config = ParserConfig(**config_dict)
        except Exception as e:
            raise ConfigurationError(f"Ошибка конфигурации парсера: {e}")

        # Создаем парсер
        parser_class = cls._parsers_registry[source_name]

        try:
            parser = parser_class(config)
            return parser
        except Exception as e:
            raise ConfigurationError(f"Ошибка создания парсера: {e}")

    @classmethod
    def register_parser(
        cls,
        source_name: str,
        parser_class: type,
        default_config: Optional[Dict[str, Any]] = None,
    ):
        """
        Регистрация нового парсера.

        Args:
            source_name: Имя источника
            parser_class: Класс парсера (должен наследоваться от BaseParser)
            default_config: Конфигурация по умолчанию
        """
        if not issubclass(parser_class, BaseParser):
            raise TypeError(
                f"Парсер должен наследоваться от BaseParser, " f"получен {parser_class}"
            )

        cls._parsers_registry[source_name] = parser_class

        if default_config:
            cls._default_configs[source_name] = default_config

    @classmethod
    def list_available_parsers(cls) -> Dict[str, str]:
        """
        Список доступных парсеров.

        Returns:
            Словарь {имя_источника: описание}
        """
        result = {}
        for source_name, parser_class in cls._parsers_registry.items():
            result[source_name] = {
                "class": parser_class.__name__,
                "source_id": SOURCE_IDS.get(source_name),
                "description": parser_class.__doc__ or "Без описания",
                "supports_kwargs": hasattr(parser_class, "parse"),
            }
        return result

    @classmethod
    def get_parser_info(cls, source_name: str) -> Dict[str, Any]:
        """
        Информация о парсере.

        Args:
            source_name: Имя источника

        Returns:
            Информация о парсере
        """
        if source_name not in cls._parsers_registry:
            raise SourceNotFoundError(f"Парсер '{source_name}' не найден")

        parser_class = cls._parsers_registry[source_name]
        default_config = cls._default_configs.get(source_name, {})

        return {
            "name": source_name,
            "class": parser_class.__name__,
            "module": parser_class.__module__,
            "source_id": SOURCE_IDS.get(source_name),
            "default_config": default_config,
            "supports_archive": hasattr(parser_class, "parse_period"),
            "supported_kwargs": cls._get_supported_kwargs(parser_class),
        }

    @classmethod
    def _get_supported_kwargs(cls, parser_class: type) -> list:
        """Получение поддерживаемых kwargs из сигнатуры метода parse"""
        import inspect

        try:
            # Получаем метод parse
            parse_method = getattr(parser_class, "parse", None)
            if parse_method and inspect.iscoroutinefunction(parse_method):
                # Анализируем сигнатуру
                signature = inspect.signature(parse_method)
                params = list(signature.parameters.keys())

                # Убираем self и limit
                if "self" in params:
                    params.remove("self")
                if "limit" in params:
                    params.remove("limit")

                return params
        except Exception:
            pass

        return []
