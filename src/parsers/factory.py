"""
Фабрика для создания парсеров по имени источника.
"""

from typing import Any

from src.core.constants import SOURCE_IDS
from src.core.exceptions import ConfigurationError, SourceNotFoundError
from src.core.models import ParserConfig
from src.parsers.base import BaseParser
from src.parsers.interfax.parser import InterfaxParser
from src.parsers.lenta.parser import LentaParser
from src.parsers.rbc.parser import RbcParser
from src.parsers.tass.parser import TassParser
from src.parsers.tinvest.parser import TInvestParser


class ParserFactory:
    """
    Фабрика для создания парсеров.
    Поддерживает передачу специфичных параметров.
    """

    # Реестр парсеров
    _parsers_registry: dict[str, type] = {
        "lenta": LentaParser,
        "tinvest": TInvestParser,
        "interfax": InterfaxParser,
        "tass": TassParser,
        "rbc": RbcParser,
    }

    _default_ua = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"

    # Конфигурации по умолчанию для каждого источника
    _default_configs: dict[str, dict[str, Any]] = {
        "lenta": {
            "base_url": "https://lenta.ru",
            "request_delay": 2.0,
            "max_retries": 3,
            "timeout": 30,
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        },
        "tinvest": {
            "request_delay": 1.5,
            "max_retries": 3,
            "timeout": 30,
            "user_agent": "TInvestParser/1.0",
        },
        "interfax": {
            "request_delay": 1.0,
            "max_retries": 3,
            "timeout": 30,
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        },
        "tass": {
            "request_delay": 1.0,
            "max_retries": 3,
            "timeout": 30,
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        },
        "rbc": {
            "request_delay": 1.5,
            "max_retries": 3,
            "timeout": 30,
            "user_agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        },
    }

    @classmethod
    def create(
        cls, source_name: str, config_overrides: dict[str, Any] | None = None
    ) -> BaseParser:
        """
        Создание парсера по имени источника.

        Args:
            source_name: Имя источника
            config_overrides: Переопределения конфигурации
                - Для Lenta: {"categories": ["Политика"]}
                - Для TInvest: {"tickers": ["SBER", "VTBR"]}

        Returns:
            Экземпляр парсера
        """
        if source_name not in cls._parsers_registry:
            available = list(cls._parsers_registry.keys())
            raise SourceNotFoundError(
                f"Источник '{source_name}' не найден. " f"Доступные: {available}"
            )

        source_id = SOURCE_IDS.get(source_name)
        if source_id is None:
            raise ConfigurationError(f"Не найден ID для источника '{source_name}'")

        # Базовая конфигурация
        default_config = cls._default_configs.get(source_name, {}).copy()

        config_dict = {
            "source_id": source_id,
            "source_name": source_name,
            **default_config,
        }

        # Добавляем переопределения
        if config_overrides:
            config_dict.update(config_overrides)

        try:
            config = ParserConfig(**config_dict)
            parser_class = cls._parsers_registry[source_name]
            return parser_class(config)
        except Exception as e:
            raise ConfigurationError(f"Ошибка создания парсера: {e}") from e

    @classmethod
    def register_parser(
        cls,
        source_name: str,
        parser_class: type,
        default_config: dict[str, Any] | None = None,
    ):
        """Регистрация нового парсера."""
        if not issubclass(parser_class, BaseParser):
            raise TypeError("Парсер должен наследоваться от BaseParser")

        cls._parsers_registry[source_name] = parser_class
        if default_config:
            cls._default_configs[source_name] = default_config

    @classmethod
    def list_available(cls) -> list:
        """Список доступных источников."""
        return list(cls._parsers_registry.keys())
