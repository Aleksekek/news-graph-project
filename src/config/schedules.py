"""
Конфигурация расписания задач.
Загружается из YAML файла с дефолтами.

После рефакторинга парсеров (2026-05) RSS-only пути дают <2 сек на цикл,
поэтому отдельные day/night конфигурации больше не нужны: один cron на 24 часа.
"""

import os
from dataclasses import dataclass, field
from typing import Any

import yaml

from src.core.constants import LENTA_CATEGORIES, TINVEST_TICKERS


@dataclass
class TaskConfig:
    """Конфигурация задачи планировщика."""

    name: str
    cron: str
    enabled: bool = True
    kwargs: dict[str, Any] = field(default_factory=dict)


class ScheduleConfig:
    """Конфигурация расписания задач."""

    def __init__(self, config_path: str | None = None):
        if config_path is None:
            # Ищем в config/schedule_config.yaml относительно корня
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            config_path = os.path.join(base_dir, "config", "schedule_config.yaml")

        self.config_path = config_path
        self.tasks = self._load_config()

    def _load_config(self) -> dict[str, TaskConfig]:
        """Загрузка конфигурации с дефолтами.

        Расписание:
          - Новостные ленты (Lenta/Interfax/TASS/RBC) — каждые 30 минут.
            RSS у каждой содержит 30+ свежих статей, потеря между фетчами
            практически невозможна.
          - TInvest — каждые 15 минут (форум, более высокая частота публикаций).
          - Источники staggered по 5 минут, чтобы не бить в БД и сеть пачкой.

        Лимиты выставлены на 30: примерно столько свежих статей в RSS
        каждого источника. Если новых меньше — берём то, что есть.
        """

        default_tasks = {
            # :00, :30
            "lenta": TaskConfig(
                name="Парсинг Lenta.ru",
                cron="0,30 * * * *",
                enabled=True,
                kwargs={"limit": 30, "categories": LENTA_CATEGORIES},
            ),
            # :05, :20, :35, :50 — каждые 15 мин
            "tinvest": TaskConfig(
                name="Парсинг TInvest Pulse",
                cron="5,20,35,50 * * * *",
                enabled=True,
                kwargs={"limit": 30, "tickers": TINVEST_TICKERS},
            ),
            # :10, :40
            "interfax": TaskConfig(
                name="Парсинг Интерфакс",
                cron="10,40 * * * *",
                enabled=True,
                kwargs={"limit": 30, "sections": ["main", "russia", "business"]},
            ),
            # :15, :45
            "tass": TaskConfig(
                name="Парсинг ТАСС",
                cron="15,45 * * * *",
                enabled=True,
                kwargs={"limit": 30},
            ),
            # :25, :55
            "rbc": TaskConfig(
                name="Парсинг РБК",
                cron="25,55 * * * *",
                enabled=True,
                # RBC RSS отдаёт ровно 30 свежих статей — забираем все
                kwargs={"limit": 30},
            ),
        }

        # Пробуем загрузить из YAML (опционально, переопределяет дефолты)
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, encoding="utf-8") as f:
                    yaml_config = yaml.safe_load(f) or {}

                for task_id, task_data in yaml_config.items():
                    if task_id in default_tasks:
                        task = default_tasks[task_id]
                        for key, value in task_data.items():
                            if hasattr(task, key):
                                setattr(task, key, value)
            except Exception as e:
                print(f"⚠️ Ошибка загрузки конфига расписания: {e}")

        return default_tasks

    def get_enabled_tasks(self) -> list[TaskConfig]:
        """Получение только включённых задач."""
        return [task for task in self.tasks.values() if task.enabled]


# Глобальный экземпляр
schedule_config = ScheduleConfig()
