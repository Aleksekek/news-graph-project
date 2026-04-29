"""
Конфигурация расписания задач.
Загружается из YAML файла с дефолтами.
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
        """Загрузка конфигурации с дефолтами."""

        # Дефолтные задачи
        default_tasks = {
            # Lenta: днем каждые 30 мин (на 00 и 30), ночью каждый час (на 00)
            "lenta_day": TaskConfig(
                name="Дневной парсинг Lenta.ru (08:00-22:00)",
                cron="0,30 8-21 * * *",
                enabled=True,
                kwargs={"limit": 30, "categories": LENTA_CATEGORIES},
            ),
            "lenta_night": TaskConfig(
                name="Ночной парсинг Lenta.ru",
                cron="0 0-7,22,23 * * *",
                enabled=True,
                kwargs={"limit": 25, "categories": LENTA_CATEGORIES},
            ),
            # TInvest: сдвинут на 7 минут (днем каждые 15 мин, ночью каждые 30 мин)
            "tinvest_day": TaskConfig(
                name="Дневной парсинг TInvest (08:00-22:00)",
                cron="7,22,37,52 8-21 * * *",  # 8:07, 8:22, 8:37, 8:52...
                enabled=True,
                kwargs={"limit": 20, "tickers": TINVEST_TICKERS},
            ),
            "tinvest_night": TaskConfig(
                name="Ночной парсинг TInvest",
                cron="7,37 0-7,22,23 * * *",  # 22:07, 22:37, 23:07, 0:07, 0:37...
                enabled=True,  # и так до 7:37
                kwargs={"limit": 25, "tickers": TINVEST_TICKERS},
            ),
        }

        # Пробуем загрузить из YAML
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
