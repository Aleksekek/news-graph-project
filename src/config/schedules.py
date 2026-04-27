"""
Конфигурация расписания задач.
Загружается из YAML файла с дефолтами.
"""

import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import yaml


@dataclass
class TaskConfig:
    """Конфигурация задачи планировщика."""

    name: str
    cron: str
    enabled: bool = True
    kwargs: Dict[str, Any] = field(default_factory=dict)


class ScheduleConfig:
    """Конфигурация расписания задач."""

    def __init__(self, config_path: Optional[str] = None):
        if config_path is None:
            # Ищем в config/schedule_config.yaml относительно корня
            base_dir = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
            config_path = os.path.join(base_dir, "config", "schedule_config.yaml")

        self.config_path = config_path
        self.tasks = self._load_config()

    def _load_config(self) -> Dict[str, TaskConfig]:
        """Загрузка конфигурации с дефолтами."""

        # Дефолтные задачи
        default_tasks = {
            "lenta_hourly": TaskConfig(
                name="Часовой парсинг Lenta.ru",
                cron="15,45 * * * *",
                enabled=True,
                kwargs={"limit": 50, "categories": LENTA_CATEGORIES},
            ),
            "tinvest_hourly": TaskConfig(
                name="Часовой парсинг TInvest",
                cron="0,30 * * * *",
                enabled=True,
                kwargs={"limit": 50, "tickers": TINVEST_TICKERS},
            ),
        }

        # Пробуем загрузить из YAML
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, "r", encoding="utf-8") as f:
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

    def get_enabled_tasks(self) -> List[TaskConfig]:
        """Получение только включённых задач."""
        return [task for task in self.tasks.values() if task.enabled]


# Для избежания циклических импортов
from src.core.constants import LENTA_CATEGORIES, TINVEST_TICKERS

# Глобальный экземпляр
schedule_config = ScheduleConfig()
