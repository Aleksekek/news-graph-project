import os
from dataclasses import dataclass
from typing import Any, Dict, List

import yaml
from pydantic import BaseModel


@dataclass
class TaskConfig:
    """Конфигурация задачи планировщика"""

    name: str
    cron: str
    enabled: bool = True
    kwargs: Dict[str, Any] = None

    def __post_init__(self):
        if self.kwargs is None:
            self.kwargs = {}


class ScheduleConfig:
    """Конфигурация расписания задач"""

    def __init__(self, config_path: str = None):
        self.config_path = config_path or os.path.join(
            os.path.dirname(__file__), "..", "..", "config", "schedule_config.yaml"
        )
        self.tasks = self._load_config()

    def _load_config(self) -> Dict[str, TaskConfig]:
        """Загрузка конфигурации из YAML файла"""
        default_config = {
            "lenta_hourly": TaskConfig(
                name="Часовой парсинг Lenta.ru",
                cron="15,45 * * * *",
                enabled=True,
                kwargs={"limit": 30},
            ),
            "lenta_weekly_archive": TaskConfig(
                name="Еженедельный архив Lenta.ru",
                cron="0 3 * * 0",
                enabled=False,
                kwargs={"days_back": 7, "max_per_day": 10},
            ),
            "tinvest_hourly": TaskConfig(
                name="Часовой парсинг Тинькофф Пульс",
                cron="30 * * * *",
                enabled=False,
                kwargs={"tickers": "SBER,VTBR", "num_posts": 50},
            ),
        }

        # Загружаем из файла, если существует
        if os.path.exists(self.config_path):
            try:
                with open(self.config_path, "r", encoding="utf-8") as f:
                    file_config = yaml.safe_load(f) or {}

                for task_id, task_data in file_config.items():
                    if task_id in default_config:
                        task = default_config[task_id]
                        for key, value in task_data.items():
                            if hasattr(task, key):
                                setattr(task, key, value)
            except Exception as e:
                print(f"⚠️ Ошибка загрузки конфигурации расписания: {e}")

        return default_config

    def get_enabled_tasks(self) -> List[TaskConfig]:
        """Получение только включенных задач"""
        return [task for task in self.tasks.values() if task.enabled]


# Глобальный экземпляр
schedule_config = ScheduleConfig()
