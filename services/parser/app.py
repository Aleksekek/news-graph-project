#!/usr/bin/env python
"""
Точка входа для сервиса парсинга.
Запускает планировщик задач для регулярного сбора новостей.
"""

import asyncio
import sys
from pathlib import Path

# Добавляем путь к src
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.app.scheduler import main

if __name__ == "__main__":
    asyncio.run(main())
