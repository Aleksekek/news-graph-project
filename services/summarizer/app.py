#!/usr/bin/env python
"""
Точка входа для сервиса суммаризации.
Запускает генерацию часовых и дневных сводок.
"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.app.summarizer import main

if __name__ == "__main__":
    asyncio.run(main())
