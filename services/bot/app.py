#!/usr/bin/env python
"""
Точка входа для Telegram бота.
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.infrastructure.telegram.bot import main

if __name__ == "__main__":
    main()
