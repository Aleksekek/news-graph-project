"""
Фикстуры и настройка путей для pytest.
"""

import sys
import warnings
from pathlib import Path

# Добавляем корень проекта в PYTHONPATH
root_dir = Path(__file__).parent.parent
sys.path.insert(0, str(root_dir))

# Подавляем предупреждения от python-telegram-bot
try:
    from telegram.warnings import PTBUserWarning
    warnings.filterwarnings("ignore", category=PTBUserWarning, message=".*CallbackQueryHandler.*")
except ImportError:
    pass