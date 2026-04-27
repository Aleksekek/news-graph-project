"""
Фикстуры и настройка путей для pytest.
"""

import sys
from pathlib import Path

# Добавляем корень проекта в PYTHONPATH
root_dir = Path(__file__).parent.parent
sys.path.insert(0, str(root_dir))

# Проверка (можно убрать после отладки)
print(f"✅ PYTHONPATH configured: {root_dir}")
