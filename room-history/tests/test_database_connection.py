# tests/test_database_connection.py
"""
Минимальный тест подключения к БД
"""

import pytest
import sys
from pathlib import Path

# Добавляем путь к проекту
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

try:
    from src.database.db_connector import create_db_connector_from_config
except ImportError:
    pytest.skip("Не удалось импортировать модули", allow_module_level=True)


def test_database_connection():
    """Простой тест подключения к БД"""
    connector = create_db_connector_from_config()
    is_connected = connector.test_connection()

    assert is_connected == True, "Не удалось подключиться к БД"


if __name__ == "__main__":
    pytest.main([__file__, "-v"])