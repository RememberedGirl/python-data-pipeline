"""
Утилита для загрузки выгруженных CSV файлов
"""

import pandas as pd
import logging
from pathlib import Path
import json

logger = logging.getLogger(__name__)


class CSVLoader:
    """Класс для загрузки данных из CSV"""

    def __init__(self):
        self.data_dir = Path(__file__).parent.parent.parent / "data" / "raw"

    def load_rooms_data(self) -> pd.DataFrame:
        """Загрузка данных о помещениях из CSV"""
        file_path = self.data_dir / "rooms.csv"

        if not file_path.exists():
            logger.error(f"Файл не найден: {file_path}")
            return pd.DataFrame()

        logger.info(f"Загружаем данные о помещениях из: {file_path}")
        df = pd.read_csv(file_path, encoding='utf-8')
        logger.info(f"Загружено {len(df)} записей о помещениях")
        return df

    def load_statuses_data(self) -> pd.DataFrame:
        """Загрузка истории статусов из CSV"""
        file_path = self.data_dir / "statuses.csv"

        if not file_path.exists():
            logger.error(f"Файл не найден: {file_path}")
            return pd.DataFrame()

        logger.info(f"Загружаем историю статусов из: {file_path}")
        df = pd.read_csv(file_path, encoding='utf-8')
        logger.info(f"Загружено {len(df)} записей истории статусов")
        return df

    def get_extraction_info(self) -> dict:
        """Получение информации о последней выгрузке"""
        metadata_file = self.data_dir / "extraction_metadata.json"

        if not metadata_file.exists():
            return {}

        with open(metadata_file, 'r', encoding='utf-8') as f:
            return json.load(f)

    def list_available_files(self) -> list:
        """Список доступных CSV файлов"""
        csv_files = list(self.data_dir.glob("*.csv"))
        return [f.name for f in csv_files]