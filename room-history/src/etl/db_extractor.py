"""
Модуль для извлечения данных из SQL Server и сохранения в CSV
"""

import pandas as pd
import logging
from pathlib import Path
import sys
from typing import List, Optional

# Добавляем путь к src для корректного импорта
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from src.database.db_connector import SQLServerConnector, create_db_connector_from_config

logger = logging.getLogger(__name__)


class DBExtractor:
    """Класс для извлечения данных из БД и сохранения в CSV"""

    def __init__(
            self,
            connector: SQLServerConnector,
            sql_dir: str = 'sql',
            output_dir: str = 'data/raw',
            encoding: str = 'utf-8'
    ):
        self.connector = connector
        self.encoding = encoding

        # Определяем пути относительно расположения этого файла
        current_file_path = Path(__file__).parent
        project_root = current_file_path.parent.parent

        self.sql_dir = project_root / sql_dir
        self.output_dir = project_root / output_dir

        self.output_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"SQL директория: {self.sql_dir}")
        logger.info(f"Output директория: {self.output_dir}")

    def read_sql_file(self, filepath: Path) -> str:
        """Читает SQL-запрос из файла"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:
                return f.read().strip()
        except Exception as e:
            logger.error(f"Ошибка чтения SQL файла {filepath}: {e}")
            raise

    def execute_sql_to_csv(self, sql_filenames: List[str] = None):
        """Выполняет SQL файлы и сохраняет результаты в CSV"""

        if sql_filenames is None:
            # Если файлы не указаны, используем все .sql файлы
            sql_files = list(self.sql_dir.glob('*.sql'))
            # Исключаем .template файлы
            sql_files = [f for f in sql_files if not f.name.endswith('.template')]
        else:
            # Используем указанные файлы
            sql_files = [self.sql_dir / filename for filename in sql_filenames]

        logger.info(f"Найдено {len(sql_files)} SQL файлов для обработки")

        for sql_path in sql_files:
            if not sql_path.exists():
                logger.warning(f"SQL файл не найден: {sql_path}")
                continue

            logger.info(f"Обработка файла: {sql_path.name}")

            try:
                # Читаем SQL запрос
                sql_query = self.read_sql_file(sql_path)

                # Проверяем наличие плейсхолдеров
                if '{' in sql_query and '}' in sql_query:
                    logger.info(f"Файл {sql_path.name} содержит плейсхолдеры, требуется специальная обработка")

                    # Для extract_statuses.sql нужно сначала получить lease_id из extract_rooms.csv
                    if sql_path.name == 'extract_statuses.sql':
                        self._process_statuses_with_placeholder(sql_query, sql_path)
                    else:
                        logger.warning(f"Неизвестный плейсхолдер в файле {sql_path.name}")
                    continue

                # Выполняем обычный запрос без плейсхолдеров
                df = self.connector.execute_query(sql_query)

                # Сохраняем в CSV
                self._save_to_csv(df, sql_path)

            except Exception as e:
                logger.error(f"❌ Ошибка при выполнении {sql_path.name}: {e}")

    def _process_statuses_with_placeholder(self, sql_template: str, sql_path: Path):
        """Обрабатывает extract_statuses.sql с плейсхолдером"""
        try:
            # Читаем lease_id из ранее созданного файла extract_rooms.csv
            rooms_csv_path = self.output_dir / 'extract_rooms.csv'

            if not rooms_csv_path.exists():
                logger.error(f"Файл {rooms_csv_path} не найден. Сначала выполните extract_rooms.sql")
                return

            # Читаем CSV с lease_id
            df_rooms = pd.read_csv(rooms_csv_path)
            lease_ids = df_rooms['lease_id'].dropna().unique().tolist()

            logger.info(f"Найдено {len(lease_ids)} уникальных lease_id")

            if not lease_ids:
                logger.warning("Не найдено lease_id для обработки")
                return

            # Обрабатываем чанками чтобы избежать слишком длинных SQL запросов
            chunk_size = 500  # Уменьшаем размер чанка для надежности
            all_chunks = []

            for i in range(0, len(lease_ids), chunk_size):
                chunk_lease_ids = lease_ids[i:i + chunk_size]
                ids_str = ','.join(str(int(lease_id)) for lease_id in chunk_lease_ids)

                # Заменяем плейсхолдер
                sql_query = sql_template.replace('{lease_id_placeholder}', ids_str)

                logger.info(f"Обработка чанка {i // chunk_size + 1} с {len(chunk_lease_ids)} lease_id")

                try:
                    df_chunk = self.connector.execute_query(sql_query)
                    all_chunks.append(df_chunk)
                    logger.info(f"Чанк {i // chunk_size + 1}: получено {len(df_chunk)} записей")
                except Exception as e:
                    logger.error(f"Ошибка в чанке {i // chunk_size + 1}: {e}")
                    # Продолжаем с следующим чанком
                    continue

            if not all_chunks:
                logger.warning("Не удалось получить данные ни из одного чанка")
                return

            # Объединяем все чанки
            df_result = pd.concat(all_chunks, ignore_index=True)

            # Сохраняем результат
            self._save_to_csv(df_result, sql_path)

        except Exception as e:
            logger.error(f"Ошибка при обработке статусов: {e}")

    def _save_to_csv(self, df: pd.DataFrame, sql_path: Path):
        """Сохраняет DataFrame в CSV файл"""
        csv_filename = sql_path.stem + '.csv'
        output_path = self.output_dir / csv_filename
        df.to_csv(output_path, index=False, encoding=self.encoding)

        logger.info(f"✅ Успешно: {len(df)} записей сохранено в {csv_filename}")
        logger.info(f"Столбцы: {list(df.columns)}")

        # Показываем пример данных
        if len(df) > 0:
            logger.info("Пример данных (первые 2 строки):")
            for i, (_, row) in enumerate(df.head(2).iterrows()):
                logger.info(f"  Строка {i + 1}:")
                for col in df.columns:
                    value = row[col]
                    if pd.isna(value):
                        display_value = "NULL"
                    elif hasattr(value, 'isoformat'):
                        display_value = value.isoformat()
                    else:
                        display_value = str(value)
                    logger.info(f"    {col}: {display_value}")
        else:
            logger.warning("Запрос вернул 0 записей")

    def test_connection(self):
        """Тестирует подключение к БД"""
        return self.connector.test_connection()


def extract_data():
    """Основная функция для извлечения данных"""
    try:
        connector = create_db_connector_from_config()

        if not connector.test_connection():
            logger.error("Не удалось подключиться к БД")
            return False

        extractor = DBExtractor(connector=connector)

        # Выполняем SQL файлы в правильном порядке
        sql_files_to_execute = [
            'extract_rooms.sql',  # Сначала получаем помещения
            'extract_statuses.sql'  # Затем статусы (использует данные из rooms)
        ]

        extractor.execute_sql_to_csv(sql_files_to_execute)

        logger.info("✅ Извлечение данных завершено")
        return True

    except Exception as e:
        logger.error(f"Ошибка: {e}")
        return False


if __name__ == "__main__":
    # Настройка логирования
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    success = extract_data()
    if not success:
        sys.exit(1)