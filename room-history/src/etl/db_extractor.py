"""
Модуль для извлечения данных из SQL Server и сохранения в CSV
"""

import pandas as pd  # Импорт библиотеки pandas для работы с данными в табличном формате
from pathlib import Path  # Импорт для работы с путями файловой системы
import sys  # Импорт системных функций


# Добавляем путь к src для корректного импорта
project_root = Path(__file__).parent.parent.parent  # Получаем путь к корню проекта (три уровня вверх)

# sys.path - это список путей, где Python ищет модули и пакеты при выполнении import
# До: sys.path содержит стандартные пути Python
# ['/usr/lib/python3.8', '/usr/local/lib/python3.8/site-packages', ...]
# После: добавляется путь к проекту в начало
# ['/home/user/my_project', '/usr/lib/python3.8', '/usr/local/lib/python3.8/site-packages', ...]

sys.path.insert(0, str(project_root))  # Добавляем корень проекта в путь для импорта модулей


from src.database.db_connector import SQLServerConnector, create_db_connector_from_config  # Импорт классов для работы с БД


class DBExtractor:
    """Класс для извлечения данных из БД и сохранения в CSV"""

    def __init__(
            self,
            connector: SQLServerConnector,  # Объект соединения с БД
            sql_dir: str = 'sql',  # Директория с SQL-файлами
            output_dir: str = 'data/raw',  # Директория для сохранения результатов
            encoding: str = 'utf-8'  # Кодировка файлов
    ):
        self.connector = connector  # Сохраняем соединение с БД
        self.encoding = encoding  # Сохраняем кодировку

        # Определяем пути относительно расположения этого файла
        self.sql_dir = project_root / sql_dir  # Формируем полный путь к директории с SQL-файлами
        self.output_dir = project_root / output_dir  # Формируем полный путь к директории для вывода

        self.output_dir.mkdir(parents=True, exist_ok=True)  # Создаем директорию для вывода (если не существует)

    def read_sql_file(self, filepath: Path) -> str:
        """Читает SQL-запрос из файла"""
        try:
            with open(filepath, 'r', encoding='utf-8') as f:  # Открываем файл для чтения в кодировке UTF-8
                return f.read().strip()  # Читаем содержимое и удаляем пробелы по краям
        except Exception as e:
            print(f"Ошибка чтения SQL файла {filepath}: {e}")  # Выводим ошибку чтения файла
            raise  # Пробрасываем исключение дальше

    def save_to_csv(self, df: pd.DataFrame, sql_path: Path):
        """Сохраняет DataFrame в CSV файл"""

        # path.stem - имя файла без расширения
        csv_filename = sql_path.stem + '.csv'  # Формируем имя CSV-файла на основе имени SQL-файла

        # оператор / для объединения объектов Path (или Path со строкой) в корректный путь файловой системы.
        output_path = self.output_dir / csv_filename  # Формируем полный путь для сохранения
        df.to_csv(output_path, index=False, encoding=self.encoding)  # Сохраняем DataFrame в CSV без индексов

        print(f"Успешно: {len(df)} записей сохранено в {csv_filename}")  # Выводим сообщение об успешном сохранении

    def test_connection(self):
        """Тестирует подключение к БД"""
        return self.connector.test_connection()  # Вызываем метод тестирования подключения

    def create_reference_table(self, df_master: pd.DataFrame, columns: list):
        """Создает справочник с уникальными сочетаниями указанных колонок"""
        if not columns:  # Проверяем что массив колонок не пустой
            return

        # Проверяем что все указанные колонки существуют в DataFrame
        missing_columns = [col for col in columns if col not in df_master.columns]
        if missing_columns:
            print(f"Предупреждение: колонки {missing_columns} не найдены в данных")
            return

        # Создаем справочник с уникальными сочетаниями колонок
        ref_df = df_master[columns].drop_duplicates().reset_index(drop=True)

        # Формируем имя файла из первых слов всех колонок
        column_prefixes = []
        for column in columns:
            # Берем первое слово до первого символа подчеркивания
            first_word = column.split('_')[0]  # Разделяем по '_' и берем первую часть
            column_prefixes.append(first_word)

        # Объединяем все префиксы в имя файла
        base_filename = "ref_" + "_".join(column_prefixes)

        # Сохраняем в CSV
        self.save_to_csv(ref_df, Path(base_filename))

        # Сохраняем в CSV
        self.save_to_csv(ref_df, Path(base_filename))

        print(f"Создан справочник {base_filename}.csv с {len(ref_df)} уникальными записями")

    def extract_history(self) -> pd.DataFrame:
        """
        Извлекает исторические данные из БД

        Returns:
            pd.DataFrame: DataFrame с историческими данными
        """
        try:
            # Формируем путь к SQL-файлу с историческими данными
            history_sql_path = self.sql_dir / 'extract_history.sql'

            # Читаем SQL-запрос из файла
            sql_query = self.read_sql_file(history_sql_path)

            # Выполняем SQL-запрос и получаем DataFrame
            df_history = self.connector.execute_query(sql_query)

            # Сохраняем исторические данные в CSV
            self.save_to_csv(df_history, history_sql_path)

            # Создаем справочник статусов из исторических данных
            self.create_status_reference(df_history)

            return df_history

        except Exception as e:
            print(f"Ошибка при извлечении исторических данных: {e}")
            return pd.DataFrame()  # Возвращаем пустой DataFrame при ошибке


    def get_master_reference(self) -> pd.DataFrame:
        """
        Получает мастер-справочник уникальных идентификаторов

        Returns:
            pd.DataFrame: Мастер-справочник с колонками model_id, unit_id, lease_id, legal_entity
        """

        master_sql_path = self.sql_dir / 'extract_master_reference.sql'  # Формируем путь к SQL-файлу
        sql_query = self.read_sql_file(master_sql_path)  # Читаем SQL из файла
        df_master = self.connector.execute_query(sql_query)  # Выполняем SQL-запрос и получаем DataFrame
        # Сохраняем мастер-справочник
        self.save_to_csv(df_master, master_sql_path)

        reference_configs = [
            ['model_id'],
            ['lease_id'],
            ['legal_entity'],
            ['trc_abbreviation'],
            ['model_id', 'legal_entity', 'unit_id'],
            ['legal_entity', 'unit_id']
        ]

        for columns in reference_configs:
            self.create_reference_table(df_master, columns)

        return df_master  # Возвращаем мастер-справочник

    def create_status_reference(self, df_history: pd.DataFrame):
        """
        Создает справочник статусов из исторических данных

        Args:
            df_history: DataFrame с историческими данными из extract_history.csv
        """
        try:
            # Проверяем наличие необходимых колонок
            required_columns = ['crm_status']
            missing_columns = [col for col in required_columns if col not in df_history.columns]

            if missing_columns:
                print(f"Предупреждение: отсутствуют колонки {missing_columns} для создания справочника статусов")
                return

            # Создаем справочник с уникальными статусами
            status_ref = df_history[['crm_status']].drop_duplicates().reset_index(drop=True)

            # Сортируем для удобства
            status_ref = status_ref.sort_values('crm_status').reset_index(drop=True)

            # Сохраняем справочник
            self.save_to_csv(status_ref, Path('ref_crm_status'))

            print(f"Создан справочник статусов: {len(status_ref)} уникальных записей")

        except Exception as e:
            print(f"Ошибка при создании справочника статусов: {e}")


    def extract_tenants_with_placeholder(self):
        """Извлекает данные арендаторов чанками по ref_lease_ids.csv"""

        # Читаем lease_id из справочника
        lease_ref_path = self.output_dir / 'ref_lease.csv'  # Формируем путь к файлу со lease_id
        if not lease_ref_path.exists():  # Проверяем существует ли файл
            return  # Если файла нет, выходим из метода

        df_lease_ref = pd.read_csv(lease_ref_path)  # Читаем CSV-файл с lease_id
        lease_ids = df_lease_ref['lease_id'].dropna().tolist()  # Получаем список уникальных lease_id без NaN

        if not lease_ids:  # Проверяем есть ли lease_id для обработки
            return  # Если список пуст, выходим из метода

        # Читаем SQL шаблон
        sql_template_path = self.sql_dir / 'extract_tenants.sql'  # Формируем путь к SQL-шаблону
        if not sql_template_path.exists():  # Проверяем существует ли файл
            return  # Если файла нет, выходим из метода

        sql_template = self.read_sql_file(sql_template_path)  # Читаем SQL-шаблон из файла

        # Обрабатываем чанками
        chunk_size = 500  # Размер чанка (количество lease_id за один запрос)
        all_chunks = []  # Список для хранения всех чанков данных

        for i in range(0, len(lease_ids), chunk_size):  # Итерируемся по lease_id с шагом chunk_size
            chunk_lease_ids = lease_ids[i:i + chunk_size]  # Получаем текущий чанк lease_id
            ids_str = ','.join(str(int(lease_id)) for lease_id in chunk_lease_ids)  # Преобразуем в строку через запятую

            # Заменяем плейсхолдер в SQL
            sql_query = sql_template.replace('{lease_id_placeholder}', ids_str)  # Подставляем lease_id в SQL-запрос

            try:
                df_chunk = self.connector.execute_query(sql_query)  # Выполняем SQL-запрос для текущего чанка
                all_chunks.append(df_chunk)  # Добавляем результат в список чанков
            except Exception:  # Перехватываем любые исключения
                continue  # Продолжаем обработку следующих чанков при ошибке

        if not all_chunks:  # Проверяем, есть ли данные в чанках
            return  # Если нет данных, выходим из метода

        # Объединяем все чанки
        df_result = pd.concat(all_chunks, ignore_index=True)  # Объединяем все чанки в один DataFrame

        # Сохраняем результат
        output_path = self.output_dir / 'extract_tenants.csv'  # Формируем путь для сохранения результата
        df_result.to_csv(output_path, index=False, encoding=self.encoding)  # Сохраняем объединенные данные в CSV

        return df_result  # Возвращаем результат

    def enrich_models_reference(self) -> pd.DataFrame:
        """
        Обогащает существующий справочник моделей дополнительными данными
        и сохраняет в тот же файл ref_model.csv

        Returns:
            pd.DataFrame: Обогащенный справочник моделей
        """
        try:
            # Читаем существующий справочник моделей
            ref_model_path = self.output_dir / 'ref_model.csv'
            if not ref_model_path.exists():
                print("Предупреждение: ref_model.csv не найден")
                return pd.DataFrame()

            df_ref_model = pd.read_csv(ref_model_path)

            if df_ref_model.empty:
                print("Предупреждение: ref_model.csv пуст")
                return pd.DataFrame()

            # Получаем список model_id для фильтрации
            model_ids = df_ref_model['model_id'].dropna().unique().tolist()

            if not model_ids:
                print("Предупреждение: не найдено model_id в справочнике")
                return pd.DataFrame()

            # Формируем путь к SQL-файлу с моделями
            models_sql_path = self.sql_dir / 'extract_models.sql'

            # Читаем SQL-запрос из файла
            sql_query = self.read_sql_file(models_sql_path)

            # Подставляем model_ids в запрос
            ids_str = ','.join(str(int(model_id)) for model_id in model_ids if pd.notna(model_id))
            sql_query = sql_query.replace('{model_id}', ids_str)

            # Выполняем SQL-запрос и получаем дополнительные данные
            df_models_additional = self.connector.execute_query(sql_query)

            if df_models_additional.empty:
                print("Предупреждение: не найдено дополнительных данных по моделям")
                return df_ref_model

            # Объединяем существующий справочник с дополнительными данными
            df_enriched = pd.merge(
                df_ref_model,
                df_models_additional,
                on='model_id',
                how='left',
                suffixes=('', '_additional')
            )

            # Удаляем дублирующиеся колонки (если есть)
            df_enriched = df_enriched.loc[:, ~df_enriched.columns.duplicated()]

            # Сохраняем обогащенный справочник В ТОТ ЖЕ ФАЙЛ
            df_enriched.to_csv(ref_model_path, index=False, encoding=self.encoding)

            print(f"Обогащенный справочник моделей сохранен в ref_model.csv: {len(df_enriched)} записей")
            return df_enriched

        except Exception as e:
            print(f"Ошибка при обогащении справочника моделей: {e}")
            return pd.DataFrame()

def extract_data():
    """Основная функция для извлечения данных"""
    try:
        connector = create_db_connector_from_config()  # Создаем соединение с БД из конфигурации

        if not connector.test_connection():  # Проверяем подключение к БД
            return False  # Возвращаем False если подключение не удалось

        extractor = DBExtractor(connector=connector)  # Создаем экземпляр extractor

        # Получаем мастер-справочник
        extractor.get_master_reference()  # Вызываем метод получения мастер-справочника

        # Извлекаем исторические данные
        extractor.extract_history()

        # Извлекаем данные арендаторов чанками
        extractor.extract_tenants_with_placeholder()  # Вызываем метод извлечения данных арендаторов

        extractor.enrich_models_reference()

        return True  # Возвращаем True при успешном выполнении

    except Exception as e:  # Перехватываем любые исключения
        print(f"Ошибка при извлечении данных: {e}")  # Выводим сообщение об ошибке
        return False  # Возвращаем False при возникновении ошибки


if __name__ == "__main__":
    success = extract_data()  # Вызываем основную функцию извлечения данных
    if not success:  # Проверяем успешность выполнения
        sys.exit(1)  # Завершаем программу с кодом ошибки 1 если были проблемы