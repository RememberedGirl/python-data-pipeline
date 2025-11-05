import pandas as pd
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


class HistoryProcessor:
    def __init__(self):
        self.data_dir = project_root / 'data' / 'raw'
        self.output_dir = project_root / 'data' / 'processed'
        self.output_dir.mkdir(parents=True, exist_ok=True)

    def load_data(self) -> pd.DataFrame:
        file_path = self.data_dir / 'extract_history.csv'
        return pd.read_csv(file_path)

    def add_primary_key_to_legal_unit(self):
        """Добавляет первичный ключ в справочник legal_entity + unit_id"""
        legal_unit_path = self.data_dir / 'ref_legal_unit.csv'

        if not legal_unit_path.exists():
            print("Предупреждение: ref_legal_unit.csv не найден")
            return pd.DataFrame()

        df_legal_unit = pd.read_csv(legal_unit_path)

        # Проверяем, есть ли уже первичный ключ
        if 'legal_unit_id' in df_legal_unit.columns:
            print("Справочник ref_legal_unit.csv уже содержит первичный ключ")
            return df_legal_unit

        # Добавляем автоинкрементный первичный ключ
        df_legal_unit = df_legal_unit.reset_index(drop=True)
        df_legal_unit.insert(0, 'legal_unit_id', range(1, len(df_legal_unit) + 1))

        print(f"Добавлен первичный ключ legal_unit_id для {len(df_legal_unit)} записей")
        return df_legal_unit

    def process_history(self, df: pd.DataFrame) -> pd.DataFrame:
        """Обрабатывает исторические данные и добавляет вторичный ключ"""
        # Сначала получаем обогащенный справочник legal_unit
        df_legal_unit = self.add_primary_key_to_legal_unit()

        if df_legal_unit.empty:
            print("Предупреждение: не удалось загрузить справочник legal_unit для добавления вторичного ключа")
            # Продолжаем обработку без вторичного ключа
            legal_unit_id = None
        else:
            # Сохраняем промежуточный справочник
            self.save_to_csv(df_legal_unit, 'processed_ref_legal_unit.csv')

        def process_group(group):
            group = group.sort_values('status_sequence')
            non_zero_leases = group[group['lease_id'] != 0]

            previous_tenants = []
            future_tenants = []

            for _, row in group.iterrows():
                current_seq = row['status_sequence']

                # Предыдущий не нулевой lease_id (включая текущий если не 0)
                previous = non_zero_leases[non_zero_leases['status_sequence'] <= current_seq]
                previous_tenant = previous.iloc[-1]['lease_id'] if not previous.empty else None

                # Следующий не нулевой lease_id (исключая текущий)
                future = non_zero_leases[non_zero_leases['status_sequence'] > current_seq]
                future_tenant = future.iloc[0]['lease_id'] if not future.empty else None

                # Если текущий lease_id не 0, оставляем его в previous_tenant
                if row['lease_id'] != 0:
                    previous_tenant = row['lease_id']

                previous_tenants.append(previous_tenant)
                future_tenants.append(future_tenant)

            group = group.copy()
            group['previous_tenant'] = previous_tenants
            group['future_tenant'] = future_tenants

            return group

        # Обрабатываем группы
        result = df.groupby(['model_id', 'unit_id', 'legal_entity']).apply(process_group).reset_index(drop=True)

        # ДОБАВЛЯЕМ ВТОРИЧНЫЙ КЛЮЧ
        if not df_legal_unit.empty:
            # Объединяем с справочником чтобы добавить legal_unit_id
            result = pd.merge(
                result,
                df_legal_unit[['legal_entity', 'unit_id', 'legal_unit_id']],
                on=['legal_entity', 'unit_id'],
                how='left'
            )

            # Перемещаем legal_unit_id в начало для удобства
            cols = ['legal_unit_id'] + [col for col in result.columns if col != 'legal_unit_id']
            result = result[cols]

            print(f"Добавлен вторичный ключ legal_unit_id в исторические данные")
        else:
            # Добавляем пустой столбец если справочник не загружен
            result['legal_unit_id'] = None

        # Дополнительная проверка и преобразование на уровне всего DataFrame
        result['previous_tenant'] = result['previous_tenant'].astype('Int64')
        result['future_tenant'] = result['future_tenant'].astype('Int64')
        result['legal_unit_id'] = result['legal_unit_id'].astype('Int64')

        return result

    def save_to_csv(self, df: pd.DataFrame, filename: str):
        output_path = self.output_dir / filename
        df.to_csv(output_path, index=False, encoding='utf-8')

    def add_fact_to_reference(self):
        """Добавляет запись '666 Факт null' в справочник моделей"""
        ref_model_path = self.data_dir / 'ref_model.csv'

        df_ref = pd.read_csv(ref_model_path)

        fact_record = pd.DataFrame({
            'model_id': [666],
            'model_type': ['Факт'],
            'forecast_year': ['все']
        })
        df_ref = pd.concat([df_ref, fact_record], ignore_index=True)

        # Сначала создаем функцию для безопасного преобразования
        def safe_convert(x):
            # Если значение NaN или пустое - возвращаем 'все'
            if pd.isna(x) or x == '' or x is None:
                return 'все'
            # Если уже 'все' - возвращаем как есть
            if x == 'все':
                return 'все'
            try:
                # Пробуем преобразовать в float, затем в int, затем в строку
                return str(int(float(x)))
            except (ValueError, TypeError):
                # Если не получается преобразовать - возвращаем 'все'
                return 'все'

        # Применяем функцию ко всем значениям
        df_ref['forecast_year'] = df_ref['forecast_year'].apply(safe_convert)

        return df_ref

    def create_expert_history(self):
        """Создает историю экспертов с заменой TrcShoppingMall на legal_entity и добавляет вторичный ключ"""
        try:
            # Загружаем mapping_trc.csv
            mapping_path = self.output_dir / 'mapping_trc.csv'
            if not mapping_path.exists():
                print("Предупреждение: mapping_trc.csv не найден")
                return pd.DataFrame()

            df_mapping = pd.read_csv(mapping_path)

            # Загружаем expert.csv
            expert_path = self.data_dir / 'expert.csv'
            if not expert_path.exists():
                print("Предупреждение: expert.csv не найден")
                return pd.DataFrame()

            df_expert = pd.read_csv(expert_path)

            print(f"Количество записей в expert.csv: {len(df_expert)}")

            # Создаем маппинг из crm названий в legal_entity
            mapping_dict = dict(zip(df_mapping['crm'], df_mapping['legal_entity']))

            # Заменяем TrcShoppingMall на legal_entity
            df_expert['legal_entity'] = df_expert['TrcShoppingMall'].map(mapping_dict)

            # Удаляем строки где не удалось смапить legal_entity
            original_count = len(df_expert)
            df_expert = df_expert.dropna(subset=['legal_entity'])
            removed_count = original_count - len(df_expert)

            print(f"Удалено несмапленных записей: {removed_count}")

            # Переименовываем колонки
            df_expert = df_expert.rename(columns={
                'TrcUnitNumber': 'unit_id',
                'TrcIsChief': 'is_chief',
                'TrcContactFullName': 'contact_full_name',
                'TrcRespStartDate': 'resp_start_date',
                'TrcRespEndDate': 'resp_end_date',
                'ModifiedOn': 'modified_on',
                'TrcBooleanActive': 'is_active'
            })

            # ДОБАВЛЯЕМ ВТОРИЧНЫЙ КЛЮЧ
            # Загружаем processed_ref_legal_unit.csv
            legal_unit_path = self.output_dir / 'processed_ref_legal_unit.csv'
            if legal_unit_path.exists():
                df_legal_unit = pd.read_csv(legal_unit_path)

                # Объединяем с справочником чтобы добавить legal_unit_id
                df_expert = pd.merge(
                    df_expert,
                    df_legal_unit[['legal_entity', 'unit_id', 'legal_unit_id']],
                    on=['legal_entity', 'unit_id'],
                    how='left'
                )
                # Преобразуем legal_unit_id в Int64 для целочисленного типа
                df_expert['legal_unit_id'] = df_expert['legal_unit_id'].astype('Int64')

                # Перемещаем legal_unit_id в начало для удобства
                cols = ['legal_unit_id'] + [col for col in df_expert.columns if col != 'legal_unit_id']
                df_expert = df_expert[cols]

                print(f"Добавлен вторичный ключ legal_unit_id в историю экспертов")
            else:
                # Добавляем пустой столбец если справочник не загружен
                df_expert['legal_unit_id'] = None
                print("Предупреждение: processed_ref_legal_unit.csv не найден, вторичный ключ не добавлен")

            # Выбираем только нужные колонки
            final_columns = [
                'legal_unit_id', 'unit_id', 'legal_entity', 'is_chief', 'contact_full_name',
                'resp_start_date', 'resp_end_date', 'modified_on', 'is_active'
            ]

            df_expert_final = df_expert[final_columns]

            print(f"Создана история экспертов: {len(df_expert_final)} записей")

            return df_expert_final

        except Exception as e:
            print(f"Ошибка при создании истории экспертов: {e}")
            return pd.DataFrame()

    def add_foreign_key_to_tenants(self):
        """Добавляет вторичный ключ в extract_tenants.csv"""
        try:
            # Загружаем extract_tenants.csv
            tenants_path = self.data_dir / 'extract_tenants.csv'
            if not tenants_path.exists():
                print("Предупреждение: extract_tenants.csv не найден")
                return pd.DataFrame()

            df_tenants = pd.read_csv(tenants_path)

            print(f"Количество записей в extract_tenants.csv: {len(df_tenants)}")

            # Загружаем processed_ref_legal_unit.csv
            legal_unit_path = self.output_dir / 'processed_ref_legal_unit.csv'
            if legal_unit_path.exists():
                df_legal_unit = pd.read_csv(legal_unit_path)

                # Объединяем с справочником чтобы добавить legal_unit_id
                df_tenants = pd.merge(
                    df_tenants,
                    df_legal_unit[['legal_entity', 'unit_id', 'legal_unit_id']],
                    on=['legal_entity', 'unit_id'],
                    how='left'
                )

                # Преобразуем legal_unit_id в Int64 для целочисленного типа
                df_tenants['legal_unit_id'] = df_tenants['legal_unit_id'].astype('Int64')

                # Перемещаем legal_unit_id в начало для удобства
                cols = ['legal_unit_id'] + [col for col in df_tenants.columns if col != 'legal_unit_id']
                df_tenants = df_tenants[cols]

                print(f"Добавлен вторичный ключ legal_unit_id в extract_tenants.csv")
            else:
                # Добавляем пустой столбец если справочник не загружен
                df_tenants['legal_unit_id'] = None
                print("Предупреждение: processed_ref_legal_unit.csv не найден, вторичный ключ не добавлен")

            return df_tenants

        except Exception as e:
            print(f"Ошибка при добавлении вторичного ключа в extract_tenants: {e}")
            return pd.DataFrame()

def process_history_data():
    processor = HistoryProcessor()

    # Обрабатываем исторические данные (теперь с добавлением вторичного ключа)
    df = processor.load_data()
    df_processed = processor.process_history(df)  # Этот метод теперь сам создает processed_ref_legal_unit.csv
    processor.save_to_csv(df_processed, 'processed_history.csv')

    # Обогащаем справочник моделей
    df_ref = processor.add_fact_to_reference()
    processor.save_to_csv(df_ref, 'processed_ref_model.csv')

    # Создаем историю экспертов
    df_expert = processor.create_expert_history()
    processor.save_to_csv(df_expert, 'processed_expert_history.csv')

    # Добавляем вторичный ключ в tenants
    df_tenants = processor.add_foreign_key_to_tenants()
    processor.save_to_csv(df_tenants, 'processed_tenants.csv')

    return True


if __name__ == "__main__":
    process_history_data()