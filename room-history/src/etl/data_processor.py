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

    def process_history(self, df: pd.DataFrame) -> pd.DataFrame:
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

            # group['previous_tenant'] = pd.Series(previous_tenants).astype(int)
            # group['future_tenant'] = pd.Series(future_tenants).astype(int)

            return group

        result = df.groupby(['model_id', 'unit_id', 'legal_entity']).apply(process_group).reset_index(drop=True)

        # Дополнительная проверка и преобразование на уровне всего DataFrame
        result['previous_tenant'] = result['previous_tenant'].astype('Int64')
        result['future_tenant'] = result['future_tenant'].astype('Int64')

        return result

    def save_to_csv(self, df: pd.DataFrame, filename: str):
        output_path = self.output_dir / filename
        df.to_csv(output_path, index=False, encoding='utf-8')

    def add_fact_to_reference(self):
        """Добавляет запись '666 Факт null' в справочник моделей"""
        ref_model_path = self.data_dir / 'ref_model.csv'

        df_ref = pd.read_csv(ref_model_path)
        # Проверяем, есть ли уже запись с model_id = 666

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



def process_history_data():
    processor = HistoryProcessor()
    df = processor.load_data()
    df_processed = processor.process_history(df)
    processor.save_to_csv(df_processed, 'processed_history.csv')
    df_ref = processor.add_fact_to_reference()
    processor.save_to_csv(df_ref, 'processed_ref_model.csv')
    return True

if __name__ == "__main__":
    process_history_data()