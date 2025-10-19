import pandas as pd
import sqlite3
import os
from pathlib import Path


def create_tables(conn):
    """Создание таблиц в SQLite"""

    # Dim tables
    dim_tables = {
        'dim_employee': '''
            CREATE TABLE IF NOT EXISTS dim_employee (
                employee_id INTEGER PRIMARY KEY,
                full_name TEXT NOT NULL
            )
        ''',
        'dim_financial_model': '''
            CREATE TABLE IF NOT EXISTS dim_financial_model (
                financial_model_id INTEGER PRIMARY KEY,
                model_name TEXT NOT NULL,
                forecast_year INTEGER,
                model_type TEXT NOT NULL
            )
        ''',
        'dim_rent_contract': '''
            CREATE TABLE IF NOT EXISTS dim_rent_contract (
                contract_id TEXT PRIMARY KEY,
                contract_number TEXT NOT NULL,
                tenant_name TEXT NOT NULL,
                start_date DATE NOT NULL,
                end_date DATE NOT NULL,
                rent_amount REAL NOT NULL,
                room_id TEXT NOT NULL,
                trc_id TEXT NOT NULL,
                legal_entity TEXT NOT NULL
            )
        ''',
        'dim_room': '''
            CREATE TABLE IF NOT EXISTS dim_room (
                room_id TEXT,
                area_sq_m REAL NOT NULL,
                floor INTEGER NOT NULL,
                trc_id TEXT NOT NULL,
                legal_entity TEXT NOT NULL,
                PRIMARY KEY (room_id, trc_id, legal_entity)
            )
        '''
    }

    # Fact tables
    fact_tables = {
        'fact_responsibility': '''
            CREATE TABLE IF NOT EXISTS fact_responsibility (
                responsibility_id INTEGER PRIMARY KEY,
                room_id TEXT NOT NULL,
                trc_id TEXT NOT NULL,
                legal_entity TEXT NOT NULL,
                start_date DATE NOT NULL,
                change_number INTEGER NOT NULL,
                employee_id INTEGER NOT NULL,
                FOREIGN KEY (room_id, trc_id, legal_entity) REFERENCES dim_room(room_id, trc_id, legal_entity),
                FOREIGN KEY (employee_id) REFERENCES dim_employee(employee_id)
            )
        ''',
        'fact_senior_responsibility': '''
            CREATE TABLE IF NOT EXISTS fact_senior_responsibility (
                responsibility_id INTEGER PRIMARY KEY,
                room_id TEXT NOT NULL,
                trc_id TEXT NOT NULL,
                legal_entity TEXT NOT NULL,
                start_date DATE NOT NULL,
                change_number INTEGER NOT NULL,
                employee_id INTEGER NOT NULL,
                FOREIGN KEY (room_id, trc_id, legal_entity) REFERENCES dim_room(room_id, trc_id, legal_entity),
                FOREIGN KEY (employee_id) REFERENCES dim_employee(employee_id)
            )
        ''',
        'fact_room_status': '''
            CREATE TABLE IF NOT EXISTS fact_room_status (
                financial_model_id INTEGER NOT NULL,
                trc_id TEXT NOT NULL,
                legal_entity TEXT NOT NULL,
                room_id TEXT NOT NULL,
                change_number INTEGER NOT NULL,
                start_date DATE NOT NULL,
                status TEXT NOT NULL,
                contract_id TEXT,
                contract_id_next TEXT,
                PRIMARY KEY (financial_model_id, trc_id, legal_entity, room_id, change_number),
                FOREIGN KEY (financial_model_id) REFERENCES dim_financial_model(financial_model_id),
                FOREIGN KEY (room_id, trc_id, legal_entity) REFERENCES dim_room(room_id, trc_id, legal_entity),
                FOREIGN KEY (contract_id) REFERENCES dim_rent_contract(contract_id),
                FOREIGN KEY (contract_id_next) REFERENCES dim_rent_contract(contract_id)
            )
        '''
    }

    # Create all tables
    cursor = conn.cursor()

    print("Creating dimension tables...")
    for table_name, create_sql in dim_tables.items():
        cursor.execute(create_sql)
        print(f"  - {table_name} created")

    print("Creating fact tables...")
    for table_name, create_sql in fact_tables.items():
        cursor.execute(create_sql)
        print(f"  - {table_name} created")

    conn.commit()


def import_csv_to_table(conn, csv_file, table_name):
    """Импорт данных из CSV в таблицу"""

    if not os.path.exists(csv_file):
        print(f"  ⚠️  CSV file not found: {csv_file}")
        return

    try:
        # Read CSV file
        df = pd.read_csv(csv_file)

        # Handle date columns
        date_columns = ['start_date', 'end_date']
        for col in date_columns:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce').dt.date

        # Import to SQLite
        df.to_sql(table_name, conn, if_exists='append', index=False)
        print(f"  ✅ {table_name}: {len(df)} rows imported")

    except Exception as e:
        print(f"  ❌ Error importing {table_name}: {str(e)}")


def main():
    # Database file
    db_file = 'mock.sqlite'

    # Remove existing database if it exists
    if os.path.exists(db_file):
        os.remove(db_file)
        print(f"Removed existing database: {db_file}")

    # Create connection
    conn = sqlite3.connect(db_file)
    print(f"Created new database: {db_file}")

    try:
        # Create tables
        create_tables(conn)
        print("\n" + "=" * 50)

        # Import data from CSV files
        print("Importing data from CSV files...")

        csv_files = [
            ('dim_employee.csv', 'dim_employee'),
            ('dim_financial_model.csv', 'dim_financial_model'),
            ('dim_rent_contract.csv', 'dim_rent_contract'),
            ('dim_room.csv', 'dim_room'),
            ('fact_responsibility.csv', 'fact_responsibility'),
            ('fact_room_status.csv', 'fact_room_status'),
            ('fact_senior_responsibility.csv', 'fact_senior_responsibility')
        ]

        for csv_file, table_name in csv_files:
            import_csv_to_table(conn, csv_file, table_name)

        print("\n" + "=" * 50)
        print("Data import completed!")

        # Show table counts
        cursor = conn.cursor()
        print("\nTable row counts:")
        tables = [
            'dim_employee', 'dim_financial_model', 'dim_rent_contract',
            'dim_room', 'fact_responsibility',
            'fact_room_status', 'fact_senior_responsibility'
        ]

        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  - {table}: {count} rows")

    except Exception as e:
        print(f"Error: {str(e)}")
    finally:
        conn.close()
        print(f"\nDatabase saved as: {db_file}")


if __name__ == "__main__":
    main()