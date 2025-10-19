import os
import pandas as pd
from pathlib import Path


def get_csv_structure():
    """Получить структуру CSV файлов в текущей папке"""

    # Получаем текущую папку
    current_dir = Path('.')

    # Находим все CSV файлы, которые не начинаются с _
    csv_files = [f for f in current_dir.glob('*.csv') if not f.name.startswith('_')]

    # Сортируем файлы по имени
    csv_files.sort()

    # Создаем список для хранения информации
    structure_info = []

    for csv_file in csv_files:
        try:
            # Читаем CSV файл
            df = pd.read_csv(csv_file)

            # Получаем названия полей
            columns = list(df.columns)

            # Добавляем информацию в список
            structure_info.append({
                'file_name': csv_file.name,
                'columns': columns,
                'row_count': len(df)
            })

        except Exception as e:
            print(f"Ошибка при чтении файла {csv_file}: {e}")

    return structure_info


def write_structure_to_file(structure_info, output_file='csv_structure.txt'):
    """Записать структуру в файл"""

    with open(output_file, 'w', encoding='utf-8') as f:
        f.write("Структура CSV файлов в папке:\n")
        f.write("=" * 50 + "\n\n")

        for info in structure_info:
            f.write(f"Файл: {info['file_name']}\n")
            f.write(f"Количество строк: {info['row_count']}\n")
            f.write("Поля:\n")

            for i, column in enumerate(info['columns'], 1):
                f.write(f"  {i}. {column}\n")

            f.write("\n" + "-" * 30 + "\n\n")

    print(f"Структура записана в файл: {output_file}")


def main():
    print("Анализ CSV файлов в текущей папке...")

    # Получаем структуру CSV файлов
    structure_info = get_csv_structure()

    if not structure_info:
        print("CSV файлы не найдены в текущей папке")
        return

    # Выводим информацию в консоль
    print(f"\nНайдено CSV файлов: {len(structure_info)}\n")

    for info in structure_info:
        print(f"  {info['file_name']}")
        print(f"Поля ({len(info['columns'])}): {', '.join(info['columns'])}")
        print()

    # Записываем в файл
    write_structure_to_file(structure_info)


if __name__ == "__main__":
    main()