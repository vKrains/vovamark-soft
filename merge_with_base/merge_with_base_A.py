import pandas as pd

# Пути к файлам
tasks_file = 'D:/Софт/скрипты и аутпутс/Выходы A/задания_A.xlsx'
supply_file = 'D:/Софт/скрипты и аутпутс/Выходы A/поставки_не_купили_A.xlsx'
database_file = 'D:/Софт/База данных/База данных.xlsx'
output_file = 'D:/Софт/скрипты и аутпутс/выходы/задания_с_названием_и_фото_A.xlsx'

# 1. Загрузка таблиц заданий и заказов из поставки
tasks_df = pd.read_excel(tasks_file)
supply_df = pd.read_excel(supply_file)

# 2. Объединяем эти две таблицы
combined_tasks = pd.concat([tasks_df, supply_df], ignore_index=True)

# 3. Загрузка базы данных (с пропуском первых строк)
db_df = pd.read_excel(database_file, header=0)

# 4. Оставляем нужные столбцы и переименуем "Баркод" → "Штрихкод"
db_trimmed = db_df[['Баркод', 'Наименование', 'Фото']].copy()
db_trimmed = db_trimmed.rename(columns={'Баркод': 'Штрихкод'})

# 5. Удалим дубликаты по Штрихкоду в базе
db_trimmed = db_trimmed.drop_duplicates(subset='Штрихкод')

# 6. Приведение Штрихкодов к строкам без пробелов
combined_tasks['Штрихкод'] = combined_tasks['Штрихкод'].astype(str).str.strip()
db_trimmed['Штрихкод'] = db_trimmed['Штрихкод'].astype(str).str.strip()

# 7. Объединение с базой по Штрихкоду
merged_df = combined_tasks.merge(db_trimmed, on='Штрихкод', how='left')

# 8. Сортировка по ПВЗ и артикулу
merged_df.sort_values(by=['Пункт выдачи', 'Артикул продавца'], inplace=True)

# 9. Сохранение результата
merged_df.to_excel(output_file, index=False)

print(f"Готово! Файл сохранён как: {output_file}")
