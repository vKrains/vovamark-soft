import pandas as pd
import os

# Путь к папке, куда будут сохраняться файлы
output_folder = "D:/Софт/скрипты и аутпутс/закупленные Екб"
os.makedirs(output_folder, exist_ok=True)  # Создаёт папку, если её нет

# Загрузка исходной таблицы
df = pd.read_excel("D:/Софт/скрипты и аутпутс/на закупку/ЗАДАНИЯ_ЕКБ.xlsx")

# Список нужных продавцов
target_sellers = ["A", "B", "C", "D", "E", "F", "G", "H"]

# Фильтрация и сохранение по каждому продавцу
for seller in target_sellers:
    seller_df = df[df["Группа"].str.strip().str.lower() == seller.lower()]
    seller_df_sorted = seller_df.sort_values(by="Артикул продавца", key=lambda col: col.str.lower())
    
    output_path = os.path.join(output_folder, f"{seller}.xlsx")
    seller_df_sorted.to_excel(output_path, index=False)
