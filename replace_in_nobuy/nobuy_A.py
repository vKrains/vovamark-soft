import os
import pandas as pd
import requests

import sys
from pathlib import Path 
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from config import API_A

# Параметры
folder_path = r"D:/Софт/скрипты и аутпутс/Листы подбора/A"  # Путь к папке
supply_id = "WB-GI-166772877"  # Подставьте ваш supplyId

# Заголовки для запроса
headers = {
    "Authorization": f"Bearer {API_A}",
    "Content-Type": "application/json"
}

# Перебираем все файлы Excel в папке
for file_name in os.listdir(folder_path):
    if file_name.endswith(".xlsx"):
        file_path = os.path.join(folder_path, file_name)
        print(f"Обработка файла: {file_name}")

        # Загружаем Excel
        df = pd.read_excel(file_path)

        # Проверяем, что нужные столбцы есть
        if "№ задания" not in df.columns or "Собрано" not in df.columns:
            print(f"В файле {file_name} нет нужных столбцов. Пропускаем.")
            continue

        # Фильтруем заказы, где "Собран" == "нет"
        uncollected = df[df["Собрано"].str.lower() == "нет"]

        for idx, row in uncollected.iterrows():
            order_id = str(row["№ задания"])

            url = f"https://marketplace-api.wildberries.ru/api/v3/supplies/{supply_id}/orders/{order_id}"

            response = requests.patch(url, headers=headers)

            if response.status_code == 200:
                print(f"Заказ {order_id} успешно отправлен в поставку {supply_id}")
                # По желанию можно пометить заказ как обработанный
                df.at[idx, "Собран"] = "отправлен"
            else:
                print(f"Ошибка при отправке заказа {order_id}: {response.status_code} - {response.text}")

        # Сохраняем обновленный файл (или в новую папку)
        df.to_excel(file_path, index=False)
