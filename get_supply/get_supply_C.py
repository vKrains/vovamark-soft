import requests
import pandas as pd
from datetime import datetime

import sys, os
from pathlib import Path 
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from config import API_C

HEADERS = {'Authorization': API_C}

URL = 'https://marketplace-api.wildberries.ru/api/v3/supplies'

# Параметры запроса
params = {
    "limit": 1000,
    "next": 0
}

# Запрос
response = requests.get(URL, headers=HEADERS, params=params)
response.raise_for_status()
data = response.json()

supplies = data.get('supplies', [])

if not supplies:
    print("Нет поставок.")
else:
    rows = []
    for s in supplies:
        created_at_raw = s.get('createdAt')
        created_at = ''
        dt_obj = None
        if created_at_raw:
            try:
                dt_obj = datetime.strptime(created_at_raw, '%Y-%m-%dT%H:%M:%SZ')
                created_at = dt_obj.strftime('%Y-%m-%d %H:%M:%S')
            except:
                created_at = created_at_raw

        rows.append({
            'ID поставки': s.get('id', ''),
            'Номер поставки': s.get('name', ''),
            'Дата создания': created_at,
            'Дата сортировки': dt_obj,
            'Завершена': s.get('done', False),
            'Тип груза': s.get('cargoType', ''),
        })

    # Преобразуем в DataFrame
    df = pd.DataFrame(rows)

    # Оставляем только НЕ завершенные
    df_filtered = df[df['Завершена'] == False]

    if df_filtered.empty:
        print("Нет активных поставок (На сборке).")
    else:
        # Сортировка по дате (новые сверху)
        df_sorted = df_filtered.sort_values(by='Дата сортировки', ascending=False)

        # Убираем вспомогательный столбец
        df_sorted = df_sorted.drop(columns=['Дата сортировки'])

        # Сохраняем
        output_file = 'D:/Софт/скрипты и аутпутс/Списки поставок/активные_поставки_на_сборке_бабурина.xlsx'
        df_sorted.to_excel(output_file, index=False)
        print(f"\nГотово! Файл сохранён как: {output_file}")


