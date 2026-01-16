import pandas as pd
import requests
from datetime import datetime
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

import streamlit as st
API_D = st.secrets.get("API_D", "")

HEADERS = {
    'Authorization': API_D,
    'Content-Type': 'application/json'
}

CREATE_SUPPLY_URL = 'https://marketplace-api.wildberries.ru/api/v3/supplies'
ADD_ORDERS_URL = 'https://marketplace-api.wildberries.ru/api/marketplace/v3/supplies/{supplyId}/orders'

LOG_FILE = 'log.txt'
EXCEL_FILE = 'D:/Софт/скрипты и аутпутс/закупленные Каледино/D.xlsx'


def log(message):
    print(message)
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(message + '\n')


# === ЗАГРУЗКА EXCEL ===
try:
    df = pd.read_excel(EXCEL_FILE)
except Exception as e:
    log(f"Ошибка загрузки Excel: {e}")
    exit(1)


# === ФИЛЬТР "Закуплено" == "да" ===
df = df[df['Закуплено'].astype(str).str.lower().str.strip() == 'да']

if df.empty:
    log("Нет строк со значением 'да' в столбце 'Закуплено'.")
    exit(0)


order_ids = (
    df['id']
    .dropna()
    .astype(int)
    .tolist()
)

if not order_ids:
    log("Нет валидных ID сборочных заданий.")
    exit(0)


# === СОЗДАНИЕ ПОСТАВКИ ===
supply_name = f"ЗАКУПЛЕННЫЕ КАЛЕДИНО {datetime.now().strftime('%Y-%m-%d')}"
create_resp = requests.post(
    CREATE_SUPPLY_URL,
    headers=HEADERS,
    json={"name": supply_name}
)

if create_resp.status_code not in (200, 201):
    log(f"Ошибка при создании поставки: {create_resp.status_code} — {create_resp.text}")
    exit(1)

supply_id = create_resp.json().get('id')
log(f"Создана поставка '{supply_name}' с ID {supply_id}")


# === ДОБАВЛЕНИЕ ЗАДАНИЙ БАТЧАМИ ПО 100 ===
BATCH_SIZE = 100

for i in range(0, len(order_ids), BATCH_SIZE):
    batch = order_ids[i:i + BATCH_SIZE]

    payload = {
        "orders": batch
    }

    resp = requests.patch(
        ADD_ORDERS_URL.format(supplyId=supply_id),
        headers=HEADERS,
        json=payload
    )

    if resp.status_code == 204:
        log(f"Добавлены задания: {batch}")
    else:
        log(
            f"Ошибка добавления {batch}: "
            f"{resp.status_code} — {resp.text}"
        )
