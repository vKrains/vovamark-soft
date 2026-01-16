import os
import time
import requests
import pandas as pd
from datetime import datetime


FOLDER_DEFAULT = r"D:/Софт/скрипты и аутпутс/Листы подбора/обработка"
REQUEST_DELAY = 0.07


def _format_date(d):
    if isinstance(d, str):
        return datetime.strptime(d, "%d.%m.%Y").strftime("%d.%m.%Y")
    return d.strftime("%d.%m.%Y")


def _send_expiration(order_id: str, expiration: str, api_key: str):
    url = f"https://marketplace-api.wildberries.ru/api/v3/orders/{order_id}/meta/expiration"
    headers = {"Authorization": api_key}
    return requests.put(url, json={"expiration": expiration}, headers=headers)


def process_file(path: str, api_key: str):
    print(f"\nОбработка: {path}")
    df = pd.read_excel(path)

    if "Срок годности" not in df.columns:
        print("⚠️ Нет столбца 'Срок годности' — пропуск.")
        return

    id_cols = ["№ задания", "Order ID", "ID задания", "ID заказа"]
    col_order = next((c for c in id_cols if c in df.columns), None)

    if not col_order:
        print("❌ Не найден столбец с ID заказа — пропуск.")
        return

    for _, row in df.iterrows():
        order_id = str(row[col_order]).strip()
        exp = row["Срок годности"]

        if not order_id or pd.isna(exp):
            continue

        expiration = _format_date(exp)

        r = _send_expiration(order_id, expiration, api_key)

        if r.status_code == 204:
            print(f"✅ {order_id} → {expiration}")
        elif r.status_code == 409:
            print(f"⚠️ {order_id} — 409 (WB отклонил, засчитывается как 10 запросов)")
        else:
            print(f"❌ {order_id} — {r.status_code}: {r.text}")

        time.sleep(REQUEST_DELAY)


def run(api_key: str, folder: str = FOLDER_DEFAULT):
    """
    Главная точка входа для приложения.
    """
    for f in os.listdir(folder):
        if f.lower().endswith(".xlsx") and not f.startswith("~$"):
            process_file(os.path.join(folder, f), api_key)
