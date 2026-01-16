# delete_supply.py
import os
import sys
import json
import requests

from pathlib import Path 
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

import streamlit as st
API_A = st.secrets.get("API_A", "")

BASE_URL = "https://marketplace-api.wildberries.ru/api/v3/supplies"

def main():
    if not API_A or API_A.startswith("<"):
        print("Ошибка: не задан API-ключ (переменная окружения WB_API_KEY).")
        sys.exit(1)

    if len(sys.argv) < 2:
        print("Использование: python delete_supply.py <supplyId>")
        sys.exit(1)

    supply_id = sys.argv[1].strip()
    url = f"{BASE_URL}/{supply_id}"
    headers = {"Authorization": API_A}

    try:
        resp = requests.delete(url, headers=headers, timeout=30)
    except requests.RequestException as e:
        print(f"Сетевая ошибка при удалении поставки {supply_id}: {e}")
        sys.exit(1)

    # 204 — успешное удаление, без тела
    if resp.status_code == 204:
        print("OK")  # печатаем краткий маркер успеха (удобно для пайплайнов)
        sys.exit(0)

    # Печатаем подробности ошибки
    try:
        body = resp.json()
    except ValueError:
        body = resp.text

    # Частые случаи: 400/401/403/404/409/429
    print(f"Ошибка удаления поставки {supply_id}: HTTP {resp.status_code} — {json.dumps(body, ensure_ascii=False)}")
    # Подсказки по типичным кодам:
    if resp.status_code == 409:
        print("Подсказка: за поставкой закреплены сборочные задания — сначала отвяжите их.")
    elif resp.status_code == 404:
        print("Подсказка: проверьте корректность supplyId или права доступа.")
    elif resp.status_code == 401:
        print("Подсказка: неверный/просроченный токен WB_API_KEY.")
    elif resp.status_code == 429:
        print("Подсказка: превышен лимит запросов — подождите и повторите попытку.")
    sys.exit(1)

if __name__ == "__main__":
    main()
