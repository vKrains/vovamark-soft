# create_supply_only.py
import os
import sys
import json
from datetime import datetime
import requests

from pathlib import Path 
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from config import API_E

CREATE_SUPPLY_URL = "https://marketplace-api.wildberries.ru/api/v3/supplies"
HEADERS = {"Authorization": API_E}

def main():
    if not API_E or API_E.startswith("<"):
        print("Ошибка: не задан API-ключ (WB_API_KEY).")
        sys.exit(1)

    # Имя можно передать первым аргументом, иначе возьмём дефолт с датой
    supply_name = sys.argv[1] if len(sys.argv) > 1 else f"NO BUY {datetime.now():%Y-%m-%d}"

    try:
        resp = requests.post(CREATE_SUPPLY_URL, headers=HEADERS, json={"name": supply_name}, timeout=30)
    except requests.RequestException as e:
        print(f"СетЕвая ошибка при создании поставки: {e}")
        sys.exit(1)

    if resp.status_code not in (200, 201):
        # Печатаем понятное сообщение об ошибке с телом ответа
        try:
            body = resp.json()
        except Exception:
            body = resp.text
        print(f"Ошибка создания поставки '{supply_name}': HTTP {resp.status_code} — {body}")
        # 409 у WB считается как 5 запросов лимита — полезно знать при отладке
        sys.exit(1)

    # Успех: пытаемся вытащить id из JSON
    try:
        data = resp.json()
    except Exception:
        print("Не удалось распарсить JSON-ответ от API.")
        sys.exit(1)

    supply_id = data.get("id")
    if not supply_id:
        print(f"Поставка создана, но в ответе нет поля 'id': {json.dumps(data, ensure_ascii=False)}")
        sys.exit(1)

    # Выводим только ID — удобно для последующего пайплайна (CLI/скрипты)
    print(supply_id)

if __name__ == "__main__":
    main()
