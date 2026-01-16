import requests
import pandas as pd
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from config import API_F


HEADERS = {
    'Authorization': API_F
}

ORDER_IDS_URL = (
    'https://marketplace-api.wildberries.ru/'
    'api/marketplace/v3/supplies/{supplyId}/order-ids'
)

OUTPUT_FILE = (
    'D:/Софт/скрипты и аутпутс/Выходы F/поставки_не_купили_F.xlsx'
)


def get_order_ids(supply_id: str) -> list[int]:
    resp = requests.get(
        ORDER_IDS_URL.format(supplyId=supply_id),
        headers=HEADERS
    )
    resp.raise_for_status()
    return resp.json().get('orderIds', [])


def main():
    # --- ID поставок приходят аргументами ---
    supply_ids = sys.argv[1:]

    if not supply_ids:
        raise RuntimeError("Не переданы ID поставок")

    rows = []

    for supply_id in supply_ids:
        supply_id = supply_id.strip()
        if not supply_id:
            continue

        order_ids = get_order_ids(supply_id)
        print(f'{supply_id} → {len(order_ids)} заказов')

        for oid in order_ids:
            rows.append({
                'supply_id': supply_id,
                'id': oid
            })

    if rows:
        df = pd.DataFrame(rows)
    else:
        df = pd.DataFrame(columns=['supply_id', 'id'])

    df['Продавец'] = 'Я ЧОРНИ'
    df['Группа'] = 'F'

    df.to_excel(OUTPUT_FILE, index=False)

    print(f'Сохранено {len(df)} ID → {OUTPUT_FILE}')


if __name__ == '__main__':
    main()
