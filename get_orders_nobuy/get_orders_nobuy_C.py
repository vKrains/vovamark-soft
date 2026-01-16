import requests
import os
import sys
import boto3
import pandas as pd
from pathlib import Path
from io import BytesIO
import streamlit as st
from botocore.client import Config

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

OUTPUT_FILE = "orders/Выходы C/поставки_не_купили_C.xlsx"

API_C = st.secrets.get("API_C", "")

HEADERS = {
    'Authorization': API_C
}

ORDER_IDS_URL = (
    'https://marketplace-api.wildberries.ru/'
    'api/marketplace/v3/supplies/{supplyId}/order-ids'
)

OUTPUT_FILE = (
    'D:/Софт/скрипты и аутпутс/Выходы C/поставки_не_купили_C.xlsx'
)

def _must(name: str) -> str:
    v = os.environ.get(name, "").strip()
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v

def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=_must("YC_S3_ENDPOINT"),
        aws_access_key_id=_must("YC_S3_KEY_ID"),
        aws_secret_access_key=_must("YC_S3_SECRET"),
        region_name=os.environ.get("YC_S3_REGION", "").strip() or None,
        config=Config(signature_version="s3v4"),
    )

def s3_bucket() -> str:
    return _must("YC_S3_BUCKET")

def upload_df_xlsx(df: pd.DataFrame, key: str):
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df.to_excel(w, index=False)
    buf.seek(0)
    s3_client().put_object(
        Bucket=s3_bucket(),
        Key=key,
        Body=buf.getvalue(),
        ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

def download_df_xlsx(key: str) -> pd.DataFrame:
    obj = s3_client().get_object(Bucket=s3_bucket(), Key=key)
    data = obj["Body"].read()
    return pd.read_excel(BytesIO(data))

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

    df['Продавец'] = 'ОБЩИЙ'
    df['Группа'] = 'C'

    out_key = os.environ.get("WB_API_KEY", OUTPUT_FILE)
    upload_df_xlsx(df, out_key)
    print(f"OK: saved to s3://{s3_bucket()}/{out_key}")

    print(f'Сохранено {len(df)} ID → {OUTPUT_FILE}')


if __name__ == '__main__':
    main()
