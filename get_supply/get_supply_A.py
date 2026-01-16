import requests
import os
import sys
import boto3
from io import BytesIO
import streamlit as st
import pandas as pd
from datetime import datetime
from pathlib import Path 
from botocore.client import Config

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

API_A = st.secrets.get("API_A", "")

HEADERS = {'Authorization': API_A}

URL = 'https://marketplace-api.wildberries.ru/api/v3/supplies'
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
        out_key = os.environ.get("ORDERS_KEY", "Списки поставок/активные_поставки_на_сборке_A.xlsx")
        upload_df_xlsx(df_sorted, out_key)
        print(f"OK: saved to s3://{s3_bucket()}/{out_key}")

        print(f"\nГотово! Файл сохранён как")


