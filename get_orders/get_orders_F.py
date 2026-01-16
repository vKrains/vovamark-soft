import requests
import streamlit as st
import pandas as pd
from datetime import datetime
import sys, os
from pathlib import Path
from io import BytesIO
import boto3
from botocore.client import Config

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

API_F = st.secrets.get("API_F", "")

HEADERS = {'Authorization': API_F}
URL = 'https://marketplace-api.wildberries.ru/api/v3/orders/new'

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

PREFIX_TO_SUPPLY = {
    'TAB': 'ТАБРИС',
    'TBRS': 'ТАБРИС',
    'BAU': 'БАУЦЕНТР',
    'MK': 'КОСМЕТИК',
    'mk': 'КОСМЕТИК',
    'YEMKP': 'КОСМЕТИК',
    'MGKSMT': 'КОСМЕТИК',
    'OKK': 'ОКЕЙ',
    'EA': 'МОСКВА АПТЕКА',
    'ASIA': 'АЗИЯЛЭНД',
    'TURC': 'ТУРЦИЯ',
    'MAG': 'МАГНИТ',
    'CHIT': 'ЧИТАЙГОРОД',
    'LEMAN': 'ЛЕМАНА',
    'LETOILE': 'ЛЕТУАЛЬ',
    'ZOOZAVR': 'ЗООЗАВР',
    'hlorid': 'МОСКВА ХЛОРИД',
    'AUCHAN': 'АШАН',
    'ACH'   : 'АШАН',
    'HUNT': 'МИРОХОТЫ',
    'MIR':  'МИРОХОТЫ',
    'MTR': 'МЕТРО',
    'MET': 'МЕТРО',
    'MODI': 'МОДИ',
    'PDRGT': 'ПИДРУЖКА',
    'TOK': 'ТОКПОКА',
    '4LAPY' : 'ЛАПЫ',
    'wb4lxltrsh': 'ОКЕЙ',
    'LENTA': 'ЛЕНТА',
    'PEREK': 'ПЕРЕКРЁСТОК',
    'PDRG': 'ПОДРУЖКА'
}

def get_magazin_by_article(article):
    for prefix in PREFIX_TO_SUPPLY:
        if article.startswith(prefix):
            return PREFIX_TO_SUPPLY[prefix]
    return ''

response = requests.get(URL, headers=HEADERS)
orders = response.json().get('orders', [])

data = []

for o in orders:
    created_at_raw = o.get('createdAt')
    created_at = ''
    if created_at_raw:
        try:
            dt = datetime.strptime(created_at_raw, '%Y-%m-%dT%H:%M:%SZ')
            created_at = dt.strftime('%Y-%m-%d %H:%M:%S')
        except:
            created_at = created_at_raw

    article = o.get('article', '')
    data.append({
        'Дата': created_at,
        'Артикул продавца': article,
        'Пункт выдачи': ", ".join(o.get('offices', [])),
        'Цена (руб)': o.get('price', 0) / 100,
        'Штрихкод': ", ".join(o.get('skus', [])),
        'Магазин': get_magazin_by_article(article),
        'id': o.get('id', '')
    })

if data:
    df = pd.DataFrame(data)
    df['Продавец'] = 'Я ЧОРНИ'
    df['Группа'] = 'F'
    out_key = os.environ.get("ORDERS_KEY", "orders/F/задания_F.xlsx")
    upload_df_xlsx(df, out_key)
    print(f"OK: saved to s3://{s3_bucket()}/{out_key}")

    print("Данные на облаке в папке orders/F'")
else:
    print("Нет новых заданий.")
