# -*- coding: utf-8 -*-
import os
import sys
from io import BytesIO
from datetime import datetime

import pandas as pd
import requests
import boto3
from botocore.client import Config
import streamlit as st

API_B = st.secrets.get("API_B", "")
if not API_B:
    raise RuntimeError("Missing API_B in st.secrets")

HEADERS = {
    "Authorization": API_B,
    "Content-Type": "application/json",
}

EXCEL_S3_KEY = "закупленные/закупленные_Каледино/B.xlsx"

SUPPLY_NAME_PREFIX = "ЗАКУПЛЕННЫЕ КАЛЕДИНО"

BOUGHT_COL = "Закуплено"
BOUGHT_YES = "да"

ORDER_ID_COL = "id"

BATCH_SIZE = 100

CREATE_SUPPLY_URL = "https://marketplace-api.wildberries.ru/api/v3/supplies"
ADD_ORDERS_URL = "https://marketplace-api.wildberries.ru/api/marketplace/v3/supplies/{supplyId}/orders"

def _must(name: str) -> str:
    v = (os.environ.get(name) or "").strip()
    if not v:
        raise RuntimeError(f"Missing env var: {name}")
    return v

def s3_client():
    return boto3.client(
        "s3",
        endpoint_url=_must("YC_S3_ENDPOINT"),
        aws_access_key_id=_must("YC_S3_KEY_ID"),
        aws_secret_access_key=_must("YC_S3_SECRET"),
        region_name=(os.environ.get("YC_S3_REGION") or "").strip() or None,
        config=Config(signature_version="s3v4"),
    )

def s3_bucket() -> str:
    return _must("YC_S3_BUCKET")

def s3_read_excel(key: str) -> pd.DataFrame:
    obj = s3_client().get_object(Bucket=s3_bucket(), Key=key)
    data = obj["Body"].read()
    return pd.read_excel(BytesIO(data))


def log(msg: str):
    print(msg)


def main():
    try:
        df = s3_read_excel(EXCEL_S3_KEY)
        log(f"OK: loaded {EXCEL_S3_KEY} rows={len(df)}")
    except Exception as e:
        raise RuntimeError(f"Ошибка загрузки Excel из S3 ({EXCEL_S3_KEY}): {e}")

    for col in (BOUGHT_COL, ORDER_ID_COL):
        if col not in df.columns:
            raise RuntimeError(f"В файле нет колонки '{col}'. Колонки: {list(df.columns)}")

    df2 = df[df[BOUGHT_COL].astype(str).str.lower().str.strip() == BOUGHT_YES].copy()

    if df2.empty:
        log("Нет строк со значением 'да' в столбце 'Закуплено'.")
        return

    ids = (
        df2[ORDER_ID_COL]
        .dropna()
        .astype(str)
        .str.strip()
        .tolist()
    )

    order_ids = []
    for x in ids:
        if x.lower() == "nan" or x == "":
            continue
        try:
            order_ids.append(int(float(x)))
        except Exception:
            pass

    if not order_ids:
        log("Нет валидных ID сборочных заданий.")
        return

    log(f"Найдено заказов к добавлению: {len(order_ids)}")

    supply_name = f"{SUPPLY_NAME_PREFIX} {datetime.now().strftime('%Y-%m-%d')}"
    create_resp = requests.post(
        CREATE_SUPPLY_URL,
        headers=HEADERS,
        json={"name": supply_name},
        timeout=60
    )

    if create_resp.status_code not in (200, 201):
        raise RuntimeError(f"Ошибка при создании поставки: {create_resp.status_code} — {create_resp.text}")

    supply_id = (create_resp.json() or {}).get("id")
    if not supply_id:
        raise RuntimeError(f"WB не вернул id поставки: {create_resp.text}")

    log(f"Создана поставка '{supply_name}' с ID {supply_id}")

    added_total = 0
    for i in range(0, len(order_ids), BATCH_SIZE):
        batch = order_ids[i:i + BATCH_SIZE]
        payload = {"orders": batch}

        resp = requests.patch(
            ADD_ORDERS_URL.format(supplyId=supply_id),
            headers=HEADERS,
            json=payload,
            timeout=60
        )

        if resp.status_code == 204:
            added_total += len(batch)
            log(f"✅ Добавлены {len(batch)} заданий (итого {added_total})")
        else:
            log(f"⚠️ Ошибка добавления батча ({len(batch)}): {resp.status_code} — {resp.text}")

    log(f"Готово. Поставка {supply_id}. Добавлено заданий: {added_total}/{len(order_ids)}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
