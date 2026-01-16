# -*- coding: utf-8 -*-
import os
import sys
from io import BytesIO

import pandas as pd
import boto3
from botocore.client import Config

TASKS_KEY    = "orders/C/задания_C.xlsx"
SUPPLY_KEY   = "orders/Выходы C/поставки_не_купили_C.xlsx"
DATABASE_KEY = "База данных/База данных.xlsx"
OUTPUT_KEY   = "orders/выходы/задания_с_названием_и_фото_C.xlsx"

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

def s3_write_excel(df: pd.DataFrame, key: str):
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

def main():
    tasks_df = s3_read_excel(TASKS_KEY)
    supply_df = s3_read_excel(SUPPLY_KEY)

    combined_tasks = pd.concat([tasks_df, supply_df], ignore_index=True)

    db_df = s3_read_excel(DATABASE_KEY)

    need_cols = ["Баркод", "Наименование", "Фото"]
    missing = [c for c in need_cols if c not in db_df.columns]
    if missing:
        raise RuntimeError(f"В базе нет колонок: {missing}")

    db_trimmed = db_df[need_cols].copy()
    db_trimmed = db_trimmed.rename(columns={"Баркод": "Штрихкод"})

    db_trimmed["Штрихкод"] = db_trimmed["Штрихкод"].astype(str).str.strip()
    db_trimmed = db_trimmed.drop_duplicates(subset="Штрихкод")

    if "Штрихкод" not in combined_tasks.columns:
        raise RuntimeError("В таблице заданий/НЕ КУПИЛИ нет колонки 'Штрихкод'")

    combined_tasks["Штрихкод"] = combined_tasks["Штрихкод"].astype(str).str.strip()

    merged_df = combined_tasks.merge(db_trimmed, on="Штрихкод", how="left")

    sort_cols = []
    if "Пункт выдачи" in merged_df.columns:
        sort_cols.append("Пункт выдачи")
    if "Артикул продавца" in merged_df.columns:
        sort_cols.append("Артикул продавца")
    if sort_cols:
        merged_df.sort_values(by=sort_cols, inplace=True)

    s3_write_excel(merged_df, OUTPUT_KEY)

    print(f"OK: saved to s3://{s3_bucket()}/{OUTPUT_KEY}")
    print(f"Rows: {len(merged_df)}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
