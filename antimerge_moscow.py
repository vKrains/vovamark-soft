# -*- coding: utf-8 -*-
import os
import sys
from io import BytesIO

import pandas as pd
import boto3
from botocore.client import Config

INPUT_KEY = "orders/готовые/ЗАДАНИЯ_МОСКВА.xlsx"

OUTPUT_PREFIX = "закупленные/закупленные_Москва/"

TARGET_GROUPS = ["A", "B", "C", "D", "E", "F", "G", "H"]

GROUP_COL = "Группа"
SORT_COL = "Артикул продавца"

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
    df = s3_read_excel(INPUT_KEY)

    for col in (GROUP_COL, SORT_COL):
        if col not in df.columns:
            raise RuntimeError(f"В входной таблице нет колонки '{col}'")

    df[GROUP_COL] = df[GROUP_COL].astype(str).str.strip()

    saved = 0

    for group in TARGET_GROUPS:
        group_df = df[df[GROUP_COL].str.lower() == group.lower()]
        if group_df.empty:
            print(f"Группа {group}: данных нет")
            continue

        group_df = group_df.sort_values(
            by=SORT_COL,
            key=lambda col: col.astype(str).str.lower()
        )

        out_key = f"{OUTPUT_PREFIX}{group}.xlsx"
        s3_write_excel(group_df, out_key)

        print(f"Сохранено: s3://{s3_bucket()}/{out_key}  ({len(group_df)} строк)")
        saved += 1

    if saved == 0:
        print("Ни по одной группе данные не найдены.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
