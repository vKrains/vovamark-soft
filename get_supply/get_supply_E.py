# -*- coding: utf-8 -*-
import os
import sys
import boto3
import requests
import streamlit as st
import pandas as pd
from datetime import datetime
from io import BytesIO
from botocore.client import Config

API_E = st.secrets.get("API_E", "")
if not API_E:
    raise RuntimeError("Missing API_E in st.secrets")

HEADERS = {"Authorization": API_E}

WB_URL = "https://marketplace-api.wildberries.ru/api/v3/supplies"

DEFAULT_OUT_KEY = "supplies/active/E.xlsx"

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

def parse_dt(value: str):
    if not value:
        return "", None
    try:
        dt = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
        return dt.strftime("%Y-%m-%d %H:%M:%S"), dt
    except Exception:
        return value, None


def main():
    out_key = os.environ.get("ACTIVE_SUPPLIES_KEY", DEFAULT_OUT_KEY)

    params = {"limit": 1000, "next": 0}
    response = requests.get(WB_URL, headers=HEADERS, params=params, timeout=60)

    if response.status_code != 200:
        raise RuntimeError(
            f"WB error {response.status_code}: {response.text}"
        )

    supplies = response.json().get("supplies", [])

    rows = []
    for s in supplies:
        created_at_str, dt_obj = parse_dt(s.get("createdAt"))

        rows.append({
            "ID поставки": s.get("id", ""),
            "Номер поставки": s.get("name", ""),
            "Дата создания": created_at_str,
            "_dt_sort": dt_obj,
            "Завершена": bool(s.get("done", False)),
            "Тип груза": s.get("cargoType", ""),
        })

    df = pd.DataFrame(rows)

    if not df.empty:
        df = df[df["Завершена"] == False]

    if "_dt_sort" in df.columns:
        df = df.sort_values(by="_dt_sort", ascending=False)
        df = df.drop(columns=["_dt_sort"])

    upload_df_xlsx(df, out_key)

    print(f"OK: saved to s3://{s3_bucket()}/{out_key}")
    print(f"Rows: {len(df)}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
