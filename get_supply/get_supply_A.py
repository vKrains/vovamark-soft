# -*- coding: utf-8 -*-
import os
import sys
from io import BytesIO
from datetime import datetime

import requests
import pandas as pd
import boto3
from botocore.client import Config
import streamlit as st


# ---------- helpers (как в get_orders_A) ----------
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


# ---------- WB supplies ----------
WB_URL = "https://marketplace-api.wildberries.ru/api/v3/supplies"


def _parse_dt(created_at_raw: str):
    """
    WB обычно отдаёт createdAt в формате 2024-01-01T12:34:56Z
    Возвращаем (строка, dt_obj) для сортировки
    """
    if not created_at_raw:
        return "", None
    try:
        dt_obj = datetime.strptime(created_at_raw, "%Y-%m-%dT%H:%M:%SZ")
        return dt_obj.strftime("%Y-%m-%d %H:%M:%S"), dt_obj
    except Exception:
        return created_at_raw, None


def main():
    # 1) API ключ — как в get_orders_A: из st.secrets
    api_key = (st.secrets.get("API_A", "") or "").strip()
    if not api_key:
        raise RuntimeError("Missing st.secrets['API_A']")

    headers = {"Authorization": api_key}

    # 2) ключ выгрузки — ТОЛЬКО из env, который прокидывает панель
    out_key = os.environ.get("ACTIVE_SUPPLIES_KEY", "supplies/active/A.xlsx").strip()
    if not out_key:
        raise RuntimeError("ACTIVE_SUPPLIES_KEY is empty")

    # 3) читаем supplies (на всякий — пагинация)
    limit = 1000
    next_val = 0
    all_supplies = []

    while True:
        params = {"limit": limit, "next": next_val}
        resp = requests.get(WB_URL, headers=headers, params=params, timeout=60)

        # если WB вернул ошибку — покажем тело (это важно для отладки)
        if resp.status_code != 200:
            raise RuntimeError(
                f"WB error {resp.status_code}: {resp.text}"
            )

        payload = resp.json() or {}
        supplies = payload.get("supplies", []) or []
        all_supplies.extend(supplies)

        # у WB бывает: next = 0 или next отсутствует, когда страниц больше нет
        next_val = payload.get("next", 0)
        if not next_val:
            break

    if not all_supplies:
        # даже если нет поставок — лучше записать пустой файл, чтобы панель не падала
        df_empty = pd.DataFrame(columns=[
            "ID поставки", "Номер поставки", "Дата создания", "Завершена", "Тип груза"
        ])
        upload_df_xlsx(df_empty, out_key)
        print(f"OK: empty saved to s3://{s3_bucket()}/{out_key}")
        return

    # 4) приводим к таблице
    rows = []
    for s in all_supplies:
        created_at_raw = s.get("createdAt", "")
        created_at_str, dt_obj = _parse_dt(created_at_raw)

        rows.append({
            "ID поставки": s.get("id", ""),
            "Номер поставки": s.get("name", ""),
            "Дата создания": created_at_str,
            "_dt_sort": dt_obj,
            "Завершена": bool(s.get("done", False)),
            "Тип груза": s.get("cargoType", ""),
        })

    df = pd.DataFrame(rows)

    # 5) только активные (не завершенные), как у тебя было
    df_active = df[df["Завершена"] == False].copy()

    # если активных нет — тоже запишем пустую (но с колонками)
    if df_active.empty:
        df_active = df.head(0).drop(columns=["_dt_sort"])

    # сортировка (новые сверху)
    if "_dt_sort" in df_active.columns:
        df_active = df_active.sort_values(by="_dt_sort", ascending=False).drop(columns=["_dt_sort"])

    # 6) сохраняем в S3
    upload_df_xlsx(df_active, out_key)
    print(f"OK: saved to s3://{s3_bucket()}/{out_key}")
    print(f"Rows: {len(df_active)}")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        # важно: печатаем ошибку в stdout/stderr, чтобы панель показала лог
        print(f"ERROR: {e}", file=sys.stderr)
        raise
