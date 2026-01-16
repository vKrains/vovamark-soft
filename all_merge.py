# -*- coding: utf-8 -*-
import os
import sys
from io import BytesIO

import pandas as pd
import boto3
from botocore.client import Config


FOLDER_IN_PREFIX = "orders/выходы/"
FOLDER_OUT_PREFIX = "orders/готовые/"

PICKUP_POINTS = {
    "Краснодар": "НА_ЗАКУПКУ_КРД.xlsx",
    "Москва, Москва_Север": "НА_ЗАКУПКУ_ЗЕЛ.xlsx",
    "Москва, Москва_Запад-Юг": "НА_ЗАКУПКУ_МСК.xlsx",
    "Екатеринбург": "НА_ЗАКУПКУ_ЕКБ.xlsx",
}

SORT_COL = "Артикул продавца"
FILTER_COL = "Пункт выдачи"

ALLOWED_EXT = (".xlsx", ".xls", ".csv")


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

def s3_list_keys(prefix: str) -> list[str]:
    keys = []
    token = None
    client = s3_client()
    while True:
        kwargs = {"Bucket": s3_bucket(), "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kwargs["ContinuationToken"] = token
        resp = client.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []) or []:
            k = obj.get("Key", "")
            if k and not k.endswith("/"):
                keys.append(k)
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys

def s3_get_bytes(key: str) -> bytes:
    obj = s3_client().get_object(Bucket=s3_bucket(), Key=key)
    return obj["Body"].read()

def s3_put_bytes(key: str, data: bytes, content_type: str):
    s3_client().put_object(
        Bucket=s3_bucket(),
        Key=key,
        Body=data,
        ContentType=content_type,
    )

def load_frame_from_s3(key: str) -> pd.DataFrame:
    data = s3_get_bytes(key)
    lower = key.lower()

    if lower.endswith(".csv"):
        try:
            return pd.read_csv(BytesIO(data))
        except UnicodeDecodeError:
            return pd.read_csv(BytesIO(data), encoding="cp1251", sep=";")

    if lower.endswith((".xlsx", ".xls")):
        return pd.read_excel(BytesIO(data))

    raise ValueError(f"Неподдерживаемое расширение файла: {key}")

def save_excel_to_s3(df: pd.DataFrame, key: str):
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as w:
        df.to_excel(w, index=False)
    buf.seek(0)
    s3_put_bytes(
        key=key,
        data=buf.getvalue(),
        content_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )


def main() -> None:
    all_keys = s3_list_keys(FOLDER_IN_PREFIX)
    file_keys = [k for k in all_keys if k.lower().endswith(ALLOWED_EXT)]

    if not file_keys:
        raise FileNotFoundError(f"В S3 нет файлов {ALLOWED_EXT} по префиксу: {FOLDER_IN_PREFIX}")

    frames: list[pd.DataFrame] = []
    for key in file_keys:
        try:
            df = load_frame_from_s3(key)
            frames.append(df)
            print(f"Загружено: {key}  ({df.shape[0]} строк, {df.shape[1]} столбцов)")
        except Exception as e:
            print(f"Пропускаю '{key}': {e}")

    if not frames:
        raise RuntimeError("Не удалось загрузить ни одного файла из S3.")

    combined = pd.concat(frames, ignore_index=True)

    if SORT_COL not in combined.columns:
        raise KeyError(f"Нет столбца для сортировки: '{SORT_COL}'. Доступные: {list(combined.columns)}")
    if FILTER_COL not in combined.columns:
        raise KeyError(f"Нет столбца для фильтрации: '{FILTER_COL}'. Доступные: {list(combined.columns)}")

    key_series = combined[SORT_COL].astype(str).str.lower()
    sorted_df = combined.assign(_key=key_series).sort_values("_key").drop(columns="_key")

    saved = 0
    for point, out_name in PICKUP_POINTS.items():
        df_point = sorted_df[sorted_df[FILTER_COL] == point]
        if df_point.empty:
            print(f"Для '{point}' данных нет.")
            continue

        out_key = f"{FOLDER_OUT_PREFIX}{out_name}"
        save_excel_to_s3(df_point, out_key)
        print(f"Сохранено: s3://{s3_bucket()}/{out_key}  ({df_point.shape[0]} строк)")
        saved += 1

    if saved == 0:
        print("Ни по одному пункту выдачи данных не нашлось — ничего не сохранено.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise
