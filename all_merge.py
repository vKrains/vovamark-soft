import os
import pandas as pd

# === Настройки путей ===
# Папка-источник с файлами для слияния
FOLDER_IN = r"D:/Софт/скрипты и аутпутс/выходы"
# Папка-назначение для выгрузок
FOLDER_OUT = r"D:/Софт/скрипты и аутпутс/на закупку/готовые"

# Соответствие "Пункт выдачи" -> имя выходного файла
PICKUP_POINTS = {
    "Краснодар": "НА_ЗАКУПКУ_КРД.xlsx",
    "Москва, Москва_Север": "НА_ЗАКУПКУ_ЗЕЛ.xlsx",
    "Москва, Москва_Запад-Юг": "НА_ЗАКУПКУ_МСК.xlsx",
    "Екатеринбург": "НА_ЗАКУПКУ_ЕКБ.xlsx"
}

# Имя столбца для сортировки
SORT_COL = "Артикул продавца"
# Имя столбца для фильтрации
FILTER_COL = "Пункт выдачи"

def load_frame(path: str) -> pd.DataFrame:
    """
    Загружает DataFrame из CSV/XLSX/XLS.
    Для CSV — пробует utf-8, затем cp1251.
    """
    _, ext = os.path.splitext(path.lower())
    if ext == ".csv":
        # попытка UTF-8, затем cp1251
        try:
            return pd.read_csv(path)
        except UnicodeDecodeError:
            return pd.read_csv(path, encoding="cp1251", sep=";")
    elif ext in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    else:
        raise ValueError(f"Неподдерживаемое расширение файла: {path}")

def main() -> None:
    if not os.path.isdir(FOLDER_IN):
        raise FileNotFoundError(f"Не найдена папка с входными файлами: {FOLDER_IN}")

    files = [f for f in os.listdir(FOLDER_IN) if f.lower().endswith((".xlsx", ".xls", ".csv"))]
    if not files:
        raise FileNotFoundError(f"В папке нет файлов .xlsx/.xls/.csv: {FOLDER_IN}")

    frames: list[pd.DataFrame] = []  # исправление: теперь список создан
    for fname in files:
        path = os.path.join(FOLDER_IN, fname)
        try:
            df = load_frame(path)
            frames.append(df)
            print(f"Загружено: {fname}  ({df.shape[0]} строк, {df.shape[1]} столбцов)")
        except Exception as e:
            print(f"Пропускаю '{fname}': {e}")

    if not frames:
        raise RuntimeError("Не удалось загрузить ни одного файла.")

    combined = pd.concat(frames, ignore_index=True)

    # Проверки наличия нужных столбцов
    if SORT_COL not in combined.columns:
        raise KeyError(f"Нет столбца для сортировки: '{SORT_COL}'. Доступные: {list(combined.columns)}")
    if FILTER_COL not in combined.columns:
        raise KeyError(f"Нет столбца для фильтрации: '{FILTER_COL}'. Доступные: {list(combined.columns)}")

    # Безопасная сортировка по строковому ключу (NaN/числа не ломают сортировку)
    key_series = combined[SORT_COL].astype(str).str.lower()
    sorted_df = combined.assign(_key=key_series).sort_values("_key").drop(columns="_key")

    os.makedirs(FOLDER_OUT, exist_ok=True)

    # Выгрузка по пунктам
    for point, out_name in PICKUP_POINTS.items():
        df_point = sorted_df[sorted_df[FILTER_COL] == point]
        if df_point.empty:
            print(f"Для '{point}' данных нет.")
            continue

        out_path = os.path.join(FOLDER_OUT, out_name)
        try:
            df_point.to_excel(out_path, index=False)
            print(f"Сохранено: {out_path}  ({df_point.shape[0]} строк)")
        except Exception as e:
            print(f"Ошибка сохранения '{out_path}': {e}")

if __name__ == "__main__":
    main()
