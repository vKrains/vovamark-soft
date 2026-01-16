# -*- coding: utf-8 -*-
import sys
import os
import re
import boto3
import base64
import requests
import subprocess
from io import BytesIO
import streamlit as st
import pandas as pd
from datetime import datetime
from pathlib import Path
from botocore.client import Config

sys.path.append(os.path.dirname(__file__))

# === Автоматическая установка UTF-8 на Windows ===
if os.name == "nt":  # если Windows
    try:
        import ctypes
        ctypes.windll.kernel32.SetConsoleOutputCP(65001)
        ctypes.windll.kernel32.SetConsoleCP(65001)
    except Exception:
        pass
    os.environ["PYTHONIOENCODING"] = "utf-8"
    try:
        sys.stdout.reconfigure(encoding='utf-8')
        sys.stderr.reconfigure(encoding='utf-8')
    except Exception:
        pass



API_A = st.secrets.get("API_A", "")
API_B = st.secrets.get("API_B", "")
API_C = st.secrets.get("API_C", "")
API_D = st.secrets.get("API_D", "")
API_E = st.secrets.get("API_E", "")
API_F = st.secrets.get("API_F", "")


# --- Простая авторизация ---
#if "authenticated" not in st.session_state:
#    st.session_state.authenticated = False
#
#if not st.session_state.authenticated:
#    st.title("🔒 Вход")
#    password = st.text_input("Введите пароль", type="password")
#    if st.button("Войти"):
#        if password == "витялох": 
#            st.session_state.authenticated = True
#            st.experimental_rerun()
#        else:
#            st.error("Неверный пароль")
#    st.stop()

def _s3():
    return boto3.client(
        "s3",
        endpoint_url=st.secrets["YC_S3_ENDPOINT"],
        aws_access_key_id=st.secrets["YC_S3_KEY_ID"],
        aws_secret_access_key=st.secrets["YC_S3_SECRET"],
        region_name=st.secrets.get("YC_S3_REGION", None),
        config=Config(signature_version="s3v4"),
    )

def _s3_bucket():
    return st.secrets["YC_S3_BUCKET"]

def s3_read_excel(key: str) -> pd.DataFrame:
    obj = _s3().get_object(Bucket=_s3_bucket(), Key=key)
    data = obj["Body"].read()
    return pd.read_excel(BytesIO(data))

barcodes_to_log = []

#----------------------------------------------САЙДБАР НАСТРОЙКИ-----------------------------------------------------------------------------

st.set_page_config(page_title="Сборочные задания WB", layout="wide")
st.title("📦 Панель управления сборочными заданиями")

people = {
    "ГРУППА A": "A",
    "ГРУППА B": "B",
    "ГРУППА C": "C",
    "ГРУППА D": "D",
    "ГРУППА E": "E",
    "ГРУППА F": "F"
}

# Выбор кабинета в сайдбаре
st.sidebar.header("👤 Кабинет")
selected_person = st.sidebar.selectbox("Выберите кабинет:", list(people.keys()))
person_id = people[selected_person]


# --- Хелперы и состояние для активных поставок ---
def _excel_key_for(pid: str) -> str:
    return f"supplies/active/{pid}.xlsx"

def _script_for(pid: str) -> str:
    return f"get_supply/get_supply_{pid}.py"

def load_active_supplies_for(pid: str):
    """Запускает скрипт кабинета (если есть) и возвращает DataFrame из S3 (или None)."""
    script = _script_for(pid)
    s3_key = _excel_key_for(pid)

    env = dict(os.environ)
    env.update({
        "YC_S3_ENDPOINT": str(st.secrets["YC_S3_ENDPOINT"]),
        "YC_S3_BUCKET": str(st.secrets["YC_S3_BUCKET"]),
        "YC_S3_KEY_ID": str(st.secrets["YC_S3_KEY_ID"]),
        "YC_S3_SECRET": str(st.secrets["YC_S3_SECRET"]),
        "YC_S3_REGION": str(st.secrets.get("YC_S3_REGION", "ru-central1")),
        "WB_API_KEY": str(st.secrets.get(f"WB_API_{pid}", "")),
        "ACTIVE_SUPPLIES_KEY": s3_key,
    })

    if os.path.exists(script):
        try:
            r = subprocess.run(
                [sys.executable, script],
                capture_output=True,
                text=True,
                timeout=120,
                env=env,
            )
            if r.returncode != 0:
                st.sidebar.error(f"Ошибка get_supply_{pid}: {r.stderr or r.stdout}")
        except Exception as ex:
            st.sidebar.error(f"Ошибка запуска get_supply_{pid}: {ex}")

    try:
        return s3_read_excel(s3_key)
    except Exception as ex:
        st.sidebar.warning(f"Не удалось прочитать active supplies из S3 для {pid}: {ex}")
        return None



# Инициализация общего кэша
if "active_supplies" not in st.session_state:
    st.session_state.active_supplies = {}  # dict: person_id -> DataFrame | None

#----------------------------------------КОНЕЦ САЙДБАР НАСТРОЙКИ------------------------------------------------------------------------------
import sys

# --- Первичные действия ---
st.subheader("📥 Первичные действия")

# Скачать задания
download_script = f"get_orders/get_orders_{person_id}.py"
if st.button("📥 Скачать задания"):
    if os.path.exists(download_script):
        result = subprocess.run([sys.executable, download_script], capture_output=True, text=True)
        st.text_area("Результат скачивания", (result.stdout or '') + (result.stderr or ''), height=300)
    else:
        st.error(f"Скрипт {download_script} не найден.")

#----------------------------------------------НЕ КУПИЛИ/СКРИПТ------------------------------------------------------------------------------

nobuy_orders_script = f"get_orders_nobuy/get_orders_nobuy_{person_id}.py"

if st.button("📥 Получить заказы НЕ КУПИЛИ"):

    df_selected = st.session_state.active_supplies.get(person_id)

    if df_selected is None or df_selected.empty:
        st.error("Нет данных активных поставок. Сначала обновите таблицы в сайдбаре.")
        st.stop()

    if "Номер поставки" not in df_selected.columns:
        st.error("В таблице нет столбца 'Номер поставки'. Проверьте формат Excel.")
        st.stop()

    mask = df_selected["Номер поставки"].astype(str).str.contains("НЕ КУПИЛИ", case=False, na=False)
    rows = df_selected.loc[mask]

    if rows.empty:
        st.error("В активных поставках не найдено 'НЕ КУПИЛИ'.")
        st.stop()

    candidate_id_cols = ["id", "ID", "Id", "Айди", "ID поставки", "Айди поставки"]
    id_col = next((c for c in candidate_id_cols if c in rows.columns), None)

    if not id_col:
        st.error("В таблице не найден столбец с ID поставки (ожидались: id / ID / Айди / ID поставки).")
        st.stop()

    supply_ids = rows[id_col].astype(str).str.strip().tolist()
    supply_ids = [x for x in supply_ids if x and x.lower() != "nan"]

    if not supply_ids:
        st.error("Не удалось получить ID поставок.")
        st.stop()

    st.info(f"Найдено поставок 'НЕ КУПИЛИ': {len(supply_ids)}")

    if not os.path.exists(nobuy_orders_script):
        st.error(f"Скрипт {nobuy_orders_script} не найден.")
        st.stop()

    # ВАЖНО: запускаем тем же интерпретатором, что и Streamlit
    cmd = [sys.executable, nobuy_orders_script, *supply_ids]

    # ВАЖНО: прокидываем env (как ты уже делаешь для get_supply) :contentReference[oaicite:1]{index=1}
    env = dict(os.environ)
    env.update({
        "YC_S3_ENDPOINT": str(st.secrets["YC_S3_ENDPOINT"]),
        "YC_S3_BUCKET": str(st.secrets["YC_S3_BUCKET"]),
        "YC_S3_KEY_ID": str(st.secrets["YC_S3_KEY_ID"]),
        "YC_S3_SECRET": str(st.secrets["YC_S3_SECRET"]),
        "YC_S3_REGION": str(st.secrets.get("YC_S3_REGION", "ru-central1")),

        # если в nobuy-скрипте нужен ключ WB:
        "WB_API_KEY": str(st.secrets.get(f"orders/Выходы A/поставки_не_купили_{person_id}.xlsx", "")),
    })

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            env=env,
            timeout=180,
        )
    except subprocess.TimeoutExpired:
        st.error("Скрипт выполнялся слишком долго и был остановлен (timeout).")
        st.stop()
    except Exception as ex:
        st.error(f"Ошибка запуска subprocess: {ex}")
        st.stop()

    if result.returncode == 0:
        st.success("Сбор заказов из 'НЕ КУПИЛИ' выполнен успешно.")
    else:
        st.error(f"Ошибка при выполнении скрипта (код {result.returncode}).")

    st.text_area(
        "Логи",
        (result.stdout or "") + ("\n" + result.stderr if result.stderr else ""),
        height=300
    )
#-----------------------------------------------НЕ КУПИЛИ КОНЕЦ------------------------------------------------------------------------------


# Объединить с базой
merge_script = f"merge_with_base/merge_with_base_{person_id}.py"
if st.button("🔗 Объединить с базой"):
    if os.path.exists(merge_script):
        result = subprocess.run(["python", merge_script], capture_output=True, text=True)
        st.text_area("Результат объединения с базой", (result.stdout or '') + (result.stderr or ''), height=300)
    else:
        st.error(f"Скрипт {merge_script} не найден.")

# --- Создать поставку (с вводом имени из Streamlit) ---
# --- Первичные действия ---
st.subheader("Создать поставку")
default_nobuy_name = f"NO BUY {datetime.now():%Y-%m-%d}"
nobuy_supply_name = st.text_input(
    "Введите название поставки",
    value=default_nobuy_name,
    key="nobuy_supply_name"
)

nobuy_orders_script = f"create_no_buy_supply/create_no_buy_supply_{person_id}.py"
if st.button("СОЗДАТЬ ПОСТАВКУ"):
    if os.path.exists(nobuy_orders_script):
        name_arg = (nobuy_supply_name or default_nobuy_name).strip()
        result = subprocess.run(
            ["python", nobuy_orders_script, name_arg],
            capture_output=True, text=True
        )
        if result.returncode == 0:
            st.success(f"ID созданной поставки: {result.stdout.strip()}")
        else:
            st.error("Ошибка при создании поставки")
            st.text_area("Логи", (result.stdout or "") + (result.stderr or ""), height=300)
    else:
        st.error(f"Скрипт {nobuy_orders_script} не найден.")

#-----------------------------------------------УДАЛЕНИЕ ПОСТАВКИ------------------------------------------------------------------------------
st.markdown("---")
st.subheader("🗑️ Удалить поставку")

delete_supply_id = st.text_input("Введите ID поставки для удаления", key="delete_supply_id")

# Пытаемся использовать персональный скрипт, иначе — общий (если он есть)
delete_script_personal = f"delete_supply/delete_supply_{person_id}.py"
delete_script_generic = "delete_supply/delete_supply.py"
delete_script = delete_script_personal if os.path.exists(delete_script_personal) else delete_script_generic

if st.button("🗑️ Удалить поставку"):
    sid = (delete_supply_id or "").strip()
    if not sid:
        st.error("Введите ID поставки.")
    elif not os.path.exists(delete_script):
        st.error(f"Скрипт для удаления не найден: {delete_script_personal} или {delete_script_generic}")
    else:
        # Скрипт ожидает supplyId как 1-й аргумент и печатает 'OK' при 204 (см. delete_supply_булыга.py)
        result = subprocess.run(["python", delete_script, sid], capture_output=True, text=True)
        out = (result.stdout or "").strip()
        err = (result.stderr or "").strip()

        if result.returncode == 0 and "OK" in out:
            st.success(f"Поставка {sid} удалена.")
        else:
            st.error(f"Не удалось удалить поставку {sid}. См. лог ниже.")
            st.text_area("Логи удаления", (out + ("\n" + err if err else "")) or "(пусто)", height=260)


        

#--------------------------------------------------ОБЩИЕ ОПЕРАЦИИ------------------------------------------------------------------------------
st.markdown("---")
st.subheader("🛠️ Общие операции")

col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    if st.button("⚙️ MERGE (общий)"):
        if os.path.exists("all_merge.py"):
            result = subprocess.run(["python", "all_merge.py"], capture_output=True, text=True)
            st.text_area("Результат MERGE", (result.stdout or '') + (result.stderr or ''), height=300)
        else:
            st.error("Скрипт all_merge.py не найден.")

with col2:
    if st.button("❌ ANTIMMERGE (KRASNODAR)"):
        if os.path.exists("antimerge_krasnodar.py"):
            result = subprocess.run(["python", "antimerge_krasnodar.py"], capture_output=True, text=True)
            st.text_area("Результат ANTIMMERGE (KRASNODAR)", (result.stdout or '') + (result.stderr or ''), height=300)
        else:
            st.error("Скрипт antimerge_krasnodar.py не найден.")

with col3:
    if st.button("❌ ANTIMMERGE (MOSCOW)"):
        if os.path.exists("antimerge_moscow.py"):
            result = subprocess.run(["python", "antimerge_moscow.py"], capture_output=True, text=True)
            st.text_area("Результат ANTIMMERGE (MOSCOW)", (result.stdout or '') + (result.stderr or ''), height=300)
        else:
            st.error("Скрипт antimerge_moscow.py не найден.")

with col4:
    if st.button("❌ ANTIMMERGE (KAL)"):
        if os.path.exists("antimerge_kal.py"):
            result = subprocess.run(["python", "antimerge_kal.py"], capture_output=True, text=True)
            st.text_area("Результат ANTIMMERGE (KAL)", (result.stdout or '') + (result.stderr or ''), height=300)
        else:
            st.error("Скрипт antimerge_kal.py не найден.")
            
with col5:
    if st.button("❌ ANTIMMERGE (EKB)"):
        if os.path.exists("antimerge_ekb.py"):
            result = subprocess.run(["python", "antimerge_ekb.py"], capture_output=True, text=True)
            st.text_area("Результат ANTIMMERGE (EKB)", (result.stdout or '') + (result.stderr or ''), height=300)
        else:
            st.error("Скрипт antimerge_ekb.py не найден.")

#-----------------------------------------------КРАСНОДАРСКИЕ ОПЕРАЦИИ------------------------------------------------------------------------------
st.markdown("---")
st.subheader("📄 FBS КРАСНОДАР")

standard_actions = {
    "🚚 Создать КРАСНОДАРСКИЕ поставки": "create_supplies_krd/create_supplies_{}.py"
}

for label, script_template in standard_actions.items():
    if st.button(label):
        script_name = script_template.format(person_id)
        if os.path.exists(script_name):
            result = subprocess.run(["python", script_name], capture_output=True, text=True)
            st.text_area(f"Результат: {label}", (result.stdout or '') + (result.stderr or ''), height=300)
        else:
            st.error(f"Скрипт {script_name} не найден.")

#------------------------------------------------МОСКОВСКИЕ ОПЕРАЦИИ------------------------------------------------------------------------------
st.markdown("---")
st.subheader("🏢 FBS МОСКВА")

moscow_actions = {
    "🚚 Создать МОСКОВСКИЕ поставки": "create_supplies_msk/create_supplies_msk_{}.py"
}

for label, script_template in moscow_actions.items():
    if st.button(label):
        script_name = script_template.format(person_id)
        if os.path.exists(script_name):
            result = subprocess.run(["python", script_name], capture_output=True, text=True)
            st.text_area(f"Результат: {label}", (result.stdout or '') + (result.stderr or ''), height=300)
        else:
            st.error(f"Скрипт {script_name} не найден.")

#------------------------------------------------------КАЛ ОПЕРАЦИИ------------------------------------------------------------------------------
st.markdown("---")
st.subheader("🏢 FBS КАЛ КОНКРЕТНЫЙ")

moscow_actions = {
    "🚚 Создать КАЛОВЫЕ поставки": "create_supplies_kal/create_supplies_kal_{}.py"
}

for label, script_template in moscow_actions.items():
    if st.button(label):
        script_name = script_template.format(person_id)
        if os.path.exists(script_name):
            result = subprocess.run(["python", script_name], capture_output=True, text=True)
            st.text_area(f"Результат: {label}", (result.stdout or '') + (result.stderr or ''), height=300)
        else:
            st.error(f"Скрипт {script_name} не найден.")

#------------------------------------------------------ЕКБ ОПЕРАЦИИ------------------------------------------------------------------------------
st.markdown("---")
st.subheader("🏢 FBS ЕКБ")

moscow_actions = {
    "🚚 Создать екб поставки": "create_supplies_ekb/create_supplies_ekb_{}.py"
}

for label, script_template in moscow_actions.items():
    if st.button(label):
        script_name = script_template.format(person_id)
        if os.path.exists(script_name):
            result = subprocess.run(["python", script_name], capture_output=True, text=True)
            st.text_area(f"Результат: {label}", (result.stdout or '') + (result.stderr or ''), height=300)
        else:
            st.error(f"Скрипт {script_name} не найден.")

#-----------------------------------------------ОБРАБОТКА ЛИСТОВ ПОДБОРА------------------------------------------------------------------------------
st.markdown("---")
st.subheader("📝 Обработать листы подбора (добавить колонки, даты, списки и т.п.)")

process_script = "list_podbor/urgen_ahsatan.py"   # ← укажите название вашего скрипта

if st.button("⚙️ Запустить обработку листов подбора"):
    if not os.path.exists(process_script):
        st.error(f"Скрипт не найден: {process_script}")
    else:
        try:
            result = subprocess.run(
                [sys.executable, process_script],
                capture_output=True,
                text=True
            )

            if result.returncode == 0:
                st.success("Обработка завершена успешно.")
                st.text_area("Лог обработки", (result.stdout or "") + (result.stderr or ""), height=250)
            else:
                st.error("Во время обработки возникли ошибки.")
                st.text_area("Лог обработки", (result.stdout or "") + (result.stderr or ""), height=250)

        except Exception as ex:
            st.error(f"Ошибка при запуске скрипта: {ex}")

#-----------------------------------------------СРОК ГОДНОСТИ------------------------------------------------------------------------------
# Словарь с API-ключами по кабинетам
api_keys = {
    "A": API_A,
    "B": API_B,
    "C": API_C,
    "D": API_D,
    "E": API_E,
    "F": API_F
}
st.markdown("---")
st.subheader("⌛ Закрепить сроки годности (FBS)")

if st.button("📌 Отправить сроки годности в WB"):
    api_key = api_keys.get(person_id)

    if not api_key:
        st.error("Не найден API-ключ для выбранной группы.")
    else:
        try:
            import sys, os
            sys.path.append(os.path.join(os.path.dirname(__file__), "list_podbor"))

            from set_experation import run   # <-- если файл называется set_expiration.py — замените тут

            import io
            buf = io.StringIO()

            # перехват stdout, чтобы показать лог в интерфейсе
            import sys
            old_out = sys.stdout
            sys.stdout = buf

            run(api_key)

            sys.stdout = old_out

            st.success("Обработка завершена.")
            st.text_area("Лог выполнения", buf.getvalue(), height=300)

        except Exception as ex:
            st.error(f"Ошибка выполнения: {ex}")




#-----------------------------------------------ПЕРЕНОС ЗАКАЗОВ В НЕ КУПИЛИ------------------------------------------------------------------------------
st.markdown("---")
st.subheader("📤 Перенос в НЕ КУПИЛИ")

nobuy_script = f"replace_in_nobuy/nobuy_{person_id}.py"
if st.button("🚫 Перенести выбранную группу в НЕ КУПИЛИ (авто)"):
    # 1) Берём таблицу активных поставок для выбранного кабинета из сайдбара
    df_selected = st.session_state.active_supplies.get(person_id)
    if df_selected is None or df_selected.empty:
        st.error("Нет данных активных поставок. Сначала обновите таблицы в сайдбаре.")
    elif "Номер поставки" not in df_selected.columns:
        st.error("В таблице нет столбца 'Номер поставки'. Проверьте формат Excel.")
    else:
        # 2) Ищем строку с поставкой 'НЕ КУПИЛИ'
        mask = df_selected["Номер поставки"].astype(str).str.contains("НЕ КУПИЛИ", case=False, na=False)
        rows = df_selected.loc[mask]

        if rows.empty:
            st.error("В активных поставках не найдено 'НЕ КУПИЛИ'.")
        else:
            # 3) Берём ID поставки из одной из типовых колонок
            candidate_id_cols = ["id", "ID", "Id", "Айди", "ID поставки", "Айди поставки"]
            id_col = next((c for c in candidate_id_cols if c in rows.columns), None)

            if not id_col:
                st.error("В таблице не найден столбец с ID поставки (ожидались: id/ID/Айди/ID поставки).")
            else:
                supply_id = str(rows.iloc[0][id_col]).strip()
                if not supply_id:
                    st.error("ID поставки пустой. Проверьте таблицу.")
                elif not os.path.exists(nobuy_script):
                    st.error(f"Скрипт {nobuy_script} не найден.")
                else:
                    try:
                        # 4) Подставляем supply_id в nobuy_{person_id}.py
                        with open(nobuy_script, "r", encoding="utf-8") as f:
                            src = f.read()

                        new_src, nsubs = re.subn(
                            r'supply_id\s*=\s*["\'][^"\']+["\']',
                            f'supply_id = "{supply_id}"',
                            src,
                            count=1
                        )

                        if nsubs == 0:
                            st.error("Не удалось найти и заменить переменную supply_id в скрипте.")
                        else:
                            with open(nobuy_script, "w", encoding="utf-8") as f:
                                f.write(new_src)

                            # 5) Запускаем скрипт и показываем лог
                            result = subprocess.run(
                                ["python", nobuy_script],
                                capture_output=True, text=True
                            )
                            if result.returncode == 0:
                                st.success(f"Группа перенесена в 'НЕ КУПИЛИ'. ID: {supply_id}")
                                st.text_area("Логи", (result.stdout or "") + (result.stderr or ""), height=300)
                            else:
                                st.error("Ошибка при переносе в 'НЕ КУПИЛИ'")
                                st.text_area("Логи", (result.stdout or "") + (result.stderr or ""), height=300)

                    except Exception as ex:
                        st.error(f"Ошибка при подготовке/запуске скрипта: {ex}")


#--------------------------------------САЙДБАР ЕЩЕ----------------------------------------------------------------------------------

# Кнопка под выбором кабинета: обновить сразу все кабинеты
if st.sidebar.button("🔄 Обновить активные поставки по ВСЕМ кабинетам"):
    updated = 0
    for pid in people.values():
        df_all = load_active_supplies_for(pid)
        st.session_state.active_supplies[pid] = df_all
        updated += 1
    st.sidebar.success(f"Обновлено таблиц: {updated}")

# Отдельная кнопка: обновить выбранный кабинет
colA, colB = st.columns([1, 2])
with colA:
    if st.sidebar.button("🔁 Обновить только выбранный кабинет"):
        df_sel = load_active_supplies_for(person_id)
        st.session_state.active_supplies[person_id] = df_sel
        st.sidebar.success(f"Обновлена таблица для: {selected_person}")

# Поле 2: выбранный кабинет — отдельный виджет
st.sidebar.markdown("#### 🎯 Выбранный кабинет")
df_selected = st.session_state.active_supplies.get(person_id)
if df_selected is None:
    st.sidebar.warning("Для выбранного кабинета данные ещё не загружены. Нажмите одну из кнопок обновления выше.")
else:
    st.sidebar.dataframe(df_selected, use_container_width=True)


#--------------------------------------САЙДБАР КОНЕЦ--------------------------------------------------------------------------------------
st.markdown("---")
st.subheader("🚚 Передать поставку в доставку")

# Ввод ID поставки
deliver_supply_id = st.text_input("Введите ID поставки для передачи в доставку")

# Словарь с API-ключами по кабинетам
api_keys = {
    "A": API_A,
    "B": API_B,
    "C": API_C,
    "D": API_D,
    "E": API_E,
    "F": API_F
}


api_key = api_keys.get(person_id)

if st.button("🚚 Передать выбранную поставку в доставку"):
    if not deliver_supply_id.strip():
        st.error("Введите ID поставки.")
    elif not api_key:
        st.error(f"Не найден API-ключ для кабинета: {person_id}")
    else:
        url = f"https://marketplace-api.wildberries.ru/api/v3/supplies/{deliver_supply_id.strip()}/deliver"
        headers = {"Authorization": api_key}
        try:
            response = requests.patch(url, headers=headers)
            response.raise_for_status()
            st.success(f"Поставка {deliver_supply_id.strip()} успешно передана в доставку.")
        except requests.HTTPError as e:
            st.error(f"Ошибка HTTP: {e.response.status_code}\n{e.response.text}")
        except Exception as ex:
            st.error(f"Неизвестная ошибка: {ex}")

#-----------------------------------------------ПОЛУЧИТЬ КЬЮАР ПОСТАВКИ------------------------------------------------------------------------------

st.markdown("---")
st.subheader("🏷️ Получить QR-код поставки")

# Ввод ID
barcode_supply_id = st.text_input("Введите ID поставки для получения QR-кода")

# Выбор формата
barcode_type = st.selectbox(
    "Выберите формат стикера",
    ["png", "svg", "zplv", "zplh"],
    index=0
)

if st.button("📥 Получить QR-код"):
    if not barcode_supply_id.strip():
        st.error("Введите ID поставки.")
    elif not api_key:
        st.error(f"Не найден API-ключ для кабинета: {person_id}")
    else:
        url = f"https://marketplace-api.wildberries.ru/api/v3/supplies/{barcode_supply_id.strip()}/barcode"
        headers = {"Authorization": api_key}
        params = {"type": barcode_type}

        try:
            response = requests.get(url, headers=headers, params=params)
            response.raise_for_status()
            data = response.json()

            # Берём base64-код из ответа
            file_base64 = data.get("file")
            if not file_base64:
                st.error("Ответ не содержит файла.")
            else:
                decoded = base64.b64decode(file_base64)
                # Сохраняем
                output_file = f"D:/Софт/скрипты и аутпутс/Списки поставок/qr_{barcode_supply_id.strip()}.{barcode_type}"
                with open(output_file, "wb") as f:
                    f.write(decoded)

                st.success(f"QR-код сохранён как {output_file}")

                # Если формат изображений — показать
                if barcode_type in ("png", "svg"):
                    st.image(decoded, caption="QR-код", use_container_width=False)
                else:
                    st.info("Тип стикера — не визуализируется (ZPL).")
        except requests.HTTPError as e:
            st.error(f"Ошибка HTTP: {e.response.status_code}\n{e.response.text}")
        except Exception as ex:
            st.error(f"Неизвестная ошибка: {ex}")

#-----------------------------------------------------ОБНОВЛЕНИЕ ОСТАТКОВ------------------------------------------------------------------------------TODO пересмотреть

# --- Обновление остатков по артикулу продавца (массовое) ---
st.markdown("---")
st.subheader("📊 Обновить остатки по артикулу (для всех баркодов)")

# Справочник складов по группе
warehouses_by_group = {
    "ГРУППА F": {"КРАСНОДАР": "1312919", "МОСКВА": "1367610", "КАЛ": "1505283"},
    "ГРУППА D": {"КРД": "754193", "ЗЕЛ": "1453417", "МСК": "1493800", "ЕКБ": "1640824"},
    "ГРУППА А": {"КРД": "", "ЗЕЛ": "", "МСК": "", "ЕКБ": ""},
    "ГРУППА E": {"КРД": "1640880", "ЗЕЛ": "1640883", "МСК": "1640882"},
    "ГРУППА C": {"КРД": "", "ЗЕЛ": "", "МСК": "", "ЕКБ": ""},
    "ГРУППА B": {"КРД": "", "ЗЕЛ": "", "МСК": "", "ЕКБ": ""},
    "ГРУППА H": {"КРД": "", "ЗЕЛ": "", "МСК": "", "ЕКБ": ""},
    "ГРУППА G": {"КРД": "", "ЗЕЛ": "", "МСК": "", "ЕКБ": ""},
}

# Путь к файлу базы и лога
db_path = r"D:/Софт/База данных/База данных.xlsx"
log_file = r"D:/Софт/скрипты и аутпутс/Остатки/остатки_логи.xlsx"

# Вводы
article_input = st.text_input("Введите артикул продавца (будут найдены все совпадения)")
amount_input2 = st.text_input("Введите остаток товара")

warehouses_for_group = warehouses_by_group.get(selected_person, {})
warehouse_name = st.selectbox(
    "Выберите склад:",
    list(warehouses_for_group.keys()) if warehouses_for_group else [],
    index=0 if warehouses_for_group else None
)
warehouse_id2 = warehouses_for_group.get(warehouse_name)

def _chunked(lst, n=1000):
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

if st.button("🔄 Найти баркоды и обновить остатки"):
    # Базовые проверки
    if not db_path.strip() or not article_input.strip() or not amount_input2.strip() or not warehouse_id2:
        st.error("Пожалуйста, заполните все поля.")
    elif not os.path.exists(db_path):
        st.error("Файл базы данных не найден.")
    elif not api_key:
        st.error(f"Не найден API-ключ для кабинета: {person_id}")
    else:
        try:
            amount = int(amount_input2)
        except ValueError:
            st.error("Количество должно быть целым числом.")
            st.stop()

        if amount < 0:
            st.error("Остаток не может быть отрицательным.")
            st.stop()

        try:
            # Загружаем базу
            df_base = pd.read_excel(db_path, header=0)
        except Exception as ex:
            st.error(f"Не удалось прочитать базу: {ex}")
            st.stop()

        # Проверяем колонки
        if not {"Артикул продавца", "Баркод"}.issubset(df_base.columns):
            st.error("В базе должны быть колонки 'Артикул продавца' и 'Баркод'.")
            st.stop()

        # Фильтруем по артикулу
        rows = df_base.loc[df_base["Артикул продавца"].astype(str).str.strip() == article_input.strip()]
        if rows.empty:
            st.error("Артикул не найден в базе данных.")
            st.stop()

        sku_list = rows["Баркод"].astype(str).str.strip().dropna().unique().tolist()
        st.write(f"Найдены баркоды ({len(sku_list)}): {sku_list}")

        url = f"https://marketplace-api.wildberries.ru/api/v3/stocks/{warehouse_id2}"
        headers = {"Content-Type": "application/json", "Authorization": api_key}

        success_skus_all = []
        error_skus_all = []

        # Отправляем пачками по 1000
        for idx, part in enumerate(_chunked(sku_list, 1000), start=1):
            body = {"stocks": [{"sku": sku, "amount": amount} for sku in part]}
            try:
                response = requests.put(url, headers=headers, json=body, timeout=30)
            except Exception as ex:
                st.error(f"[Пачка {idx}] Сетевая ошибка: {ex}")
                error_skus_all.extend(part)
                continue

            if response.status_code == 204:
                st.success(f"[Пачка {idx}] ✅ Обновлён остаток {amount} для {len(part)} SKU.")
                success_skus_all.extend(part)
            else:
                # Пытаемся выделить SKU с ошибкой из тела (чаще при 409)
                try:
                    data = response.json()
                except Exception:
                    data = {}

                bad = []
                if response.status_code == 409:
                    try:
                        bad = [item["sku"] for item in data.get("data", []) if "sku" in item]
                    except Exception:
                        bad = []

                if bad:
                    ok = [sku for sku in part if sku not in bad]
                    if ok:
                        st.success(f"[Пачка {idx}] Частично успешно: {len(ok)} SKU.")
                    st.error(f"[Пачка {idx}] ⚠️ Ошибка 409. Не обновлено: {bad}")
                    success_skus_all.extend(ok)
                    error_skus_all.extend(bad)
                else:
                    st.error(f"[Пачка {idx}] ⚠️ Ошибка {response.status_code}: {response.text}")
                    error_skus_all.extend(part)

        # Итог
        st.markdown("### Итоги")
        st.write(f"Успешно: {len(success_skus_all)} шт.")
        st.write(f"Не удалось: {len(error_skus_all)} шт.")

        # Лог только успешных
        if success_skus_all:
            try:
                log_entry = pd.DataFrame([{
                    "Дата и время": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Группа": selected_person,
                    "Артикул продавца": article_input.strip(),
                    "Баркоды": ", ".join(success_skus_all),
                    "Склад": warehouse_name,
                    "ID склада": warehouse_id2,
                    "Остаток": amount
                }])

                if not os.path.exists(log_file):
                    os.makedirs(os.path.dirname(log_file), exist_ok=True)
                    log_entry.to_excel(log_file, index=False)
                else:
                    existing_log = pd.read_excel(log_file)
                    pd.concat([existing_log, log_entry], ignore_index=True).to_excel(log_file, index=False)

                st.info(f"Лог записан: {log_file}")
            except Exception as ex:
                st.warning(f"Не удалось записать лог: {ex}")

        if not success_skus_all and error_skus_all:
            st.warning("Все позиции вернулись с ошибкой — проверьте артикул/базу/анкеты товара/разрешения на склад.")

#-----------------------------------------------УДАЛЕНИЕ ТАБЛИЦ В ПОСТАВКАХ------------------------------------------------------------------------------

# --- Удаление всех .xlsx из папки Списки поставок ---
st.markdown("---")
st.sidebar.subheader("🗑️ Очистка списка поставок (удаление всех .xlsx)")

supplies_dir = r"D:\Софт\скрипты и аутпутс\Списки поставок"

if st.sidebar.button("🗑️ Удалить все .xlsx из папки Списки поставок"):
    import glob
    if not os.path.exists(supplies_dir):
        st.sidebar.error(f"Папка не найдена: {supplies_dir}")
    else:
        pattern = os.path.join(supplies_dir, "*.xlsx")
        files = glob.glob(pattern)
        if not files:
            st.sidebar.info("Нет файлов .xlsx для удаления.")
        else:
            deleted_count = 0
            for file_path in files:
                try:
                    os.remove(file_path)
                    deleted_count += 1
                except Exception as e:
                    st.sidebar.error(f"Ошибка при удалении {file_path}: {e}")
            st.sidebar.success(f"Удалено файлов: {deleted_count}")

if st.sidebar.button("🚀 Запустить обработку"):
    try:
        # Запускаем ваш готовый скрипт (например highlight_three.py)
        # sys.executable гарантирует запуск тем же интерпретатором, что и streamlit
        result = subprocess.run(
            [sys.executable, "подсветка.py"],
            capture_output=True,
            text=True
        )
        st.success("Скрипт выполнен!")
        st.code(result.stdout)
        if result.stderr:
            st.error("Ошибки во время выполнения:")
            st.code(result.stderr)
    except Exception as e:
        st.error(f"Ошибка запуска: {e}")

