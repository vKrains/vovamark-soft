import requests
import pandas as pd
from datetime import datetime

import sys, os
from pathlib import Path 
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

import streamlit as st
API_A = st.secrets.get("API_A", "")

HEADERS = {'Authorization': API_A}
URL = 'https://marketplace-api.wildberries.ru/api/v3/orders/new'

out_dir = Path("/tmp/Выходы A")
out_dir.mkdir(parents=True, exist_ok=True)

# === ПРЕФИКСЫ К НАЗВАНИЯМ ===
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
    'LENTA': 'ЛЕНТА',
    'PEREK': 'ПЕРЕКРЁСТОК',
    'PDRG': 'ПОДРУЖКА'
}

def get_magazin_by_article(article):
    for prefix in PREFIX_TO_SUPPLY:
        if article.startswith(prefix):
            return PREFIX_TO_SUPPLY[prefix]
    return ''

# === ЗАПРОС СБОРОЧНЫХ ЗАДАНИЙ ===
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

# === СОХРАНЕНИЕ В EXCEL ===
if data:
    df = pd.DataFrame(data)
    df['Продавец'] = 'ОБЩИЙ'
    df['Группа'] = 'A'
    df.to_excel(out_dir / "задания_A.xlsx", index=False)
    print("Упрощённые данные сохранены в 'D:/Софт/скрипты и аутпутс/Выходы A/задания_A.xlsx'")
else:
    print("Нет новых заданий.")
