import requests
import pandas as pd
from datetime import datetime

import sys, os
from pathlib import Path 
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.append(str(PROJECT_ROOT))

from config import API_F

HEADERS = {'Authorization': API_F}
URL = 'https://marketplace-api.wildberries.ru/api/v3/orders/new'

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
    df['Продавец'] = 'Я ЧОРНИ'
    df['Группа'] = 'F'
    df.to_excel('D:/Софт/скрипты и аутпутс/Выходы F/задания_F.xlsx', index=False)
    print("Упрощённые данные сохранены в 'D:/Софт/скрипты и аутпутс/Выходы F/задания_F.xlsx'")
else:
    print("Нет новых заданий.")
