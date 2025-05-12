from tqdm import tqdm
import requests
from sqlalchemy import Table, Column, MetaData, String, Numeric, JSON, DateTime, Integer, Boolean
from sqlalchemy.dialects.postgresql import JSONB
from datetime import datetime, timedelta
import json

# === Функция для загрузки всех страниц из API ===
def get_all_pages(url, headers, method='GET', body=None):
    page = 1
    all_data = []

    with tqdm(desc=f"Загрузка {url.split('/')[-1]}") as pbar:
        while True:
            params_or_json = {"page": page, "page_size": 100}
            if method == 'GET':
                response = requests.get(url, headers=headers, params=params_or_json)
            else:
                if body:
                    body.update(params_or_json)
                response = requests.post(url, headers=headers, json=body)

            if response.status_code != 200:
                print(f"Ошибка запроса: {response.status_code} {response.text}")
                break

            data = response.json().get('data', [])
            if not data:
                break

            all_data.extend(data)
            page += 1
            pbar.update(len(data))

    return all_data

# Функция для преобразования колонки с персоналом
def transform_staff(dict_list):
    result = [
        {
            'id': item['id'],
            'seance_length': item['seance_length'],
            'price': item['price']['max'] if isinstance(item.get('price'), dict) else None
        }
        for item in dict_list
    ]
    return result

# Преобразует все столбцы, содержащие списки или словари, в строки JSON.
def normalize_json_columns(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, (dict, list))).any():
            df[col] = df[col].apply(
                lambda x: json.dumps(x, ensure_ascii=False) if isinstance(x, (dict, list)) else x
                )
    return df

# === Функция для создания таблиц с правильными типами данных в БД ===

def create_table_with_types(df, table_name, engine):
    metadata = MetaData()
    columns = []

    for col in df.columns:
        sample_value = df[col].dropna().iloc[0] if not df[col].dropna().empty else None

        if isinstance(sample_value, dict) or isinstance(sample_value, list):
            col_type = JSONB
        elif isinstance(sample_value, bool):
            col_type = Boolean
        elif isinstance(sample_value, int):
            col_type = Integer
        elif isinstance(sample_value, float):
            col_type = Numeric
        elif isinstance(sample_value, datetime.datetime):
            col_type = DateTime
        else:
            col_type = String

        columns.append(Column(col, col_type))

    table = Table(table_name, metadata, *columns)
    metadata.drop_all(engine, [table], checkfirst=True)  # Удалить если уже есть (заменить)
    metadata.create_all(engine)  # Создать таблицу с нужными типами
    print(f"✅ Таблица {table_name} создана с правильными типами колонок.")

# === Функция для заливки датафрейма в БД ===
def upload_to_postgres(df, table_name, engine):
    if not df.empty:
        create_table_with_types(df, table_name, engine)
        df.to_sql(table_name, engine, if_exists='append', index=False)
        print(f"📥 Данные загружены в таблицу {table_name} ({len(df)} строк).")
    else:
        print(f"⚠️ Таблица {table_name} пуста, пропущена.")