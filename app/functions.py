from tqdm import tqdm
import requests
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy import Table, Column, MetaData, String, Numeric, JSON, DateTime, Integer, Boolean
from sqlalchemy.dialects.postgresql import JSONB
from tqdm import tqdm
from datetime import datetime, timedelta

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


# === Сотрудники ===
def get_staff(company_id, headers):
    url=f"https://api.yclients.com/api/v1/company/{company_id}/staff/"
    cols = [
        'id', 'name', 'specialization', 'position.title', 'weight', 'rating', 'avatar', 
        'avatar_big', 'information', 'hidden', 'fired', 'dismissal_date', 'dismissal_reason', 
        'schedule_till', 'has_schedule', 'user.phone', 'user.email'
        ]
    response = requests.get(url, headers=headers)
    return pd.json_normalize(response.json()['data'])[cols]


# === Категории услуг  ===
def get_service_categories(company_id, headers):
    url = f"https://api.yclients.com/api/v1/company/{company_id}/service_categories/"
    cols = ['id', 'category_id', 'salon_service_id', 'title', 'weight', 'staff']
    response = requests.get(url, headers=headers)
    return pd.json_normalize(response.json()['data'])[cols]


# === Услуги  ===
def get_services(company_id, headers):
    url = f"https://api.yclients.com/api/v1/company/{company_id}/services/"
    cols = [
        'booking_title', 'service_type', 'schedule_template_type', 'online_invoicing_status',
        'price_prepaid_percent', 'id', 'salon_service_id', 'category_id', 'weight', 
        'staff', 'duration'
    ]
    response = requests.get(url, headers=headers)
    services = pd.json_normalize(response.json()['data'])[cols]
    services['staff'] = services['staff'].apply(
        lambda dict_list: [
            {
                'id': item['id'],
                'seance_length': item['seance_length'],
                'price': item['price']['max'] if isinstance(item.get('price'), dict) else None
            }
            for item in dict_list
        ]
    )
    return services


# === Клиенты  ===
def get_clients(company_id, headers):
    url = f"https://api.yclients.com/api/v1/company/{company_id}/clients/search"
    body = {"fields": ["id", "name", "surname", "phone", "email"]}
    clients_from_api = pd.DataFrame(get_all_pages(url, headers, method='POST', body=body))

    url_rec = f"https://api.yclients.com/api/v1/records/{company_id}"
    clients_from_records = pd.DataFrame(get_all_pages(url_rec, headers))['client']
    clients_from_records = pd.json_normalize(clients_from_records)[[
        'id', 'name', 'surname', 'phone', 'email']].dropna(subset='id').astype({'id' : 'int32'})
    
    all_clients = pd.concat(
        [
            clients_from_api, 
            clients_from_records
        ], axis=0
    ).drop_duplicates('id').reset_index(drop=True)
    return all_clients


# === Продукты ===
def get_goods(company_id, headers):
    url = f"https://api.yclients.com/api/v1/goods/{company_id}/"
    cols = [
        'title', 'category', 'category_id', 'good_id', 'cost', 
        'unit_short_title', 'actual_cost','last_change_date'
    ]
    return pd.DataFrame(get_all_pages(url, headers))[cols]


# === Записи ===
def get_records(company_id, headers):
    url = f"https://api.yclients.com/api/v1/records/{company_id}"
    cols = [
        'id', 'staff_id', 'services', 'goods_transactions', 'client', 'date',
        'create_date', 'attendance', 'length', 'visit_id', 'paid_full', 'payment_status'
    ]
    records = pd.DataFrame(get_all_pages(url, headers))[cols]
    records.loc[records['client'].notnull(), 'client'] \
        = records.loc[records['client'].notnull(), 'client'].apply(lambda x: x.get('id'))

    # Убираем записи с пустым списком услуг, затем разворачиваем ДФ по услугам и продажам товаров 
    records = records[records['services'].astype(bool)]\
        .explode('services').explode('goods_transactions').reset_index(drop=True)

    # Создаем ДФ с услугами
    services_part = pd.json_normalize(records['services'])[[
        'id', 'title', 'cost_to_pay','discount', 'first_cost'
    ]].add_prefix("service_")

    # Создаем ДФ с продажами товаров
    good_transactions_part = pd.json_normalize(records['goods_transactions'])[[
        'title', 'cost_to_pay', 'good_id'
    ]].add_prefix("good_transaction_")

    # Соединяем их по индексам строк
    records = pd.concat(
        [
            records.drop(['services', 'goods_transactions'], axis=1), 
            services_part, 
            good_transactions_part
        ], axis=1
    )
    return records


# === Функция для создания правильных типов данных ===
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