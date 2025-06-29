import requests
import pandas as pd 
import os
import time
from sqlalchemy import create_engine
from functions import (
    get_records_and_clients, 
    get_services,
    get_service_categories, 
    get_staff, 
    get_schedule
)
from dotenv import load_dotenv

load_dotenv()
partner_token = os.getenv('PARTNER_TOKEN')
login = os.getenv('LOGIN')
password = os.getenv('PASSWORD')
company_id = os.getenv('COMPANY_ID')
partner_id = os.getenv('PARTNER_ID')

# Получаем токен через авторизвцию
user_token = requests.post(
    'https://api.yclients.com/api/v1/auth', 
    headers={
        "Authorization": f"Bearer {partner_token}", 
        "Accept": "application/vnd.yclients.v2+json"
    }, 
    json={"login": login, "password": password}
).json()['data']['user_token']

# Устанавливаем заголовки для запросов
headers = {
    "Accept": "application/vnd.yclients.v2+json",
    "Content-Type" : "application/json",
    "Authorization": f"Bearer {partner_token}, User {user_token}"
}

# === Данные подключения к PostgreSQL ===
db_user = os.getenv('DB_USER')
db_password = os.getenv('DB_PASSWORD')
db_name = os.getenv('DB_NAME')
db_host = os.getenv('DB_HOST')
db_port = os.getenv('DB_PORT')

# Строка подключения
connection_string = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
engine = create_engine(connection_string)

# === Основной процесс ===
while True:
    print("🚀 Начинаем полную загрузку данных из YClients и запись в PostgreSQL...\n")

    staff = get_staff(company_id, headers)
    schedule = get_schedule(company_id, headers)
    service_categories = get_service_categories(company_id, headers)
    services = get_services(company_id, headers)
    records, clients = get_records_and_clients(company_id, headers)

    entities = {
        "clients": clients,
        "records": records,
        "staff": staff,
        "schedule" : schedule,
        "service_categories": service_categories,
        "services" : services
    }
        
    for table_name, df in entities.items():
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"📥 Данные загружены в таблицу {table_name} ({len(df)} строк).")
        # df.to_excel(f"Dashboard/{table_name}.xlsx", index=False) # Для выгрузки excel

    print("\n🎉 Все данные успешно загружены в БД!")
    print(f"🕒 Следующая загрузка через 24 часа...")
    time.sleep(60 * 60 * 24)