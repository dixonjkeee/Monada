import requests
import pandas as pd 
import os
from functions import get_records_and_clients, get_services, \
    get_service_categories, get_staff, upload_to_postgres
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
        "Authorization": f"Bearer {partner_token}", "Accept": "application/vnd.yclients.v2+json"
    }, 
    json={
        "login": login, "password": password
    }
).json()['data']['user_token']

# Устанавливаем заголовки для запросов
headers = {
    "Accept": "application/vnd.yclients.v2+json",
    "Content-Type" : "application/json",
    "Authorization": f"Bearer {partner_token}, User {user_token}"
}
        
# === Данные подключения к PostgreSQL ===
# db_user = 'user'
# db_password = 'password'
# db_name = 'Monada'
# db_host = 'localhost'
# db_port = '5433'

# Строка подключения
# connection_string = f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
# engine = create_engine(connection_string)


# === Основной процесс ===
print("🚀 Начинаем полную загрузку данных из YClients и запись в PostgreSQL...\n")

staff = get_staff(company_id, headers)
service_categories = get_service_categories(company_id, headers)
services = get_services(company_id, headers)
records, clients = get_records_and_clients(company_id, headers)

entities = {
    "clients": clients,
    "records": records,
    "staff": staff,
    "service_categories": service_categories,
    "services" : services
}

for table_name, df in entities.items():
    # print(f"\n=== {table_name.upper()} ===")
    # upload_to_postgres(df, table_name, engine)
    df.to_excel(f"Dashboard/{table_name}.xlsx", index=False) # Для выгрузки excel

print("\n🎉 Все данные успешно загружены в БД!")