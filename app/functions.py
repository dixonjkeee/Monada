from tqdm import tqdm
import requests
import pandas as pd

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
        'id', 'name', 'specialization', 'position.title', 'weight', 'rating', 'hidden', 
        'fired', 'dismissal_date', 'user.phone', 'user.email'
        ]
    response = requests.get(url, headers=headers)
    staff = pd.json_normalize(response.json()['data'])[cols].astype({
        'dismissal_date' : 'datetime64[ns]'
        })
    return staff

# === Расписание ===
def get_schedule(company_id, headers):
    url = f"https://api.yclients.com/api/v1/company/{company_id}/staff/schedule"
    today = pd.Timestamp.now().strftime('%Y-%m-%d')
    params = {
        'start_date' : '2024-01-01',
        'end_date' : today
    }
    response = requests.get(url, headers=headers, params=params)
    schedule = pd.json_normalize(response.json()['data']).explode('slots').reset_index(drop=True)
    schedule[['from', 'to']] =  pd.json_normalize(schedule['slots'])
    schedule.drop('slots', axis=1, inplace=True)
    return schedule


# === Категории услуг  ===
def get_service_categories(company_id, headers):
    url = f"https://api.yclients.com/api/v1/company/{company_id}/service_categories/"
    cols = ['id', 'category_id', 'salon_service_id', 'title', 'weight']
    response = requests.get(url, headers=headers)
    return pd.json_normalize(response.json()['data'])[cols]


# === Услуги  ===
def get_services(company_id, headers):
    url = f"https://api.yclients.com/api/v1/company/{company_id}/services/"
    cols = [
        'booking_title', 'service_type', 'schedule_template_type', 'online_invoicing_status',
        'price_prepaid_percent', 'id', 'salon_service_id', 'category_id', 'weight', 'duration'
    ]
    response = requests.get(url, headers=headers)
    services = pd.json_normalize(response.json()['data'])[cols]
    return services

# === Записи ===
def get_records_and_clients(company_id, headers):
    url = f"https://api.yclients.com/api/v1/records/{company_id}"
    
    cols = [
        'id', 'staff_id', 'services', 'goods_transactions', 'client', 'date',
        'attendance', 'length', 'visit_id', 'paid_full', 'payment_status'
    ]
    records = pd.DataFrame(get_all_pages(url, headers))[cols]
    
    # Создаем ДФ с клиентами
    clients = pd.json_normalize(records['client'])[['id', 'name', 'surname', 'phone', 'email']]\
        .groupby('id').last().reset_index().astype({'id' : 'int64'})
        
    # Убираем записи с неизвестными клиентами  
    records = records[records['client'].notna()]

    # Вытаскиваем нужные колонки по клиентам
    records['client_is_new'] = records['client'].apply(lambda x: x.get('is_new'))
    records['client'] = records['client'].apply(lambda x: x.get('id'))

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
    # Меняем типы данных
    records = records.astype({
        'client' : 'Int64', 'date' : 'datetime64[ns]', 'client_is_new' : 'bool',
        'good_transaction_cost_to_pay' : 'Int64', 'good_transaction_good_id' : 'Int64',
        'service_cost_to_pay' : 'int64'
    })
    return records, clients