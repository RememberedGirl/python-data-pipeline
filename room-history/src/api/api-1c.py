import requests
import pandas as pd
from pathlib import Path
import sys

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from config.credentials import CRM_API_USERNAME, CRM_API_PASSWORD, CRM_AUTH_ENDPOINT, CRM_ENDPOINT, CurrentConfig

session = requests.Session()

# Авторизация через сервис
auth_url = f"{CurrentConfig.CRM_BASE_URL}{CRM_AUTH_ENDPOINT}"
auth_data = {"UserName": CRM_API_USERNAME, "UserPassword": CRM_API_PASSWORD}

print(f"URL авторизации: {auth_url}")
print(f"Тело запроса авторизации: {auth_data}")

auth_response = session.post(auth_url, json=auth_data)

print(f"Код авторизации: {auth_response.status_code}")
print(f"Ответ авторизации: {auth_response.text}")

if auth_response.status_code == 200 and auth_response.json().get("Code") == 0:
    # Запрос данных с датой 01.01.2025
    start_date = "2025-01-01T00:00:01Z"
    select_fields = "TrcUnitNumber,TrcShoppingMall,TrcIsChief,TrcContactFullName,TrcRespStartDate,TrcRespEndDate,ModifiedOn,TrcBooleanActive"
    filter_condition = f"ModifiedOn ge {start_date}"

    url = f"{CurrentConfig.CRM_BASE_URL}{CRM_ENDPOINT}?$select={select_fields}&$filter={filter_condition}"
    print(f"URL данных: {url}")

    response = session.get(url, timeout=30)
    print(f"Код ответа данных: {response.status_code}")

    if response.status_code == 200:
        data = response.json()
        df = pd.DataFrame(data.get('value', []))

        # Сохраняем в файл
        output_path = project_root / 'data' / 'raw' / 'expert.csv'
        output_path.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(output_path, index=False, encoding='utf-8')
        print(f"Сохранено {len(df)} записей в expert.csv")
    else:
        print(f"Ошибка данных: {response.text}")
else:
    print("Ошибка авторизации")