"""
api-crm.py
Модуль для работы с API CRM
"""

import requests
import pandas as pd
from pathlib import Path
import sys
from datetime import datetime

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from config.credentials import CRM_API_USERNAME, CRM_API_PASSWORD, CRM_ENDPOINT, CRM_AUTH_ENDPOINT, CurrentConfig


class CRMClient:
    def __init__(self):
        self.session = requests.Session()
        self.base_url = CurrentConfig.CRM_BASE_URL

    def auth(self, username, password):
        auth_url = f"{self.base_url}{CRM_AUTH_ENDPOINT}"
        auth_data = {"UserName": username, "UserPassword": password}
        response = self.session.post(auth_url, json=auth_data)

        print(f"Код ответа: {response.status_code}")
        print(auth_url)
        return response.status_code == 200 and response.json().get("Code") == 0

    def get_experts(self, date_str=None, only_active=False):
        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")

        start_date = f"{date_str}T00:00:01Z"
        end_date = f"{date_str}T23:59:59Z"

        select_fields = "TrcUnitNumber,TrcShoppingMall,TrcIsChief,TrcContactFullName,TrcRespStartDate,TrcRespEndDate,ModifiedOn,TrcBooleanActive"
        filter_condition = f"ModifiedOn ge {start_date} and ModifiedOn le {end_date}"

        if only_active:
            filter_condition += " and TrcBooleanActive eq true"

        api_url = f"{self.base_url}{CRM_ENDPOINT}?$select={select_fields}&$filter={filter_condition}"
        response = self.session.get(api_url)
        return response.json() if response.status_code == 200 else None


def save_to_csv(df, filename, output_dir='data/raw'):
    output_path = project_root / output_dir / filename
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding='utf-8')
    print(f"Успешно: {len(df)} записей сохранено в {filename}")


def extract_crm_data():
    try:
        client = CRMClient()

        if not client.auth(CRM_API_USERNAME, CRM_API_PASSWORD):
            print("Ошибка авторизации CRM")
            return False

        experts_data = client.get_experts()
        if not experts_data:
            print("Нет данных от CRM")
            return False

        df = pd.DataFrame(experts_data.get('value', []))
        save_to_csv(df, 'crm.csv')
        print(f"Количество записей CRM: {len(df)}")
        return True

    except Exception as e:
        print(f"Ошибка CRM: {e}")
        return False


if __name__ == "__main__":
    success = extract_crm_data()
    if not success:
        sys.exit(1)