#settings.py

"""
Universal ETL Pipeline Configuration
Generic settings for data integration from ERP, CRM and SQL Server
"""

import os
from pathlib import Path

# Base paths
BASE_DIR = Path(__file__).parent.parent
DATA_DIR = BASE_DIR / "data"

# Generic API Configuration
API_CONFIG = {
    'crm_system': {
        'base_url': os.getenv('CRM_BASE_URL', 'https://crm.company.com'),
        'timeout': int(os.getenv('API_TIMEOUT', 30)),
        'retry_attempts': int(os.getenv('API_RETRY_ATTEMPTS', 3)),
        'page_size': int(os.getenv('CRM_PAGE_SIZE', 100))
    },
    'erp_system': {
        'base_url': os.getenv('ERP_BASE_URL', 'https://erp.company.com'),
        'timeout': int(os.getenv('API_TIMEOUT', 30)),
        'version': os.getenv('ERP_API_VERSION', 'v1'),
        'batch_size': int(os.getenv('ERP_BATCH_SIZE', 500))
    }
}

# SQL Server Database Configuration
DATABASE_CONFIG = {
    'driver': os.getenv('DB_DRIVER', 'ODBC Driver 18 for SQL Server'),
    'server': os.getenv('DB_SERVER', 'localhost'),
    'port': int(os.getenv('DB_PORT', 1433)),
    'database': os.getenv('DB_NAME', 'business_intelligence'),
    'schema': os.getenv('DB_SCHEMA', 'staging'),
    'connection_timeout': int(os.getenv('DB_TIMEOUT', 30)),
    'command_timeout': int(os.getenv('DB_COMMAND_TIMEOUT', 600))
}

