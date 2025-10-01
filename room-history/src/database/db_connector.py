# src/database/db_connector.py
"""
Универсальный коннектор для работы с SQL Server
"""

import pyodbc
import logging
import pandas as pd
import sqlalchemy as sa
from typing import List, Dict, Optional, Any
from contextlib import contextmanager
import urllib.parse

logger = logging.getLogger(__name__)


class SQLServerConnector:
    """
    Универсальный коннектор для работы с SQL Server
    Поддерживает различные методы аутентификации
    """

    def __init__(self, server: str, database: str, username: str = None,
                 password: str = None, driver: str = 'ODBC Driver 18 for SQL Server',
                 use_windows_auth: bool = False, trust_server_certificate: bool = True):
        """
        Инициализация подключения к SQL Server
        """
        self.server = server
        self.database = database
        self.username = username
        self.password = password
        self.driver = driver
        self.use_windows_auth = use_windows_auth
        self.trust_server_certificate = trust_server_certificate

        self.connection_string = self._build_connection_string()
        self.engine = self._create_sqlalchemy_engine()

    def _build_connection_string(self) -> str:
        """Построение строки подключения для pyodbc"""
        connection_parts = [
            f'DRIVER={{{self.driver}}};',
            f'SERVER={self.server};',
            f'DATABASE={self.database};'
        ]

        if self.use_windows_auth:
            connection_parts.append('Trusted_Connection=yes;')
        else:
            if self.username and self.password:
                connection_parts.append(f'UID={self.username};')
                connection_parts.append(f'PWD={self.password};')

        connection_parts.extend([
            f'TrustServerCertificate={"yes" if self.trust_server_certificate else "no"};',
            'Encrypt=yes;',
            'Connection Timeout=30;'
        ])

        return ''.join(connection_parts)

    def _create_sqlalchemy_engine(self) -> sa.engine.Engine:
        """Создание SQLAlchemy engine для работы с pandas"""
        if self.use_windows_auth:
            connection_uri = (
                f"mssql+pyodbc://{self.server}/{self.database}"
                f"?driver={urllib.parse.quote_plus(self.driver)}"
                f"&trusted_connection=yes"
                f"&Encrypt=yes"
                f"&TrustServerCertificate={'yes' if self.trust_server_certificate else 'no'}"
            )
        else:
            if self.username and self.password:
                connection_uri = (
                    f"mssql+pyodbc://{self.username}:{self.password}@{self.server}/{self.database}"
                    f"?driver={urllib.parse.quote_plus(self.driver)}"
                    f"&Encrypt=yes"
                    f"&TrustServerCertificate={'yes' if self.trust_server_certificate else 'no'}"
                )
            else:
                raise ValueError("Для SQL аутентификации необходимо указать username и password")

        return sa.create_engine(connection_uri, pool_pre_ping=True)

    @contextmanager
    def get_connection(self) -> pyodbc.Connection:
        """Context manager для получения подключения через pyodbc"""
        connection = None
        try:
            connection = pyodbc.connect(self.connection_string)
            logger.info(f"Успешное подключение к БД {self.database} на сервере {self.server}")
            yield connection
        except pyodbc.Error as e:
            logger.error(f"Ошибка подключения к БД: {e}")
            raise
        finally:
            if connection:
                connection.close()

    def test_connection(self) -> bool:
        """Тестирование подключения к базе данных"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result[0] == 1
        except Exception as e:
            logger.error(f"Тест подключения к БД провален: {e}")
            return False

    def execute_query(self, query: str, params: tuple = None) -> pd.DataFrame:
        """Выполнение SQL запроса и возврат результата в виде DataFrame"""
        try:
            with self.get_connection() as conn:
                df = pd.read_sql(query, conn, params=params)
                logger.info(f"Запрос выполнен. Возвращено {len(df)} строк")
                return df
        except Exception as e:
            logger.error(f"Ошибка выполнения запроса: {e}")
            raise

def create_db_connector_from_config() -> SQLServerConnector:
    """
    Фабрика для создания подключения на основе конфигурации из credentials.py
    """
    try:
        from config.credentials import CurrentConfig, DB_USERNAME, DB_PASSWORD, DB_NAME
        username = DB_USERNAME
        password = DB_PASSWORD
        db_name = DB_NAME


        return SQLServerConnector(
            server=CurrentConfig.DB_SERVER,
            database=db_name,  # Теперь этот атрибут существует
            username=username,
            password=password,
            driver='ODBC Driver 18 for SQL Server'
        )
    except ImportError:
        raise ImportError("Не удалось импортировать конфигурацию из config.credentials")
    except AttributeError as e:
        raise AttributeError(f"Отсутствует необходимый атрибут в конфигурации: {e}")