
```
│   .gitignore
│   main.py
│   README.md
│   requirements.txt
│
├───config
│   │   credentials.py
│   │   credentials.py.template
│   │   settings.py
│   │   __init__.py
│   │
│   └───__pycache__
│           credentials.cpython-313.pyc
│           __init__.cpython-313.pyc
│
├───data
│   ├───output
│   ├───processed
│   └───raw
│           erp.csv
│           expert.csv
│           rooms.csv
│           statuses.csv
│
├───sql
│       extract_rooms.sql
│       extract_rooms.sql.template
│       extract_statuses.sql
│       extract_statuses.sql.template
│
├───src
│   │   __init__.py
│   │
│   ├───api
│   │       api-1c.py
│   │       api-crm.py
│   │       __init__.py
│   │
│   ├───database
│   │   │   db_connector.py
│   │   │   __init__.py
│   │   │
│   │   └───__pycache__
│   │           db_connector.cpython-313.pyc
│   │           __init__.cpython-313.pyc
│   │
│   ├───etl
│   │   │   bi_mart.py
│   │   │   data_processor.py
│   │   │   db_extractor.py
│   │   │   __init__.py
│   │   │
│   │   └───data
│   │       └───raw
│   ├───utils
│   │       csv_loader.py
│   │       helpers.py
│   │       __init__.py
│   │
│   └───__pycache__
│           __init__.cpython-313.pyc
│
└───tests
    │   test_apis.py
    │   test_database_connection.py
    │   test_data_processor.py
    │   __init__.py
    │
    ├───.pytest_cache
    │   │   .gitignore
    │   │   CACHEDIR.TAG
    │   │   README.md
    │   │
    │   └───v
    │       └───cache
    │               lastfailed
    │               nodeids
    │
    └───__pycache__
            test_database_connection.cpython-313-pytest-8.4.2.pyc
            __init__.cpython-313.pyc
```