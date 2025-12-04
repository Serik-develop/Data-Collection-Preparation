# Data Collection & Preparation — DDTC loader

Project implements full mini-pipeline: scraping (Selenium) > cleaning > load (SQLite) > orchestration (Airflow).

**Deadline:** commit to GitHub by Dec 4, 2025 23:59:59 (local time).  
**Oral defense:** Dec 5, 2025.

## Structure
project/
¦ README.md
¦ requirements.txt
¦ airflow_dag.py
+-- src/
¦ +-- scraper.py
¦ +-- cleaner.py
¦ L-- loader.py
L-- data/
  L output.db

## Quick start (local, dev)
1. Create virtualenv and install:

2. Run scraper (dev):
результат: `data/raw_ddtc.csv`

3. Очистка:
результат: `data/clean_ddtc.csv`

4. Загрузка в SQLite:
результат: `data/output.db` (таблица `statutorily_debarred_parties`)

## Airflow
1. Убедитесь, что Airflow установлен (см. requirements).
2. Поместите `airflow_dag.py` в папку `AIRFLOW_HOME/dags`.
3. Инициализируйте Airflow DB, запустите вебсервер и шедулер.
4. DAG `ddtc_pipeline` настроен на `@daily`. Он выполняет scraping > clean > load, с логами и retry=2.

## Замечания
- Убедитесь, что сайт, выбранный вами, не использовался другими парами (требование курса).
- Для выполнения Selenium локально потребуется Chrome и соответствующий драйвер; `webdriver-manager` автоматически поставит драйвер.
