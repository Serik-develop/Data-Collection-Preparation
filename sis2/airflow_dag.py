"""
Airflow DAG: ddtc_pipeline
Place this file into AIRFLOW_HOME/dags/
DAG schedule: once per day (@daily)
Tasks:
 - scrape_task (PythonOperator) -> runs src.scraper.scrape
 - clean_task  -> src.cleaner.clean
 - load_task   -> src.loader.load
"""
from datetime import datetime, timedelta
import os
import sys

# ensure project src can be imported from DAG context (adjust path as needed)
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__)))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


from airflow.operators.python import PythonOperator
from airflow import DAG






# import functions
# we import via path relative assumption `dags` sits in project root; if not, adjust PYTHONPATH
from src.scraper import scrape
from src.cleaner import clean
from src.loader import load as load_db

default_args = {
    "owner": "student",
    "depends_on_past": False,
    "email": [],
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ddtc_pipeline",
    default_args=default_args,
    start_date=datetime(2025, 12, 1),
    schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["ddtc", "scraping"],
) as dag:

    def _scrape_callable(**kwargs):
        # returns path to raw csv
        url = "https://deccspmddtc.servicenowservices.com/ddtc_public?id=ddtc_kb_article_page&sys_id=7188dac6db3cd30044f9ff621f961914"
        # run headless in scheduler
        raw = scrape(url, headless=True, max_pages=200)
        # push xcom
        return raw

    def _clean_callable(**kwargs):
        cleaned = clean()
        return cleaned

    def _load_callable(**kwargs):
        db = load_db()
        return db

    scrape_task = PythonOperator(
        task_id="scrape_task",
        python_callable=_scrape_callable,
        retries=2,
    )

    clean_task = PythonOperator(
        task_id="clean_task",
        python_callable=_clean_callable,
    )

    load_task = PythonOperator(
        task_id="load_task",
        python_callable=_load_callable,
    )

    scrape_task >> clean_task >> load_task
