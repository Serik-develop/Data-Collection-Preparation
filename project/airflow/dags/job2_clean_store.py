from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# импортируем логику из src
from src.job2_cleaner import clean_and_store

default_args = {
    'owner': 'team',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'job2_clean_store',
    default_args=default_args,
    description='Hourly cleaning job: read Kafka -> clean -> SQLite',
    schedule_interval='@hourly',
    catchup=False
)

task = PythonOperator(
    task_id='clean_kafka_and_store_sqlite',
    python_callable=clean_and_store,
    dag=dag
)
