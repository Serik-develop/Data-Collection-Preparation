from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from src.job1_producer import fetch_and_send

default_args = {
    'owner': 'team',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

dag = DAG(
    'job1_ingestion',
    default_args=default_args,
    description='Continuous ingestion: API -> Kafka',
    schedule_interval='*/5 * * * *',  
    catchup=False
)

task = PythonOperator(
    task_id='fetch_and_send_to_kafka',
    python_callable=fetch_and_send,
    dag=dag
)
