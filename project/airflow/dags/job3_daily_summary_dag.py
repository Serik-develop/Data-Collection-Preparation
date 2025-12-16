from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


from src.job3_analytics import daily_summary

default_args = {
    'owner': 'team',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 15),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'job3_daily_summary',
    default_args=default_args,
    description='Daily analytics job: compute summary from events',
    schedule_interval='@daily',
    catchup=False
)

task = PythonOperator(
    task_id='compute_daily_summary',
    python_callable=daily_summary,
    dag=dag
)
