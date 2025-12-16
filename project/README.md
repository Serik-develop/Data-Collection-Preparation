# Final Project: Streaming + Batch Data Pipeline

## Team Members
- Bakhramov Serik
- Birlikzhanova Aruzhan
- Sadykova Yasmin

## Project Description
This project demonstrates a streaming + batch data pipeline using:
- **API (EIA)** > real-time electricity demand data
- **Kafka** > message broker for streaming data
- **Airflow** > scheduling DAGs
- **SQLite** > storage for cleaned and aggregated data

## Architecture
1. **DAG 1 (job1_ingestion)**  
   Continuous ingestion from EIA API > Kafka topic `raw_events`

2. **DAG 2 (job2_clean_store)**  
   Hourly batch job: Kafka > cleaning > SQLite `events` table

3. **DAG 3 (job3_daily_summary)**  
   Daily analytics: SQLite `events` > aggregated summary > `daily_summary` table


project/
¦ README.md
¦ requirements.txt
+- src/
¦ +- job1_producer.py
¦ +- job2_cleaner.py
¦ +- job3_analytics.py
¦ L- db_utils.py
+- airflow/dags/
¦ +- job1_ingestion_dag.py
¦ +- job2_clean_store_dag.py
¦ L- job3_daily_summary_dag.py
+- data/
¦ L- app.db
+- report/
¦ L- report.pdf
L- docker-compose.yml


## How to Run

1. Install Python dependencies:
```bash
pip install -r requirements.txt

2. Start Kafka and Zookeeper:

docker-compose up -d

3. Start Airflow (LocalExecutor recommended):

airflow db init
airflow webserver -p 8080
airflow scheduler

4. Check DAGs in Airflow UI (http://localhost:8080)

5. Trigger DAGs manually or wait for scheduled runs.

Notes:

SQLite database is located at data/app.db

Kafka topic: raw_events

All DAGs import functions from src/



