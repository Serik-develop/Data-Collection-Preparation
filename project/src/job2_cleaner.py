import json
import pandas as pd
import sqlite3
from datetime import datetime
from kafka import KafkaConsumer

def clean_and_store():
    

    create_events_table()

    consumer = KafkaConsumer(
        'raw_events',
        bootstrap_servers='localhost:9092',
        auto_offset_reset='earliest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')),
        consumer_timeout_ms=10000
    )

    records = [msg.value for msg in consumer]
    consumer.close()

    if not records:
        print("No new messages in Kafka")
        return

    df = pd.DataFrame(records)

   
    df = df[df['type'] == 'D'] 
    df['value'] = df['value'].astype(int)
    df['period'] = pd.to_datetime(df['period'])
    df['region'] = df['respondent']
    df['ingestion_time'] = datetime.utcnow().isoformat()

    df = df[['period', 'region', 'value', 'ingestion_time']]

    conn = get_db_connection()
    df.to_sql('events', conn, if_exists='append', index=False)
    conn.close()
    print(f"{len(df)} records inserted into events")
