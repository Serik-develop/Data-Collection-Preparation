import pandas as pd
import sqlite3
from datetime import datetime, timedelta
from src.db_utils import get_db_connection, create_daily_summary_table

def daily_summary():
    
    create_daily_summary_table()

    conn = get_db_connection()

    yesterday = (datetime.utcnow() - timedelta(days=1)).date()

    df = pd.read_sql("SELECT * FROM events", conn)
    if df.empty:
        print("No events data")
        conn.close()
        return

    df['period'] = pd.to_datetime(df['period']).dt.date
    df = df[df['period'] == yesterday]

    if df.empty:
        print(f"No events for {yesterday}")
        conn.close()
        return

    
    summary = df.groupby('region').agg(
        min_value=('value', 'min'),
        max_value=('value', 'max'),
        avg_value=('value', 'mean'),
        count=('value', 'count')
    ).reset_index()

    summary['date'] = yesterday

    summary.to_sql('daily_summary', conn, if_exists='append', index=False)
    conn.close()
    print(f"{len(summary)} summary rows inserted for {yesterday}")
