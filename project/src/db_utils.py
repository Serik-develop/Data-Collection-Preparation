import sqlite3
import os
from datetime import datetime


SRC_DIR = os.path.dirname(os.path.abspath(__file__))           
BASE_DIR = os.path.abspath(os.path.join(SRC_DIR, ".."))        
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)              
DB_PATH = os.path.join(DATA_DIR, "app.db")


def get_db_connection():
    return sqlite3.connect(DB_PATH)


def create_events_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS events (
            period TEXT,
            region TEXT,
            value INTEGER,
            ingestion_time TEXT
        )
    """)
    conn.commit()
    conn.close()


def create_daily_summary_table():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS daily_summary (
            date TEXT,
            region TEXT,
            min_value REAL,
            max_value REAL,
            avg_value REAL,
            count INTEGER
        )
    """)
    conn.commit()
    conn.close()
