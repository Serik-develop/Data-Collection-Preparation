#!/usr/bin/env python3
"""
Loader: reads data/clean_ddtc.csv and loads into SQLite3 database data/output.db
Creates table statutorily_debarred_parties if not exists.
"""
import os
import sqlite3
from datetime import datetime

import pandas as pd

DB_PATH = os.path.join("data", "output.db")
CSV_PATH = os.path.join("data", "clean_ddtc.csv")
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, f"{datetime.now().date()}_loader.log")

def log(msg, level="INFO"):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{level}] {ts} - {msg}"
    print(line)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(line + "\n")

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS statutorily_debarred_parties (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    date_of_birth DATE,
    federal_register_notice TEXT,
    notice_date DATE,
    corrected_notice TEXT,
    corrected_notice_date DATE,
    changedate DATE
);
"""

def load(overwrite: bool = True):
    if not os.path.exists(CSV_PATH):
        log(f"Clean CSV not found: {CSV_PATH}", level="ERROR")
        raise FileNotFoundError(CSV_PATH)

    df = pd.read_csv(CSV_PATH, dtype=str)

    # convert date-like columns to ISO strings or NULL
    for col in ["date_of_birth", "notice_date", "corrected_notice_date"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date

    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute(SCHEMA_SQL)
    conn.commit()

    if overwrite:
        cur.execute("DELETE FROM statutorily_debarred_parties;")
        conn.commit()
        log("Table truncated (deleted existing rows).")

    # Insert rows
    insert_sql = """
    INSERT INTO statutorily_debarred_parties
    (name, date_of_birth, federal_register_notice, notice_date, corrected_notice, corrected_notice_date, changedate)
    VALUES (?, ?, ?, ?, ?, ?, ?);
    """
    success = 0
    for _, row in df.iterrows():
        vals = (
            row.get("name"),
            row.get("date_of_birth") if pd.notna(row.get("date_of_birth")) else None,
            row.get("federal_register_notice"),
            row.get("notice_date") if pd.notna(row.get("notice_date")) else None,
            row.get("corrected_notice"),
            row.get("corrected_notice_date") if pd.notna(row.get("corrected_notice_date")) else None,
            row.get("changedate")
        )
        try:
            cur.execute(insert_sql, vals)
            success += 1
        except Exception as e:
            log(f"Insert error: {e}", level="ERROR")
            continue

    conn.commit()
    conn.close()
    log(f"Loaded {success} rows into {DB_PATH}")
    return DB_PATH

if __name__ == "__main__":
    load()
