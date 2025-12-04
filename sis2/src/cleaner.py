#!/usr/bin/env python3
"""
Cleaner: loads data/raw_ddtc.csv -> cleans -> saves data/clean_ddtc.csv
Cleaning steps:
 - strip whitespace
 - remove exact duplicates
 - normalize name case (title)
 - parse dates (to ISO YYYY-MM-DD) for notice_date & corrected_notice_date
 - ensure at least 100 records (warning if not)
"""
import os
from datetime import date, datetime

import pandas as pd

LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
log_file = os.path.join(LOG_DIR, f"{date.today()}_cleaner.log")

def log(msg, level="INFO"):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{level}] {ts} - {msg}"
    print(line)
    with open(log_file, "a", encoding="utf-8") as f:
        f.write(line + "\n")

INPUT = os.path.join("data", "raw_ddtc.csv")
OUTPUT = os.path.join("data", "clean_ddtc.csv")

def parse_date_safe(x):
    try:
        return pd.to_datetime(x, errors="coerce").date()
    except Exception:
        return None

def clean():
    if not os.path.exists(INPUT):
        log(f"Input not found: {INPUT}", level="ERROR")
        raise FileNotFoundError(INPUT)

    df = pd.read_csv(INPUT, dtype=str).fillna("")
    # strip strings
    for c in df.columns:
        df[c] = df[c].astype(str).apply(lambda s: s.strip())

    # normalize names
    if "name" in df.columns:
        df["name"] = df["name"].apply(lambda s: s.title() if s else s)

    # parse dates columns
    for col in ["notice_date", "corrected_notice_date", "date_of_birth"]:
        if col in df.columns:
            df[col] = df[col].replace({"": None}).apply(parse_date_safe)

    # remove duplicates (exact)
    before = len(df)
    df = df.drop_duplicates()
    after = len(df)
    log(f"Removed {before - after} duplicate rows")

    # Replace empty strings with None
    df = df.replace({"": None})

    # Add changedate
    df["changedate"] = date.today()

    os.makedirs("data", exist_ok=True)
    df.to_csv(OUTPUT, index=False)
    log(f"Cleaned data saved to {OUTPUT}. Rows: {len(df)}")

    if len(df) < 100:
        log("WARNING: cleaned dataset has less than 100 records — requirement is 100 minimum.", level="WARNING")
    return OUTPUT

if __name__ == "__main__":
    clean()
