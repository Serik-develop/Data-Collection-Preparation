import sqlite3
import pandas as pd

# Подключаемся к базе
conn = sqlite3.connect("data/output.db")
cur = conn.cursor()

# Посмотреть список таблиц
cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cur.fetchall()
print("Tables:", tables)

# Посмотреть первые 5 строк таблицы (замени на реальное имя таблицы)
df = pd.read_sql_query("SELECT * FROM statutorily_debarred_parties LIMIT 5;", conn)
print(df)

conn.close()
