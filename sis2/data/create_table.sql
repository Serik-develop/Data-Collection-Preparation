-- create_table.sql
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
