# BLedgerGuard | Banking Transactions ETL Pipeline

A batch data engineering pipeline that ingests raw banking transactions, validates every record against business rules, quarantines bad data with reason codes instead of silently dropping it, and loads clean data into partitioned Parquet and an indexed SQLite database for SQL analytics.

Stack: Python · PySpark · SQL · SQLite · Parquet · Pytest · Shell
