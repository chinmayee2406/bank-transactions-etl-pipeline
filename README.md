# Banking Transactions ETL Pipeline

A production-grade retail banking ETL pipeline built with **PySpark**, **SQL**, and **automated Data Quality validation**. Demonstrates strict schema enforcement, dead-letter quarantining, partitioned Parquet lakehouse storage, and financial SQL analytics.

---

## Architecture Flow

```text
[Raw Ingestion]        CSV (200k records, ~5% injected anomalies)
       │
       ▼
[ETL & Validation]    PySpark StructType Schema (Zero inferSchema)
       │              Pure Python Validator Functions (validators.py)
       ├─── Pass ──►  Partitioned Parquet (txn_year_month) + SQLite (banking.db)
       └─── Fail ──►  Quarantine Dead-Letter Store (rejected_transactions.csv)
       │
       ▼
[Serving Layer]       6 Analytical SQL Queries (Windowing, CTEs, Outlier Detection)
