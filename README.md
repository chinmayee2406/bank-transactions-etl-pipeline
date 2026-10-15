# Banking Transactions ETL Pipeline
### PySpark + SQL + Automated Data Quality + Regression Testing
**Target Role:** Bank of America — Data Engineering Campus / Graduate Program  
**Tech Stack:** Python 3.10+, PySpark (Local Mode), SQLite, SQL (Window Functions & CTEs), Pytest / Unittest, Bash  
**Runtime Compatibility:** Local Laptop (macOS/Linux/Windows WSL) & Google Colab (Zero paid cloud dependencies)

---

## 1. Project Overview

In global banking institutions like **Bank of America**, millions of credit, debit, and wire transfer transactions flow daily through payment rails (Zelle, ACH, Wire, POS terminals). Processing this data requires:
1. **Strict Data Integrity & Governance:** Rejection of malformed data without halting pipeline execution (quarantine pattern).
2. **Deterministic Schemas:** Eliminating silent schema drift and expensive inferSchema scans.
3. **Partitioned Cold Storage & Low-Latency Query Stores:** Storing analytical archives in partitioned Parquet format while serving operational queries through indexed SQL relational stores.
4. **Automated Auditability & Data Lineage:** Full accountability through comprehensive Data Quality (DQ) metrics and traceable rejection logs for regulatory compliance (e.g., BCBS 239, SOX).

This repository implements a production-ready, local-first ETL pipeline that ingests **200,000 raw banking transactions**, validates them against 6 rigorous business constraints, segregates clean vs. quarantined records, partitions data by `txn_year_month`, loads clean records into an indexed SQLite relational store, and executes 6 high-value financial analytical queries.

---

## 2. Architecture & Data Flow

### Architecture Diagram (Mermaid)

```mermaid
flowchart TD
    A["Raw Transaction Ingestion<br/>(CSV: 200,000 rows)"] --> B["PySpark / Python Extract<br/>(Explicit StructType Schema)"]
    B --> C["Data Quality & Validation Engine<br/>(validators.py)"]
    
    C -->|Violations Found<br/>(~5% Anomalies)| D["Quarantined Dead-Letter Store<br/>(data/rejected/rejected_transactions.csv)<br/>+ reject_reason Column"]
    C -->|Passes All Checks<br/>(~95% Clean)| E["Clean Transaction Records<br/>(data/clean/)"]
    
    C --> F["Data Quality Audit Report<br/>(reports/data_quality_report.json & Console)"]
    
    E --> G["Cold Storage Archive<br/>(Partitioned Parquet by txn_year_month)"]
    E --> H["Indexed Relational Store<br/>(SQLite: db/banking.db<br/>Composite Index: account_id + txn_date)"]
    
    H --> I["Financial Analytics Engine<br/>(sql/analytics.sql via run_sql.py)"]
    I --> J["1. Monthly Throughput & Volume"]
    I --> K["2. Top 5 Spend Categories"]
    I --> L["3. Running Balance (Window Func)"]
    I --> M["4. Top 3 Txns per Customer (ROW_NUMBER)"]
    I --> N["5. Anomaly Detection (>3x Account Avg)"]
    I --> O["6. Daily Active Accounts (DAA)"]
```

### ASCII Architecture

```text
+-----------------------------------------------------------------------------------+
|                           STEP 0: RAW DATA INGESTION                              |
|   data/raw/bank_transactions_raw.csv (200k rows, 5% deliberate anomaly injection) |
+-----------------------------------------+-----------------------------------------+
                                          |
                                          v
+-----------------------------------------------------------------------------------+
|                    STEP 1 & 2: EXTRACT & TRANSFORM / VALIDATE                     |
|   - PySpark StructType Explicit Schema (Zero inferSchema latency)                 |
|   - Pure Function Rule Evaluators (src/validators.py)                             |
|   - Multi-Violation Reason Tagging (e.g., NOT_NULL;NEGATIVE_AMOUNT)               |
+--------------------+---------------------------------------------+----------------+
                     |                                             |
          [Failed Validations]                            [Passed Validations]
                     |                                             |
                     v                                             v
+--------------------------------------+      +-------------------------------------+
|        DEAD-LETTER QUARANTINE        |      |          STEP 3: LOAD               |
| data/rejected/rejected_txns.csv      |      | 1. Parquet: data/clean/             |
| (with concatenated reject_reason)    |      |    (Partitioned by txn_year_month)  |
|                                      |      | 2. SQLite: db/banking.db            |
| DQ Audit Report:                     |      |    (Table: transactions,            |
| reports/data_quality_report.json     |      |     Index: account_id + txn_date)   |
+--------------------------------------+      +------------------+------------------+
                                                                 |
                                                                 v
+-----------------------------------------------------------------------------------+
|                             STEP 4: SQL ANALYTICS                                 |
|   sql/analytics.sql executed via src/run_sql.py                                   |
|   1. Monthly Volume & Total Amount                                                |
|   2. Top 5 Merchant Categories by Debit Spend                                     |
|   3. Running Balance per Account (Window: SUM over unbounded preceding)           |
|   4. Top 3 Largest Transactions per Customer (ROW_NUMBER() OVER partition)        |
|   5. Fraud / Anomaly Outliers (Amount > 3x Account Baseline Average)              |
|   6. Daily Active Accounts (DAA) & Liquidity Volume                               |
+-----------------------------------------------------------------------------------+
```

---

## 3. Repository Folder Structure

```text
bank_transactions_etl/
├── data/
│   ├── raw/
│   │   └── bank_transactions_raw.csv    # 200,000 synthetic banking records (generated)
│   ├── clean/                           # Partitioned Parquet files (partitioned by txn_year_month)
│   └── rejected/
│       └── rejected_transactions.csv    # Quarantined bad records with reject_reason column
├── db/
│   └── banking.db                       # Indexed SQLite relational database for SQL analytics
├── logs/
│   └── pipeline.log                     # Timestamped pipeline execution logs
├── reports/
│   └── data_quality_report.json         # Automated JSON audit metrics & violation breakdown
├── sql/
│   └── analytics.sql                    # 6 production-grade analytical SQL queries
├── src/
│   ├── __init__.py
│   ├── generate_data.py                 # Step 0: Generates 200k synthetic bank transactions
│   ├── validators.py                    # Step 2: Pure validation rules + PySpark validation logic
│   ├── etl_pipeline.py                  # Steps 1-3: Extract, Transform/Validate & Load
│   └── run_sql.py                       # Step 4: Executes analytics.sql and outputs formatted tables
├── tests/
│   ├── __init__.py
│   ├── conftest.py                      # Test fixtures (sample rows and 10-row fixture file)
│   ├── test_validators.py               # Step 5: Unit tests for each validator (11 tests, zero Spark)
│   └── test_pipeline_e2e.py             # Step 5: End-to-end regression test on 10-row fixture
├── run_pipeline.sh                      # Step 6: Master bash orchestrator with error-trapping & crontab
├── requirements.txt                     # Pipeline dependencies
└── README.md                            # Step 7: Architecture, documentation, and interview defense
```

---

## 4. How to Run

### Option A: Local Laptop (macOS, Linux, WSL)

1. **Clone or navigate to the repository:**
   ```bash
   cd bank_transactions_etl
   ```

2. **Install Dependencies (Python 3.10+):**
   ```bash
   pip install -r requirements.txt
   ```
   *(Note: The pipeline features an automated zero-dependency fallback engine that executes seamlessly even on minimal Python standard library environments).*

3. **Execute Master Pipeline Orchestrator:**
   ```bash
   chmod +x run_pipeline.sh
   ./run_pipeline.sh
   ```

4. **Run Individual Steps Manually (if preferred):**
   ```bash
   # Step 0: Generate 200k synthetic dataset with 5% bad rows
   python3 src/generate_data.py

   # Steps 1-3: Run ETL (Extract, Validate & Quarantine, Partitioned Parquet & SQLite Load)
   python3 src/etl_pipeline.py

   # Step 4: Execute SQL Analytics
   python3 src/run_sql.py

   # Step 5: Run Automated Tests
   pytest tests/ -v
   # OR via unittest:
   python3 -m unittest discover tests/ -v
   ```

---

### Option B: Google Colab (One-Click Setup)

Copy and run this single cell in a fresh Google Colab notebook:

```python
# 1. Install PySpark
!pip install pyspark -q

# 2. Clone repository or upload repository folder
# (If running directly from scratch):
import os, sys

# 3. Execute Pipeline Orchestrator
!chmod +x run_pipeline.sh
!./run_pipeline.sh
```

---

## 5. Sample Data Quality Report Output

The pipeline automatically compiles audit metrics into `reports/data_quality_report.json` and renders this terminal summary:

```text
============================================================
           DATA QUALITY AUDIT REPORT (SUMMARY)
============================================================
 Total Records Processed : 200,000
 Clean Records Accepted  : 189,957
 Rejected Records        : 10,043
 Overall Pass Rate       : 94.98%
------------------------------------------------------------
 RULE VIOLATION BREAKDOWN:
------------------------------------------------------------
  * NOT_NULL_VIOLATION              :    2,050 ( 1.03%)
  * DUPLICATE_TRANSACTION_ID        :    2,084 ( 1.04%)
  * NEGATIVE_OR_ZERO_AMOUNT         :    1,952 ( 0.98%)
  * INVALID_OR_OUT_OF_RANGE_DATE    :    1,980 ( 0.99%)
  * UNRECOGNIZED_TXN_TYPE           :    1,018 ( 0.51%)
  * MALFORMED_CURRENCY_CODE         :      959 ( 0.48%)
============================================================
```

### Quarantined Row Example (`data/rejected/rejected_transactions.csv`):
```csv
transaction_id,account_id,customer_id,txn_date,txn_type,amount,currency,merchant_category,branch_city,status,reject_reason
TXN_10000006,,CUST_104192,2024-05-01,DEBIT,-45.00,USD,TRAVEL,Chicago,COMPLETED,NOT_NULL_VIOLATION(account_id);NEGATIVE_OR_ZERO_AMOUNT
TXN_10000008,ACC_210442,CUST_108392,2024-07-04,CRYPTO_SWAP,100.00,USDD,OTHER,Seattle,COMPLETED,UNRECOGNIZED_TXN_TYPE;MALFORMED_CURRENCY_CODE
TXN_10000001,ACC_219324,CUST_102049,2024-09-01,DEBIT,35.00,USD,ENTERTAINMENT,Phoenix,COMPLETED,DUPLICATE_TRANSACTION_ID
```

---

## 6. Sample SQL Query Results

Below are excerpts of the results printed by `python3 src/run_sql.py`:

### Query 2: Top 5 Merchant Categories by Debit Spend
```text
+-------------------+-------------------+-----------------+---------------+
| merchant_category | transaction_count | total_spend_usd | avg_spend_usd |
+-------------------+-------------------+-----------------+---------------+
| TRAVEL            |             12601 |      2833126.21 |        224.83 |
| ENTERTAINMENT     |             12501 |       2831638.3 |        226.51 |
| GROCERY           |             12482 |      2816901.16 |        225.68 |
| ATM_WITHDRAWAL    |             12378 |      2793374.34 |        225.67 |
| INVESTMENT        |             12281 |      2790265.04 |         227.2 |
+-------------------+-------------------+-----------------+---------------+
```

### Query 3: Running Balance per Account (Window Function)
```text
+----------------+------------+------------+----------+---------+-----------------+
| transaction_id | account_id | txn_date   | txn_type | amount  | running_balance |
+----------------+------------+------------+----------+---------+-----------------+
| TXN_10191081   | ACC_200154 | 2024-01-04 | CREDIT   | 4558.98 |         4558.98 |
| TXN_10123682   | ACC_200154 | 2024-01-05 | TRANSFER |  1990.9 |         2568.08 |
| TXN_10111506   | ACC_200154 | 2024-01-20 | DEBIT    |    71.4 |         2496.68 |
| TXN_10195357   | ACC_200154 | 2024-01-20 | DEBIT    |  254.76 |         2241.92 |
| TXN_10103646   | ACC_200154 | 2024-02-16 | CREDIT   |  299.68 |          2541.6 |
| TXN_10069434   | ACC_200154 | 2024-04-12 | CREDIT   | 2750.13 |         5291.73 |
+----------------+------------+------------+----------+---------+-----------------+
```

### Query 5: Anomaly Outliers (> 3x Account Baseline Average)
```text
+------------+----------------+------------+----------+-------------------+--------------------+----------------------+---------------------+
| account_id | transaction_id | txn_date   | txn_type | merchant_category | transaction_amount | account_baseline_avg | multiplier_over_avg |
+------------+----------------+------------+----------+-------------------+--------------------+----------------------+---------------------+
| ACC_220219 | TXN_10117153   | 2024-10-07 | CREDIT   | GROCERY           |            4005.41 |               415.04 |                9.65 |
| ACC_223234 | TXN_10004199   | 2024-11-07 | CREDIT   | HEALTHCARE        |            4730.09 |               494.97 |                9.56 |
| ACC_224574 | TXN_10119624   | 2024-08-04 | CREDIT   | INVESTMENT        |            4305.02 |               464.88 |                9.26 |
| ACC_203724 | TXN_10142040   | 2024-01-09 | CREDIT   | RETAIL            |            4164.25 |               452.03 |                9.21 |
+------------+----------------+------------+----------+-------------------+--------------------+----------------------+---------------------+
```

---

## 7. Data Lineage

```text
[src/generate_data.py]
         │
         ▼
[data/raw/bank_transactions_raw.csv]
         │
         ├── (Reads explicit StructType schema) ──► [src/etl_pipeline.py]
         │                                                   │
         │                        ┌──────────────────────────┴──────────────────────────┐
         │                        ▼                                                     ▼
         │               [Clean Records: ~95%]                                [Quarantined: ~5%]
         │                        │                                                     │
         │         ┌──────────────┴──────────────┐                                      ▼
         │         ▼                             ▼                    [data/rejected/rejected_transactions.csv]
         │  [data/clean/parquet]         [db/banking.db]                                │
         │  (Partitioned by YYYY-MM)     (Indexed transactions table)                   ▼
         │                                       │                        [reports/data_quality_report.json]
         │                                       ▼
         └──────────────────────────────► [src/run_sql.py]
                                                 │
                                                 ▼
                                        [Analytics Outputs]
```

---

## 8. Design Decisions & Engineering Tradeoffs

### 1. Explicit Schema vs. `inferSchema=True`
* **Performance:** `inferSchema` forces PySpark to perform a full additional pass over the raw CSV dataset before starting execution. On a multi-gigabyte or terabyte dataset at Bank of America, this doubles ingestion I/O.
* **Predictability & Governance:** In financial data, a missing decimal point or unexpected string in a numeric field could cause Spark to infer a column as `StringType` instead of `DoubleType`. An explicit schema enforces data contracts at the ingestion boundary.

### 2. Partitioned Parquet Storage
* **Columnar Format:** Unlike row-based CSVs, Parquet stores data column-by-column, allowing analytical queries (e.g., calculating average spend across 50 million rows) to read only the `amount` column, eliminating 80%+ of disk I/O.
* **Compression & Snappy:** Parquet achieves ~75% compression ratios over raw CSV using dictionary encoding and Snappy compression.
* **Partition Pruning:** Partitioning by `txn_year_month` enables query engines to skip scanning unrelated directory partitions when filtering for specific months (`WHERE txn_date BETWEEN '2024-06-01' AND '2024-06-30'`).

### 3. The Dead-Letter / Quarantined Rejected-Rows Store
* In banking, pipelines cannot simply call `.dropna()` or drop invalid transactions silently. Regulatory standards (BCBS 239, Sarbanes-Oxley) mandate that every transaction ingested must be accounted for.
* Rejected records are quarantined into a dedicated store with explicit reason tags (`reject_reason`). This allows data operations teams to audit issues, correct source system anomalies, and replay/backfill records without re-running the entire ingestion job.

### 4. Decoupled, Pure Validation Functions
* Validation functions in `src/validators.py` are written as pure Python functions with zero dependencies on Spark context or JVM runtimes.
* This allows the engineering team to run automated unit tests with `pytest` in **0.001 seconds**, dramatically speeding up CI/CD pipelines.
* For distributed execution, the exact same rules are expressed via PySpark native column expressions (`F.when`, `F.array_compact`).

---

## 9. Orchestration & Production Scheduling

The pipeline includes an automated shell runner with error trapping (`set -euo pipefail`):
```bash
./run_pipeline.sh
```

### Production Crontab Schedule
To execute the pipeline daily at **02:00 AM Eastern Time** (off-peak batch reconciliation window):
```cron
# Open crontab editor
crontab -e

# Daily execution at 2 AM
0 2 * * * cd /Users/username/bank_transactions_etl && ./run_pipeline.sh >> logs/cron.log 2>&1
```
