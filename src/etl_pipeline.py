"""
etl_pipeline.py - Steps 1, 2, 3: Extract, Transform/Validate, Load
==================================================================
Production-grade ETL pipeline for retail banking transactions.
Built for Bank of America Data Engineering standards:
    - Step 1 (Extract)  : Explicit StructType schema (avoids inferSchema performance hit)
    - Step 2 (Transform): Pure validation rules, quarantine rejected rows, DQ report
    - Step 3 (Load)     : Clean partitioned Parquet (year-month), rejected CSV, SQLite DB
"""

import os
import sys
import time
import json
import sqlite3
import logging
from datetime import datetime

# Add project root to sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.validators import (
    validate_records_pure_python,
    format_dq_report_table,
    save_dq_report_json
)

# Setup Logging
LOG_FILE = os.path.join(PROJECT_ROOT, "logs", "pipeline.log")
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

logger = logging.getLogger("etl_pipeline")
logger.setLevel(logging.INFO)

# Formatter with precise timestamp
formatter = logging.Formatter(
    fmt="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# File handler
fh = logging.FileHandler(LOG_FILE, mode="a", encoding="utf-8")
fh.setFormatter(formatter)
logger.addHandler(fh)

# Console handler
ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(formatter)
logger.addHandler(ch)


# Paths configuration
RAW_DATA_PATH = os.path.join(PROJECT_ROOT, "data", "raw", "bank_transactions_raw.csv")
CLEAN_PARQUET_PATH = os.path.join(PROJECT_ROOT, "data", "clean", "transactions_parquet")
REJECTED_CSV_PATH = os.path.join(PROJECT_ROOT, "data", "rejected", "rejected_transactions.csv")
SQLITE_DB_PATH = os.path.join(PROJECT_ROOT, "db", "banking.db")
DQ_REPORT_PATH = os.path.join(PROJECT_ROOT, "reports", "data_quality_report.json")


def get_spark_session(app_name: str = "BofA_Banking_Transactions_ETL"):
    """
    Initializes a local PySpark session with optimized memory and partition configs.
    """
    try:
        from pyspark.sql import SparkSession
        spark = (
            SparkSession.builder
            .appName(app_name)
            .master("local[*]")
            .config("spark.sql.shuffle.partitions", "4")
            .config("spark.driver.memory", "2g")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        spark.sparkContext.setLogLevel("ERROR")
        return spark
    except ImportError:
        return None


def get_explicit_spark_schema():
    """
    Explicit PySpark schema definition.
    Crucial Interview Point: We NEVER use inferSchema=True in production because:
      1. inferSchema requires a full 2-pass scan of the dataset before job execution.
      2. Type mismatches or partial columns silently degrade data integrity.
      3. It violates enterprise data contracts and schema governance.
    """
    from pyspark.sql.types import (
        StructType, StructField, StringType, DoubleType
    )
    return StructType([
        StructField("transaction_id", StringType(), nullable=True),
        StructField("account_id", StringType(), nullable=True),
        StructField("customer_id", StringType(), nullable=True),
        StructField("txn_date", StringType(), nullable=True),
        StructField("txn_type", StringType(), nullable=True),
        StructField("amount", DoubleType(), nullable=True),
        StructField("currency", StringType(), nullable=True),
        StructField("merchant_category", StringType(), nullable=True),
        StructField("branch_city", StringType(), nullable=True),
        StructField("status", StringType(), nullable=True)
    ])


def init_sqlite_database(db_path: str):
    """
    Initializes the SQLite database schema with types and indexing.
    Enforces idempotency using CREATE TABLE IF NOT EXISTS.
    """
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # Create transactions table
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS transactions (
        transaction_id VARCHAR(32) PRIMARY KEY,
        account_id VARCHAR(32) NOT NULL,
        customer_id VARCHAR(32) NOT NULL,
        txn_date DATE NOT NULL,
        txn_type VARCHAR(16) NOT NULL,
        amount DECIMAL(12, 2) NOT NULL,
        currency VARCHAR(3) NOT NULL,
        merchant_category VARCHAR(64),
        branch_city VARCHAR(64),
        status VARCHAR(16),
        txn_year_month VARCHAR(7),
        loaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """)

    # Create composite index on (account_id, txn_date) for high-performance windowing and account lookup
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_transactions_account_date
    ON transactions (account_id, txn_date);
    """)

    # Create index on txn_date for time-series filtering
    cursor.execute("""
    CREATE INDEX IF NOT EXISTS idx_transactions_date
    ON transactions (txn_date);
    """)

    conn.commit()
    conn.close()
    logger.info(f"Initialized SQLite database schema and indexes at {db_path}")


def load_clean_data_to_sqlite(clean_records: list, db_path: str):
    """
    Loads clean records into SQLite database using batch parameter binding.
    Uses INSERT OR REPLACE to ensure idempotent loads.
    """
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    insert_sql = """
    INSERT OR REPLACE INTO transactions (
        transaction_id, account_id, customer_id, txn_date, txn_type,
        amount, currency, merchant_category, branch_city, status, txn_year_month
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
    """

    batch_data = []
    for r in clean_records:
        txn_date = str(r["txn_date"])
        year_month = txn_date[:7] if len(txn_date) >= 7 else "UNKNOWN"
        batch_data.append((
            r["transaction_id"],
            r["account_id"],
            r["customer_id"],
            txn_date,
            r["txn_type"],
            float(r["amount"]),
            r["currency"],
            r.get("merchant_category", "UNKNOWN"),
            r.get("branch_city", "UNKNOWN"),
            r.get("status", "COMPLETED"),
            year_month
        ))

    # Execute in chunks of 10,000 to manage SQLite transaction memory
    batch_size = 10_000
    for i in range(0, len(batch_data), batch_size):
        chunk = batch_data[i:i + batch_size]
        cursor.executemany(insert_sql, chunk)
        conn.commit()

    conn.close()
    logger.info(f"Loaded {len(batch_data):,} clean records into SQLite table 'transactions'.")


# ==============================================================================
# SPARK-BASED ETL EXECUTION
# ==============================================================================

def run_spark_pipeline(spark, raw_csv: str) -> dict:
    """
    Executes ETL using PySpark.
    """
    from pyspark.sql import functions as F
    from src.validators import validate_spark_dataframe

    # STEP 1: EXTRACT
    logger.info(f"[STAGE 1/3: EXTRACT] Reading raw CSV from {raw_csv} with EXPLICIT SCHEMA...")
    schema = get_explicit_spark_schema()
    raw_df = (
        spark.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(schema)
        .csv(raw_csv)
    )

    total_extracted_count = raw_df.count()
    logger.info(f"[STAGE 1/3: EXTRACT] Successfully extracted {total_extracted_count:,} raw rows.")

    # STEP 2: TRANSFORM & VALIDATE
    logger.info("[STAGE 2/3: TRANSFORM] Validating records and segregating clean vs rejected...")
    clean_df, rejected_df, dq_report = validate_spark_dataframe(raw_df)

    # Add txn_year_month partition column to clean_df
    clean_df = clean_df.withColumn(
        "txn_year_month",
        F.substring(F.col("txn_date"), 1, 7)
    )

    # Print and Save Data Quality Audit Report
    report_table = format_dq_report_table(dq_report)
    print("\n" + report_table + "\n")
    logger.info("\n" + report_table)
    save_dq_report_json(dq_report, DQ_REPORT_PATH)

    # STEP 3: LOAD
    logger.info("[STAGE 3/3: LOAD] Loading clean data to partitioned Parquet...")
    (
        clean_df.write
        .mode("overwrite")
        .partitionBy("txn_year_month")
        .parquet(CLEAN_PARQUET_PATH)
    )
    logger.info(f"Clean data stored in partitioned Parquet format at: {CLEAN_PARQUET_PATH}")

    logger.info("[STAGE 3/3: LOAD] Writing quarantined rejected records to CSV...")
    (
        rejected_df.write
        .mode("overwrite")
        .option("header", "true")
        .csv(REJECTED_CSV_PATH)
    )
    logger.info(f"Rejected records stored with reasons at: {REJECTED_CSV_PATH}")

    # Load clean data into SQLite
    logger.info("[STAGE 3/3: LOAD] Loading clean data into SQLite database...")
    init_sqlite_database(SQLITE_DB_PATH)
    clean_rows_collected = [row.asDict() for row in clean_df.collect()]
    load_clean_data_to_sqlite(clean_rows_collected, SQLITE_DB_PATH)

    return dq_report


# ==============================================================================
# PURE PYTHON ETL EXECUTION (Zero-dependency fallback for laptop / test envs)
# ==============================================================================

def run_python_fallback_pipeline(raw_csv: str) -> dict:
    """
    Executes ETL using pure Python standard library.
    Guarantees that the student can run the pipeline anywhere with zero setup issues.
    """
    import csv

    # STEP 1: EXTRACT
    logger.info(f"[STAGE 1/3: EXTRACT] Reading raw CSV from {raw_csv} (Explicit Type Parsing)...")
    records = []
    with open(raw_csv, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            records.append(row)

    total_extracted_count = len(records)
    logger.info(f"[STAGE 1/3: EXTRACT] Successfully extracted {total_extracted_count:,} raw rows.")

    # STEP 2: TRANSFORM & VALIDATE
    logger.info("[STAGE 2/3: TRANSFORM] Validating records and segregating clean vs rejected...")
    clean_records, rejected_records, dq_report = validate_records_pure_python(records)

    # Print and Save Data Quality Audit Report
    report_table = format_dq_report_table(dq_report)
    print("\n" + report_table + "\n")
    logger.info("\n" + report_table)
    save_dq_report_json(dq_report, DQ_REPORT_PATH)

    # STEP 3: LOAD
    logger.info("[STAGE 3/3: LOAD] Saving rejected records to CSV...")
    os.makedirs(os.path.dirname(REJECTED_CSV_PATH), exist_ok=True)
    if rejected_records:
        rejected_keys = list(rejected_records[0].keys())
        with open(REJECTED_CSV_PATH, mode="w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=rejected_keys)
            writer.writeheader()
            writer.writerows(rejected_records)
    logger.info(f"Rejected records stored with reasons at: {REJECTED_CSV_PATH}")

    logger.info("[STAGE 3/3: LOAD] Partitioning clean data and loading to SQLite...")
    init_sqlite_database(SQLITE_DB_PATH)
    load_clean_data_to_sqlite(clean_records, SQLITE_DB_PATH)

    # Partitioned storage demonstration
    # Group clean records by year-month and store partitioned CSV/JSON
    partition_base = os.path.join(PROJECT_ROOT, "data", "clean", "transactions_partitioned")
    os.makedirs(partition_base, exist_ok=True)
    partitions = {}
    for r in clean_records:
        ym = str(r["txn_date"])[:7] if len(str(r["txn_date"])) >= 7 else "UNKNOWN"
        partitions.setdefault(ym, []).append(r)

    for ym, part_records in partitions.items():
        part_dir = os.path.join(partition_base, f"txn_year_month={ym}")
        os.makedirs(part_dir, exist_ok=True)
        part_file = os.path.join(part_dir, "data.csv")
        with open(part_file, mode="w", newline="", encoding="utf-8") as pf:
            writer = csv.DictWriter(pf, fieldnames=list(part_records[0].keys()))
            writer.writeheader()
            writer.writerows(part_records)

    logger.info(f"Partitioned clean records saved across {len(partitions)} year-month partitions.")
    return dq_report


def main():
    start_time = time.time()
    logger.info("=" * 60)
    logger.info("STARTING BANK OF AMERICA TRANSACTIONS ETL PIPELINE")
    logger.info("=" * 60)

    if not os.path.exists(RAW_DATA_PATH):
        logger.error(f"Raw data file not found at {RAW_DATA_PATH}. Run generate_data.py first!")
        sys.exit(1)

    spark = get_spark_session()
    if spark:
        logger.info("Running pipeline with Apache PySpark execution engine.")
        try:
            report = run_spark_pipeline(spark, RAW_DATA_PATH)
            spark.stop()
        except Exception as e:
            logger.error(f"Spark execution failed: {e}. Falling back to Python engine.", exc_info=True)
            report = run_python_fallback_pipeline(RAW_DATA_PATH)
    else:
        logger.info("PySpark not detected or Java unavailable. Executing high-performance Python engine.")
        report = run_python_fallback_pipeline(RAW_DATA_PATH)

    elapsed = time.time() - start_time
    logger.info(f"Pipeline completed successfully in {elapsed:.2f} seconds.")


if __name__ == "__main__":
    main()
