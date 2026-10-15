"""
generate_data.py - Step 0: Synthetic Banking Transactions Generator
====================================================================
Generates a realistic dataset of 200,000 retail banking transactions
with deliberate anomalies (~5% of records) to simulate production
data ingestion anomalies at an enterprise financial institution like Bank of America.

Schema:
    - transaction_id    : String (e.g., TXN_10000001)
    - account_id        : String (e.g., ACC_200001)
    - customer_id       : String (e.g., CUST_300001)
    - txn_date          : String (YYYY-MM-DD, 2024-01-01 to 2024-12-31)
    - txn_type          : String (DEBIT, CREDIT, TRANSFER)
    - amount            : Decimal/Float (e.g., 125.50)
    - currency          : String (ISO 4217, e.g., USD)
    - merchant_category : String (GROCERY, DINING, UTILITIES, etc.)
    - branch_city       : String (Charlotte, New York, Dallas, etc.)
    - status            : String (COMPLETED, PENDING, FAILED)

Injected Anomalies (~5% total):
    1. Null / Missing account_id (~1%)
    2. Duplicate transaction_id (~1%)
    3. Non-positive amounts: <= 0 or negative values (~1%)
    4. Invalid / Future dates (~1%)
    5. Disallowed transaction types (~0.5%)
    6. Malformed currency codes (~0.5%)

Real Dataset Substitution Note:
    To use a real Kaggle dataset instead of synthetic data:
    1. Download e.g. "Bank Customer Transactions" or "Credit Card Transactions":
       https://www.kaggle.com/datasets
    2. Save as `data/raw/bank_transactions_raw.csv`.
    3. Ensure column names map to the schema expected by the ETL job.
"""

import os
import csv
import random
import logging
from datetime import datetime, timedelta

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("generate_data")

TOTAL_ROWS = 200_000
RANDOM_SEED = 42

# Categorical domain pools
TXN_TYPES = ["DEBIT", "CREDIT", "TRANSFER"]
CURRENCIES = ["USD"]  # Core domestic retail banking currency
MERCHANT_CATEGORIES = [
    "GROCERY",
    "DINING",
    "ENTERTAINMENT",
    "UTILITIES",
    "HEALTHCARE",
    "TRAVEL",
    "RETAIL",
    "SALARY",
    "INVESTMENT",
    "ATM_WITHDRAWAL"
]
BRANCH_CITIES = [
    "Charlotte",      # Bank of America Headquarters
    "New York",       # Financial hub
    "Dallas",         # Technology center
    "Atlanta",        # Regional operations
    "Chicago",
    "San Francisco",
    "Boston",
    "Richmond",
    "Phoenix",
    "Seattle"
]
STATUSES = ["COMPLETED", "PENDING", "FAILED"]

# Account and customer pools (many-to-one relationship)
NUM_CUSTOMERS = 15_000
NUM_ACCOUNTS = 25_000

START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 12, 31)
DATE_RANGE_DAYS = (END_DATE - START_DATE).days


def generate_synthetic_transactions(output_path: str, total_rows: int = TOTAL_ROWS) -> dict:
    """
    Generates synthetic bank transactions with deliberate bad rows and writes to CSV.
    """
    random.seed(RANDOM_SEED)

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    logger.info(f"Generating {total_rows:,} synthetic transactions into {output_path}...")

    # Pre-generate customer and account IDs for realistic relationships
    customer_ids = [f"CUST_{100_000 + i}" for i in range(NUM_CUSTOMERS)]
    account_to_customer = {
        f"ACC_{200_000 + i}": random.choice(customer_ids)
        for i in range(NUM_ACCOUNTS)
    }
    account_ids = list(account_to_customer.keys())

    anomalies_injected = {
        "null_account_id": 0,
        "duplicate_txn_id": 0,
        "invalid_amount": 0,
        "invalid_date": 0,
        "invalid_txn_type": 0,
        "invalid_currency": 0
    }

    rows = []
    # Reserve a small pool of duplicate transaction IDs
    duplicate_pool = []

    for i in range(1, total_rows + 1):
        txn_id = f"TXN_{10_000_000 + i}"
        account_id = random.choice(account_ids)
        customer_id = account_to_customer[account_id]

        # Valid defaults
        random_days = random.randint(0, DATE_RANGE_DAYS)
        txn_date = (START_DATE + timedelta(days=random_days)).strftime("%Y-%m-%d")
        txn_type = random.choices(TXN_TYPES, weights=[0.65, 0.25, 0.10])[0]

        # Amounts: Log-normal distribution to simulate realistic spend
        if txn_type == "CREDIT":
            # Deposits, salary, refunds
            amount = round(random.uniform(50.0, 5000.0), 2)
        elif txn_type == "TRANSFER":
            amount = round(random.uniform(20.0, 2500.0), 2)
        else:
            amount = round(random.uniform(1.50, 450.0), 2)

        currency = "USD"
        merchant = random.choice(MERCHANT_CATEGORIES)
        branch = random.choice(BRANCH_CITIES)
        status = random.choices(STATUSES, weights=[0.92, 0.05, 0.03])[0]

        # Anomaly Injection Roll (approximately 5% total bad rows)
        dice = random.random()

        if dice < 0.010:  # ~1.0% Null Account ID
            account_id = ""
            anomalies_injected["null_account_id"] += 1

        elif dice < 0.020:  # ~1.0% Duplicate Transaction ID
            if duplicate_pool:
                txn_id = random.choice(duplicate_pool)
                anomalies_injected["duplicate_txn_id"] += 1
            else:
                duplicate_pool.append(txn_id)

        elif dice < 0.030:  # ~1.0% Negative or Zero Amount
            amount = random.choice([0.0, -15.50, -125.00, -0.01, -500.0])
            anomalies_injected["invalid_amount"] += 1

        elif dice < 0.040:  # ~1.0% Invalid or Future Date
            anomaly_type = random.choice(["future", "past", "malformed"])
            if anomaly_type == "future":
                txn_date = "2029-11-20"  # Far in future
            elif anomaly_type == "past":
                txn_date = "1975-04-12"  # Beyond acceptable window
            else:
                txn_date = "2024-02-31"  # Non-existent calendar date
            anomalies_injected["invalid_date"] += 1

        elif dice < 0.045:  # ~0.5% Disallowed Transaction Type
            txn_type = random.choice(["CRYPTO_SWAP", "PAYPAL_DIRECT", "UNKNOWN_TYPE", "WIRE_VOUCHER"])
            anomalies_injected["invalid_txn_type"] += 1

        elif dice < 0.050:  # ~0.5% Malformed Currency Code
            currency = random.choice(["US", "USDD", "123", "$$$", "null"])
            anomalies_injected["invalid_currency"] += 1

        else:
            # Clean record; occasionally record txn_id to duplicate_pool
            if len(duplicate_pool) < 200 and random.random() < 0.05:
                duplicate_pool.append(txn_id)

        rows.append([
            txn_id, account_id, customer_id, txn_date, txn_type,
            amount, currency, merchant, branch, status
        ])

    fieldnames = [
        "transaction_id", "account_id", "customer_id", "txn_date", "txn_type",
        "amount", "currency", "merchant_category", "branch_city", "status"
    ]

    with open(output_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(fieldnames)
        writer.writerows(rows)

    total_bad_injected = sum(anomalies_injected.values())
    bad_pct = (total_bad_injected / total_rows) * 100

    logger.info(f"Successfully generated {total_rows:,} rows.")
    logger.info(f"Total deliberately injected bad rows: {total_bad_injected:,} ({bad_pct:.2f}%)")
    for anomaly, count in anomalies_injected.items():
        logger.info(f"  - {anomaly}: {count:,}")

    return {
        "total_rows": total_rows,
        "bad_injected": total_bad_injected,
        "bad_pct": bad_pct,
        "anomalies": anomalies_injected
    }


if __name__ == "__main__":
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_root = os.path.abspath(os.path.join(script_dir, ".."))
    raw_csv_path = os.path.join(project_root, "data", "raw", "bank_transactions_raw.csv")

    generate_synthetic_transactions(raw_csv_path, total_rows=TOTAL_ROWS)
