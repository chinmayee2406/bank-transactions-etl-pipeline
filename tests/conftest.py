"""
conftest.py - Pytest Fixtures for Banking ETL Pipeline Tests
============================================================
Provides reusable transaction dictionaries and tiny fixture files.
Runs without requiring any Spark cluster or JVM.
"""

import os
import csv
import pytest

@pytest.fixture
def valid_record():
    return {
        "transaction_id": "TXN_99990001",
        "account_id": "ACC_888001",
        "customer_id": "CUST_777001",
        "txn_date": "2024-06-15",
        "txn_type": "DEBIT",
        "amount": 145.75,
        "currency": "USD",
        "merchant_category": "GROCERY",
        "branch_city": "Charlotte",
        "status": "COMPLETED"
    }

@pytest.fixture
def tiny_fixture_csv(tmp_path):
    """
    Creates a tiny 10-row CSV fixture: 7 valid rows, 3 deliberately bad rows.
    """
    file_path = tmp_path / "tiny_fixture.csv"
    fieldnames = [
        "transaction_id", "account_id", "customer_id", "txn_date", "txn_type",
        "amount", "currency", "merchant_category", "branch_city", "status"
    ]
    rows = [
        # 1. Clean row 1
        ["TXN_001", "ACC_001", "CUST_001", "2024-01-10", "DEBIT", 50.00, "USD", "GROCERY", "New York", "COMPLETED"],
        # 2. Clean row 2
        ["TXN_002", "ACC_001", "CUST_001", "2024-02-15", "CREDIT", 1500.00, "USD", "SALARY", "New York", "COMPLETED"],
        # 3. Clean row 3 (Leap year date)
        ["TXN_003", "ACC_002", "CUST_002", "2024-02-29", "TRANSFER", 200.00, "USD", "INVESTMENT", "Charlotte", "COMPLETED"],
        # 4. Clean row 4
        ["TXN_004", "ACC_002", "CUST_002", "2024-03-05", "DEBIT", 12.50, "USD", "DINING", "Dallas", "COMPLETED"],
        # 5. Clean row 5
        ["TXN_005", "ACC_003", "CUST_003", "2024-04-12", "CREDIT", 250.00, "USD", "RETAIL", "Atlanta", "COMPLETED"],
        # 6. Bad row 1: Null account_id + negative amount (multiple violations)
        ["TXN_006", "", "CUST_004", "2024-05-01", "DEBIT", -45.00, "USD", "TRAVEL", "Chicago", "COMPLETED"],
        # 7. Clean row 6
        ["TXN_007", "ACC_004", "CUST_004", "2024-06-20", "TRANSFER", 75.00, "USD", "UTILITIES", "Boston", "COMPLETED"],
        # 8. Bad row 2: Invalid transaction type (CRYPTO) + Malformed currency (USDD)
        ["TXN_008", "ACC_005", "CUST_005", "2024-07-04", "CRYPTO", 100.00, "USDD", "OTHER", "Seattle", "COMPLETED"],
        # 9. Clean row 7
        ["TXN_009", "ACC_005", "CUST_005", "2024-08-19", "DEBIT", 89.20, "USD", "HEALTHCARE", "San Francisco", "COMPLETED"],
        # 10. Bad row 3: Duplicate transaction_id of TXN_001
        ["TXN_001", "ACC_006", "CUST_006", "2024-09-01", "DEBIT", 35.00, "USD", "ENTERTAINMENT", "Phoenix", "COMPLETED"]
    ]

    with open(file_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(fieldnames)
        writer.writerows(rows)

    return str(file_path)
