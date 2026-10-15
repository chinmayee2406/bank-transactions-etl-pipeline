"""
test_pipeline_e2e.py - Step 5: End-to-End Pipeline Verification
===============================================================
Runs an end-to-end test on a tiny CSV fixture (10 rows: 7 valid, 3 invalid).
Asserts exact clean vs. rejected counts and validates rejection reasons.
Runs in milliseconds without requiring any Spark cluster or JVM.
"""

import os
import sys
import csv
import tempfile
import unittest

# Add project root to sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.validators import (
    validate_records_pure_python,
    RULE_NOT_NULL,
    RULE_DUPLICATE_TXN_ID,
    RULE_INVALID_AMOUNT,
    RULE_INVALID_TXN_TYPE,
    RULE_INVALID_CURRENCY
)


def create_tiny_test_csv(file_path: str):
    """
    Creates a controlled 10-row CSV fixture:
      - 7 valid rows (including leap day)
      - 3 invalid rows:
          Row 6: Null account_id AND negative amount (-45.0)
          Row 8: Invalid txn_type ('CRYPTO') AND malformed currency ('USDD')
          Row 10: Duplicate transaction_id ('TXN_001')
    """
    fieldnames = [
        "transaction_id", "account_id", "customer_id", "txn_date", "txn_type",
        "amount", "currency", "merchant_category", "branch_city", "status"
    ]
    rows = [
        # 1. Clean row 1
        ["TXN_001", "ACC_001", "CUST_001", "2024-01-10", "DEBIT", "50.00", "USD", "GROCERY", "New York", "COMPLETED"],
        # 2. Clean row 2
        ["TXN_002", "ACC_001", "CUST_001", "2024-02-15", "CREDIT", "1500.00", "USD", "SALARY", "New York", "COMPLETED"],
        # 3. Clean row 3 (Leap year date: 2024-02-29)
        ["TXN_003", "ACC_002", "CUST_002", "2024-02-29", "TRANSFER", "200.00", "USD", "INVESTMENT", "Charlotte", "COMPLETED"],
        # 4. Clean row 4
        ["TXN_004", "ACC_002", "CUST_002", "2024-03-05", "DEBIT", "12.50", "USD", "DINING", "Dallas", "COMPLETED"],
        # 5. Clean row 5
        ["TXN_005", "ACC_003", "CUST_003", "2024-04-12", "CREDIT", "250.00", "USD", "RETAIL", "Atlanta", "COMPLETED"],
        # 6. Bad row 1: Null account_id + negative amount (-45.00)
        ["TXN_006", "", "CUST_004", "2024-05-01", "DEBIT", "-45.00", "USD", "TRAVEL", "Chicago", "COMPLETED"],
        # 7. Clean row 6
        ["TXN_007", "ACC_004", "CUST_004", "2024-06-20", "TRANSFER", "75.00", "USD", "UTILITIES", "Boston", "COMPLETED"],
        # 8. Bad row 2: Invalid txn_type ('CRYPTO') + Malformed currency ('USDD')
        ["TXN_008", "ACC_005", "CUST_005", "2024-07-04", "CRYPTO", "100.00", "USDD", "OTHER", "Seattle", "COMPLETED"],
        # 9. Clean row 7
        ["TXN_009", "ACC_005", "CUST_005", "2024-08-19", "DEBIT", "89.20", "USD", "HEALTHCARE", "San Francisco", "COMPLETED"],
        # 10. Bad row 3: Duplicate transaction_id of TXN_001
        ["TXN_001", "ACC_006", "CUST_006", "2024-09-01", "DEBIT", "35.00", "USD", "ENTERTAINMENT", "Phoenix", "COMPLETED"]
    ]

    with open(file_path, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(fieldnames)
        writer.writerows(rows)


def test_end_to_end_pipeline_on_fixture():
    """
    E2E Test: Loads 10-row fixture, executes validation and quarantine segregation,
    and asserts exact clean and rejected row counts.
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        fixture_path = os.path.join(tmp_dir, "test_input.csv")
        create_tiny_test_csv(fixture_path)

        # 1. Extract
        records = []
        with open(fixture_path, mode="r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                records.append(row)

        assert len(records) == 10, f"Expected 10 extracted rows, got {len(records)}"

        # 2. Transform & Validate
        clean_records, rejected_records, dq_report = validate_records_pure_python(records)

        # 3. Assert Exact Counts
        assert len(clean_records) == 7, f"Expected 7 clean records, got {len(clean_records)}"
        assert len(rejected_records) == 3, f"Expected 3 rejected records, got {len(rejected_records)}"
        assert dq_report["total_rows"] == 10
        assert dq_report["clean_rows"] == 7
        assert dq_report["rejected_rows"] == 3
        assert dq_report["pass_rate_pct"] == 70.0

        # 4. Verify Specific Quarantined Failure Reasons
        rejected_by_id = {r["transaction_id"]: r["reject_reason"] for r in rejected_records}

        # TXN_006 had empty account_id and negative amount
        assert "TXN_006" in rejected_by_id
        assert RULE_NOT_NULL in rejected_by_id["TXN_006"]
        assert RULE_INVALID_AMOUNT in rejected_by_id["TXN_006"]

        # TXN_008 had CRYPTO type and USDD currency
        assert "TXN_008" in rejected_by_id
        assert RULE_INVALID_TXN_TYPE in rejected_by_id["TXN_008"]
        assert RULE_INVALID_CURRENCY in rejected_by_id["TXN_008"]

        # Duplicate TXN_001
        assert "TXN_001" in rejected_by_id
        assert RULE_DUPLICATE_TXN_ID in rejected_by_id["TXN_001"]

        # Check that original clean TXN_001 remains in clean_records
        clean_ids = [r["transaction_id"] for r in clean_records]
        assert "TXN_001" in clean_ids
        assert len([i for i in clean_ids if i == "TXN_001"]) == 1


class TestPipelineE2EUnittest(unittest.TestCase):
    def test_e2e_pipeline(self):
        test_end_to_end_pipeline_on_fixture()


if __name__ == "__main__":
    unittest.main()
