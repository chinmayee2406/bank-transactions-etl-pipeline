"""
test_validators.py - Step 5: Unit Tests for Validation Rules
=============================================================
Comprehensive unit tests for every validator in src/validators.py.
Tested against regular and edge cases.
Zero dependency on Spark cluster or JVM.
"""

import sys
import os
import unittest
import math

# Add project root to sys.path
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

from src.validators import (
    is_valid_not_null,
    is_valid_amount,
    is_valid_date,
    is_valid_txn_type,
    is_valid_currency,
    validate_single_record,
    RULE_NOT_NULL,
    RULE_DUPLICATE_TXN_ID,
    RULE_INVALID_AMOUNT,
    RULE_INVALID_DATE,
    RULE_INVALID_TXN_TYPE,
    RULE_INVALID_CURRENCY
)


# ==============================================================================
# PYTEST-STYLE TEST FUNCTIONS
# ==============================================================================

def test_is_valid_not_null():
    """Test 1: Evaluates not-null logic on normal strings, empty strings, None, and zero."""
    assert is_valid_not_null("ACC_12345") is True
    assert is_valid_not_null(0) is True  # Numeric zero is NOT null
    assert is_valid_not_null(0.0) is True
    assert is_valid_not_null(False) is True
    assert is_valid_not_null(None) is False
    assert is_valid_not_null("") is False
    assert is_valid_not_null("   ") is False


def test_is_valid_amount_positive():
    """Test 2: Evaluates valid positive numeric amounts."""
    assert is_valid_amount(100.50) is True
    assert is_valid_amount(0.01) is True  # Minimum valid cent
    assert is_valid_amount(5000) is True  # Integer
    assert is_valid_amount("250.75") is True  # String representation


def test_is_valid_amount_edge_cases():
    """Test 3: Evaluates edge cases for amounts (zero, negative, NaN, Inf, non-numeric)."""
    assert is_valid_amount(0.0) is False  # Zero amount is not allowed
    assert is_valid_amount(0) is False
    assert is_valid_amount(-0.01) is False
    assert is_valid_amount(-150.00) is False
    assert is_valid_amount(float("nan")) is False
    assert is_valid_amount(float("inf")) is False
    assert is_valid_amount("invalid_number") is False
    assert is_valid_amount(None) is False


def test_is_valid_date_valid_calendar():
    """Test 4: Evaluates valid dates including leap day."""
    assert is_valid_date("2024-01-01") is True
    assert is_valid_date("2024-06-15") is True
    assert is_valid_date("2024-12-31") is True
    # 2024 was a leap year, so Feb 29 is valid
    assert is_valid_date("2024-02-29") is True


def test_is_valid_date_invalid_calendar():
    """Test 5: Evaluates invalid calendar dates (e.g. Feb 31, April 31) and malformed strings."""
    assert is_valid_date("2024-02-31") is False  # Calendar invalid
    assert is_valid_date("2024-04-31") is False  # April has 30 days
    assert is_valid_date("2024-13-01") is False  # Invalid month
    assert is_valid_date("06/15/2024") is False  # Incorrect format (must be YYYY-MM-DD)
    assert is_valid_date("20240615") is False    # Missing separators
    assert is_valid_date("not-a-date") is False
    assert is_valid_date(None) is False
    assert is_valid_date("") is False


def test_is_valid_date_out_of_range():
    """Test 6: Evaluates dates outside the allowed business boundary."""
    assert is_valid_date("2029-01-01") is False  # Future date
    assert is_valid_date("1990-05-12") is False  # Ancient historical date
    assert is_valid_date("2023-12-31") is False  # Day before boundary window


def test_is_valid_txn_type():
    """Test 7: Evaluates allowed transaction types and rejection of disallowed types."""
    assert is_valid_txn_type("DEBIT") is True
    assert is_valid_txn_type("CREDIT") is True
    assert is_valid_txn_type("TRANSFER") is True
    assert is_valid_txn_type("debit") is True  # Case insensitive handling
    assert is_valid_txn_type("CRYPTO") is False
    assert is_valid_txn_type("REFUND") is False
    assert is_valid_txn_type("UNKNOWN") is False
    assert is_valid_txn_type("") is False
    assert is_valid_txn_type(None) is False


def test_is_valid_currency():
    """Test 8: Evaluates 3-letter uppercase ISO currency code enforcement."""
    assert is_valid_currency("USD") is True
    assert is_valid_currency("EUR") is True
    assert is_valid_currency("GBP") is True
    assert is_valid_currency("US") is False    # Too short (2 letters)
    assert is_valid_currency("USDD") is False  # Too long (4 letters)
    assert is_valid_currency("usd") is False   # Lowercase not permitted by regex
    assert is_valid_currency("123") is False   # Numeric
    assert is_valid_currency("$$$") is False   # Symbols
    assert is_valid_currency(None) is False


def test_validate_single_record_clean():
    """Test 9: Verifies that a clean record produces zero rejection reasons."""
    clean_row = {
        "transaction_id": "TXN_101",
        "account_id": "ACC_202",
        "customer_id": "CUST_303",
        "txn_date": "2024-05-10",
        "txn_type": "DEBIT",
        "amount": 42.50,
        "currency": "USD"
    }
    reasons = validate_single_record(clean_row)
    assert len(reasons) == 0
    assert reasons == []


def test_validate_single_record_multiple_violations():
    """Test 10: Verifies concatenation when a single record violates multiple business rules."""
    bad_row = {
        "transaction_id": "TXN_999",
        "account_id": "",          # Missing account_id (NOT_NULL)
        "customer_id": "CUST_888",
        "txn_date": "2029-05-01",  # Future date (INVALID_DATE)
        "txn_type": "CRYPTO_BUY",  # Disallowed type (UNRECOGNIZED_TXN_TYPE)
        "amount": -50.00,          # Negative amount (NEGATIVE_OR_ZERO_AMOUNT)
        "currency": "US"           # Malformed currency (MALFORMED_CURRENCY_CODE)
    }
    reasons = validate_single_record(bad_row)
    # Check that each expected violation is captured
    reason_str = ";".join(reasons)
    assert RULE_NOT_NULL in reason_str
    assert RULE_INVALID_AMOUNT in reason_str
    assert RULE_INVALID_DATE in reason_str
    assert RULE_INVALID_TXN_TYPE in reason_str
    assert RULE_INVALID_CURRENCY in reason_str
    assert len(reasons) >= 5


def test_duplicate_transaction_id_flagged():
    """Test 11: Verifies that duplicate transaction IDs are detected across sequential rows."""
    seen = set()
    row1 = {
        "transaction_id": "TXN_DUP_001",
        "account_id": "ACC_1",
        "customer_id": "CUST_1",
        "txn_date": "2024-01-05",
        "txn_type": "DEBIT",
        "amount": 20.0,
        "currency": "USD"
    }
    row2 = {
        "transaction_id": "TXN_DUP_001",  # Same ID
        "account_id": "ACC_2",
        "customer_id": "CUST_2",
        "txn_date": "2024-01-06",
        "txn_type": "CREDIT",
        "amount": 100.0,
        "currency": "USD"
    }

    reasons1 = validate_single_record(row1, seen_ids=seen)
    reasons2 = validate_single_record(row2, seen_ids=seen)

    assert reasons1 == []
    assert RULE_DUPLICATE_TXN_ID in reasons2


# ==============================================================================
# UNITTEST WRAPPER (Enables execution via python3 -m unittest without pytest)
# ==============================================================================
class TestValidatorsUnittest(unittest.TestCase):
    def test_01_not_null(self):
        test_is_valid_not_null()

    def test_02_amount_positive(self):
        test_is_valid_amount_positive()

    def test_03_amount_edge_cases(self):
        test_is_valid_amount_edge_cases()

    def test_04_date_calendar(self):
        test_is_valid_date_valid_calendar()

    def test_05_date_invalid_calendar(self):
        test_is_valid_date_invalid_calendar()

    def test_06_date_out_of_range(self):
        test_is_valid_date_out_of_range()

    def test_07_txn_type(self):
        test_is_valid_txn_type()

    def test_08_currency(self):
        test_is_valid_currency()

    def test_09_clean_record(self):
        test_validate_single_record_clean()

    def test_10_multiple_violations(self):
        test_validate_single_record_multiple_violations()

    def test_11_duplicate_txn_id(self):
        test_duplicate_transaction_id_flagged()


if __name__ == "__main__":
    unittest.main()
