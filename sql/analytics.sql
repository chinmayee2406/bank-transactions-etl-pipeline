-- ==============================================================================
-- analytics.sql - Step 4: Analytical & Diagnostic SQL Queries
-- ==============================================================================
-- Database: banking.db (SQLite)
-- Schema: transactions(transaction_id, account_id, customer_id, txn_date,
--                      txn_type, amount, currency, merchant_category,
--                      branch_city, status, txn_year_month, loaded_at)
-- ==============================================================================

-- ------------------------------------------------------------------------------
-- QUERY 1: Monthly Transaction Volume and Total Amount
-- Purpose: Executive summary of monthly throughput, velocity, and average ticket size.
-- ------------------------------------------------------------------------------
-- @NAME: Query 1 - Monthly Volume and Amount
SELECT
    strftime('%Y-%m', txn_date) AS txn_month,
    COUNT(*) AS total_transactions,
    ROUND(SUM(amount), 2) AS total_volume_usd,
    ROUND(AVG(amount), 2) AS avg_transaction_amount
FROM transactions
GROUP BY strftime('%Y-%m', txn_date)
ORDER BY txn_month ASC;

-- ------------------------------------------------------------------------------
-- QUERY 2: Top 5 Merchant Categories by Spend (Debit Outflows)
-- Purpose: Identifies customer expenditure concentration across retail categories.
-- ------------------------------------------------------------------------------
-- @NAME: Query 2 - Top 5 Merchant Categories by Spend
SELECT
    merchant_category,
    COUNT(*) AS transaction_count,
    ROUND(SUM(amount), 2) AS total_spend_usd,
    ROUND(AVG(amount), 2) AS avg_spend_usd
FROM transactions
WHERE txn_type = 'DEBIT'
GROUP BY merchant_category
ORDER BY total_spend_usd DESC
LIMIT 5;

-- ------------------------------------------------------------------------------
-- QUERY 3: Running Balance per Account Using Window Function
-- Purpose: Demonstrates stateful financial ledger computation over time partitions.
-- Note: Credits add (+), Debits and Transfers subtract (-).
-- ------------------------------------------------------------------------------
-- @NAME: Query 3 - Running Balance per Account (Sample Account Window)
WITH signed_transactions AS (
    SELECT
        transaction_id,
        account_id,
        customer_id,
        txn_date,
        txn_type,
        amount,
        CASE
            WHEN txn_type = 'CREDIT' THEN amount
            ELSE -amount
        END AS net_amount
    FROM transactions
)
SELECT
    transaction_id,
    account_id,
    txn_date,
    txn_type,
    amount,
    ROUND(
        SUM(net_amount) OVER (
            PARTITION BY account_id
            ORDER BY txn_date ASC, transaction_id ASC
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ), 2
    ) AS running_balance
FROM signed_transactions
WHERE account_id IN (
    SELECT account_id FROM transactions GROUP BY account_id HAVING COUNT(*) >= 15 LIMIT 1
)
ORDER BY account_id, txn_date ASC, transaction_id ASC
LIMIT 15;

-- ------------------------------------------------------------------------------
-- QUERY 4: Top 3 Largest Transactions per Customer Using ROW_NUMBER()
-- Purpose: High-value transaction monitoring and VIP profiling via analytical ranking.
-- ------------------------------------------------------------------------------
-- @NAME: Query 4 - Top 3 Largest Transactions per Customer
WITH ranked_transactions AS (
    SELECT
        customer_id,
        transaction_id,
        account_id,
        txn_date,
        txn_type,
        amount,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY amount DESC, txn_date DESC
        ) AS rank_by_amount
    FROM transactions
)
SELECT
    customer_id,
    rank_by_amount,
    transaction_id,
    account_id,
    txn_date,
    txn_type,
    amount
FROM ranked_transactions
WHERE rank_by_amount <= 3
  AND customer_id IN (
      SELECT customer_id FROM transactions GROUP BY customer_id HAVING COUNT(*) >= 10 LIMIT 3
  )
ORDER BY customer_id ASC, rank_by_amount ASC;

-- ------------------------------------------------------------------------------
-- QUERY 5: Accounts with Unusual Activity (> 3x Individual Account Average)
-- Purpose: Rule-based anomaly and fraud detection flagging statistical outliers.
-- ------------------------------------------------------------------------------
-- @NAME: Query 5 - Outlier Transactions (>3x Account Baseline Average)
WITH account_profiles AS (
    SELECT
        account_id,
        COUNT(*) AS total_account_txns,
        ROUND(AVG(amount), 2) AS avg_txn_amount
    FROM transactions
    GROUP BY account_id
    HAVING COUNT(*) >= 5
)
SELECT
    t.account_id,
    t.transaction_id,
    t.txn_date,
    t.txn_type,
    t.merchant_category,
    t.amount AS transaction_amount,
    p.avg_txn_amount AS account_baseline_avg,
    ROUND(t.amount / p.avg_txn_amount, 2) AS multiplier_over_avg
FROM transactions t
JOIN account_profiles p ON t.account_id = p.account_id
WHERE t.amount > (3.0 * p.avg_txn_amount)
ORDER BY multiplier_over_avg DESC
LIMIT 10;

-- ------------------------------------------------------------------------------
-- QUERY 6: Daily Active Accounts (DAA) and Activity Volume
-- Purpose: Platform engagement and liquidity monitoring across calendar days.
-- ------------------------------------------------------------------------------
-- @NAME: Query 6 - Daily Active Accounts and Volume
SELECT
    txn_date,
    COUNT(DISTINCT account_id) AS active_accounts,
    COUNT(*) AS total_transactions,
    ROUND(SUM(amount), 2) AS daily_volume_usd
FROM transactions
GROUP BY txn_date
ORDER BY txn_date ASC
LIMIT 15;
