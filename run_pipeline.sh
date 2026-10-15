#!/usr/bin/env bash
# ==============================================================================
# run_pipeline.sh - End-to-End Orchestrator for Banking Transactions ETL Pipeline
# ==============================================================================
# Execution Stages:
#   1. Generate 200,000 synthetic banking records with ~5% deliberate anomalies
#   2. Execute PySpark/Python ETL (Extract, Validate & Quarantine, Partitioned Load & SQLite)
#   3. Execute SQL Analytics (Window functions, aggregates, outlier detection)
#   4. Execute Unit and End-to-End Test Suite (Zero-Spark fast regression)
#
# CRONTAB SCHEDULING SPECIFICATION (Daily at 02:00 AM Eastern Time):
# ------------------------------------------------------------------------------
# Open crontab editor:
#   crontab -e
# Add the following entry:
#   0 2 * * * cd /path/to/bank_transactions_etl && ./run_pipeline.sh >> logs/cron.log 2>&1
# ==============================================================================

set -euo pipefail

# ANSI Color codes for stage visibility
readonly BOLD='\033[1m'
readonly GREEN='\033[0;32m'
readonly BLUE='\033[0;34m'
readonly YELLOW='\033[1;33m'
readonly RED='\033[0;31m'
readonly NC='\033[0m' # No Color

# Determine project root directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "${SCRIPT_DIR}"

PIPELINE_START_TIME=$(date +%s)
LOG_FILE="${SCRIPT_DIR}/logs/pipeline.log"
mkdir -p "${SCRIPT_DIR}/logs"

log_info() {
    local msg="$1"
    local timestamp
    timestamp=$(date "+%Y-%m-%d %H:%M:%S")
    echo -e "${BLUE}[${timestamp}] [PIPELINE]${NC} ${msg}"
}

log_stage() {
    local stage="$1"
    local desc="$2"
    echo -e "\n${BOLD}${YELLOW}================================================================================${NC}"
    echo -e "${BOLD}${GREEN} ${stage}: ${desc}${NC}"
    echo -e "${BOLD}${YELLOW}================================================================================${NC}"
}

log_error() {
    local msg="$1"
    local timestamp
    timestamp=$(date "+%Y-%m-%d %H:%M:%S")
    echo -e "${RED}[${timestamp}] [ERROR]${NC} ${msg}" >&2
}

# Trap unexpected errors
trap 'log_error "Pipeline failed at line $LINENO while executing command: $BASH_COMMAND"; exit 1' ERR

echo -e "${BOLD}${BLUE}"
cat << "EOF"
  ____             _     ____  _               _ _             
 | __ )  ___   ___| |__ |  _ \(_)_ __   ___| (_)_ __   ___   
 |  _ \ / _ \ / _ \ '_ \| |_) | | '_ \ / _ \ | | '_ \ / _ \  
 | |_) | (_) |  __/ | | |  __/| | |_) |  __/ | | | | |  __/  
 |____/ \___/ \___|_| |_|_|   |_| .__/ \___|_|_|_| |_|\___|  
                                |_|                           
 Bank of America - Data Engineering Portfolio Project
 Pipeline Orchestrator v1.0
EOF
echo -e "${NC}"

log_info "Initiating end-to-end pipeline run from directory: ${SCRIPT_DIR}"

# ------------------------------------------------------------------------------
# STAGE 1: SYNTHETIC DATA GENERATION
# ------------------------------------------------------------------------------
log_stage "STAGE 1/4" "Generating 200,000 Synthetic Transactions (with ~5% Bad Rows)"
python3 src/generate_data.py

# ------------------------------------------------------------------------------
# STAGE 2: ETL EXTRACTION, VALIDATION, AND LOADING
# ------------------------------------------------------------------------------
log_stage "STAGE 2/4" "Executing Extract, Transform/Validate & Load Pipeline"
python3 src/etl_pipeline.py

# ------------------------------------------------------------------------------
# STAGE 3: SQL ANALYTICS EXECUTION
# ------------------------------------------------------------------------------
log_stage "STAGE 3/4" "Running 6 Analytical SQL Queries (Windowing, Aggregates, Outliers)"
python3 src/run_sql.py

# ------------------------------------------------------------------------------
# STAGE 4: UNIT & END-TO-END TEST SUITE
# ------------------------------------------------------------------------------
log_stage "STAGE 4/4" "Executing Automated Test Suite (Pytest / Unittest Zero-Spark)"
if command -v pytest >/dev/null 2>&1; then
    pytest tests/ -v
else
    python3 -m unittest discover tests/ -v
fi

# ------------------------------------------------------------------------------
# PIPELINE COMPLETION SUMMARY
# ------------------------------------------------------------------------------
PIPELINE_END_TIME=$(date +%s)
TOTAL_DURATION=$((PIPELINE_END_TIME - PIPELINE_START_TIME))

echo -e "\n${BOLD}${GREEN}================================================================================${NC}"
echo -e "${BOLD}${GREEN} PIPELINE RUN COMPLETED SUCCESSFULLY in ${TOTAL_DURATION}s!${NC}"
echo -e "${BOLD}${GREEN}================================================================================${NC}"
log_info "Outputs Generated:"
log_info "  - Raw CSV        : data/raw/bank_transactions_raw.csv (200,000 rows)"
log_info "  - Clean Data     : data/clean/ (Partitioned by txn_year_month)"
log_info "  - Rejected CSV   : data/rejected/rejected_transactions.csv"
log_info "  - SQLite DB      : db/banking.db (Table: transactions with composite index)"
log_info "  - DQ JSON Report : reports/data_quality_report.json"
log_info "  - Execution Logs : logs/pipeline.log"
