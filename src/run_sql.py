"""
run_sql.py - Step 4: Analytical Query Execution Runner
======================================================
Parses sql/analytics.sql, executes all 6 queries against the SQLite database,
and renders structured tabular outputs with column headers and timing.
"""

import os
import sys
import time
import sqlite3

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.abspath(os.path.join(SCRIPT_DIR, ".."))
DB_PATH = os.path.join(PROJECT_ROOT, "db", "banking.db")
SQL_FILE_PATH = os.path.join(PROJECT_ROOT, "sql", "analytics.sql")


def print_ascii_table(title: str, headers: list, rows: list):
    """
    Renders an ASCII table without requiring third-party libraries.
    """
    col_widths = [len(h) for h in headers]
    for row in rows:
        for idx, val in enumerate(row):
            col_widths[idx] = max(col_widths[idx], len(str(val) if val is not None else "NULL"))

    border_line = "+" + "+".join(["-" * (w + 2) for w in col_widths]) + "+"
    header_line = "| " + " | ".join([f"{h:<{col_widths[i]}}" for i, h in enumerate(headers)]) + " |"

    print("\n" + "=" * len(border_line))
    print(f" {title.upper()}")
    print("=" * len(border_line))
    print(border_line)
    print(header_line)
    print(border_line)

    if not rows:
        print("| " + "No records returned".center(len(border_line) - 4) + " |")
    else:
        for row in rows:
            formatted_vals = []
            for i, val in enumerate(row):
                val_str = str(val) if val is not None else "NULL"
                # Right align numbers, left align strings
                if isinstance(val, (int, float)):
                    formatted_vals.append(f"{val_str:>{col_widths[i]}}")
                else:
                    formatted_vals.append(f"{val_str:<{col_widths[i]}}")
            print("| " + " | ".join(formatted_vals) + " |")

    print(border_line)
    print(f"Rows returned: {len(rows)}")


def parse_and_run_queries(db_path: str, sql_file: str):
    """
    Executes each analytical query from the SQL script.
    """
    if not os.path.exists(db_path):
        print(f"Error: Database not found at {db_path}. Run the ETL pipeline first!")
        sys.exit(1)

    if not os.path.exists(sql_file):
        print(f"Error: SQL file not found at {sql_file}")
        sys.exit(1)

    with open(sql_file, "r", encoding="utf-8") as f:
        content = f.read()

    # Split by query marker @NAME:
    raw_sections = content.split("-- @NAME: ")
    queries = []

    for sec in raw_sections:
        if not sec.strip():
            continue
        lines = sec.strip().split("\n")
        title = lines[0].strip()
        sql_body = "\n".join(lines[1:]).strip()
        if sql_body:
            queries.append((title, sql_body))

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    print(f"Executing {len(queries)} analytical queries against: {db_path}...")

    total_start = time.time()
    for idx, (title, sql) in enumerate(queries, 1):
        q_start = time.time()
        try:
            cursor.execute(sql)
            headers = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            q_time = (time.time() - q_start) * 1000
            print_ascii_table(f"[{idx}/6] {title} ({q_time:.1f}ms)", headers, rows)
        except Exception as e:
            print(f"\n[ERROR] Failed to execute query {idx}: {title}\nDetails: {e}")

    conn.close()
    print(f"\nAll analytics queries completed in {(time.time() - total_start):.2f} seconds.\n")


if __name__ == "__main__":
    parse_and_run_queries(DB_PATH, SQL_FILE_PATH)
