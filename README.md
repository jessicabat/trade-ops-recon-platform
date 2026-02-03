# Trade Operations Reconciliation Platform

**Project Status:** Complete | **Stack:** Python, PostgreSQL, Bash, SQL

## Project Overview

This project is an end-to-end simulation of a Middle Office trading operations platform. I built this to understand the lifecycle of a trade *after* execution—specifically how financial firms ensure their internal books match external records (brokers/custodians) to prevent financial loss and settlement failure.

The system generates 50,000+ daily synthetic trades, corrupts a subset of them to simulate real-world operational breaks (e.g., price mismatches, missing booking, settlement errors), and runs an automated ETL and reconciliation pipeline to detect, classify, and report these risks.

## Motivation

As a Data Science student, I am comfortable with data analysis, but I wanted to understand the engineering and operational challenges of high-volume financial systems. I built this project to answer three questions:

1. How do firms catch expensive errors (like a missing trade) before they impact the bottom line?
2. How can SQL be used not just for querying, but for complex logic (Anti-Joins for break detection)?
3. How do you build a data pipeline that is **idempotent** (can be safely re-run multiple times)?

## Architecture

The system follows a standard extract-load-transform (ELT) pattern:

1. **Data Generation:** Python scripts create "Golden Source" internal logs and "Corrupted" broker logs.
2. **Ingestion:** A high-performance loader pushes CSVs into PostgreSQL.
3. **Processing:** SQL logic handles the heavy lifting for reconciliation and PnL calculation.
4. **Reporting:** Python extracts results for dashboards and daily status reports.

## Technical Highlights

### 1. Performance Optimization: Bulk Loading

Initially, I used standard Pandas `to_sql` inserts, which were too slow for 50,000+ rows. I refactored the pipeline to use the PostgreSQL `COPY` protocol via an in-memory buffer. This reduced the daily load time from ~45 seconds to <2 seconds, simulating the need for low-latency data handling in trading environments.

### 2. Reconciliation Logic (SQL)

I chose to implement the reconciliation logic in SQL rather than Python to ensure data integrity and auditability. The core logic uses **Anti-Joins** to find missing records and **Inner Joins** to compare field-level attributes.

**Example: Detecting "Missing in Broker" Breaks (Left Anti-Join)**
This query finds trades booked internally that the broker has no record of—a critical risk.

```sql
SELECT 
    i.trade_id,
    i.symbol,
    'MISSING_IN_BROKER' as break_type,
    ABS(i.quantity * i.price) as notional_impact
FROM internal_trades i
LEFT JOIN broker_trades b ON i.trade_id = b.trade_id
WHERE i.trade_date = '2026-02-01' 
  AND b.trade_id IS NULL; -- The "Anti-Join" filter

```

### 3. Risk Management: Dynamic Severity Classification

Not all breaks are equal. A $10 price break is noise; a $1M missing trade is a crisis. I implemented a UDF (User Defined Function) in Postgres to auto-classify breaks, allowing the operations team to prioritize "CRITICAL" issues first.

```sql
CREATE OR REPLACE FUNCTION classify_severity(p_notional NUMERIC) RETURNS VARCHAR AS $$
BEGIN
    CASE
        WHEN p_notional >= 100000 THEN RETURN 'CRITICAL';
        WHEN p_notional >= 10000 THEN RETURN 'HIGH';
        WHEN p_notional >= 1000 THEN RETURN 'MEDIUM';
        ELSE RETURN 'LOW';
    END CASE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

```

## The Data Pipeline

The entire system is orchestrated by a Bash script (`run_eod_pipeline.sh`) that simulates an End-of-Day (EOD) process. It enforces **idempotency**: before loading data for a specific date, it cleans up any partial data for that date to prevent duplicates.

**Pipeline Steps:**

1. **Load:** Ingest Internal Trades, Broker Trades, Positions, and Cash balances.
2. **Reconcile Trades:** Detect execution errors (Price, Qty, Fees, Settlement Date).
3. **Reconcile Books:** Compare aggregated Net Positions and Cash Balances.
4. **Calculate PnL:** Compute realized Profit & Loss by Strategy and Account.
5. **Report:** Generate HTML summaries and detailed CSV break lists.

## Sample Output

Below is the actual terminal output from a run processing 50,000 trades. The system successfully identified nearly $800M in "Critical" risk (simulated missing trades).

```text
BREAK TYPE                | SEVERITY   | COUNT    | TOTAL $         | AVG $
---------------------------------------------------------------------------
MISSING_IN_BROKER         | CRITICAL   | 458      | $526,553,334.79 | $1,149,679.77
MISSING_IN_INTERNAL       | CRITICAL   | 231      | $268,819,780.05 | $1,163,721.99
QTY_MISMATCH              | MEDIUM     | 169      | $   511,357.45 | $    3,025.78
FEE_MISMATCH              | LOW        | 959      | $       479.11 | $        0.50
---------------------------------------------------------------------------
Total Breaks Found: 2893

```

## Database Schema

The database is normalized to separate "Source of Truth" (Internal) from "External Data" (Broker).

* `internal_trades` / `broker_trades`: Raw trade logs.
* `recon_trades`: The exception table. Only stores problems.
* `pipeline_runs`: Metadata table to track SLA (start time, end time, success/fail status).

## How to Run Locally

**Prerequisites:** Python 3.10+, PostgreSQL.

1. **Clone and Setup:**
```bash
git clone https://github.com/yourusername/trade-ops-recon-platform.git
cd trade-ops-recon-platform
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

```


2. **Initialize Database:**
```bash
createdb trade_ops_recon
psql -d trade_ops_recon -f sql/schema.sql

```


3. **Run the Simulation:**
This command generates fresh data for the date and runs the full pipeline.
```bash
# Generate data and run pipeline for a specific date
python src/generate_data.py
./scripts/run_eod_pipeline.sh 2026-02-01

```


4. **View Dashboard:**
```bash
streamlit run src/dashboard.py

```



## Key Learnings

* **Settlement Date Risk:** I learned that a trade matching on Price/Qty is not enough; if the Internal system expects settlement T+1 and the Broker expects T+2, the firm will have a cash liquidity issue. My system now checks specifically for `SETTLEMENT_MISMATCH`.
* **The "Books and Records" Concept:** I initially thought PnL was just `Price * Qty`. I realized that for Operations, PnL must be reconciled against the *actual* cash movement at the broker, accounting for fees, to be accurate.
* **Operational Resilience:** Writing the pipeline to be re-runnable (handling failures gracefully) was just as important as the reconciliation logic itself.