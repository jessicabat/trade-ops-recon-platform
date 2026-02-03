# Trade Operations Reconciliation Platform

This is a small end-to-end project that simulates a daily trading operations workflow.  
It takes synthetic trade files from an "internal" system and a "broker", loads them into PostgreSQL, runs reconciliation logic in SQL to find breaks, aggregates positions and cash, and then calculates daily PnL.

I built this to practice:
- Designing a simple schema for trades, positions, cash, and PnL.
- Using SQL as the main place for reconciliation logic (joins, comparisons, aggregations).
- Building an idempotent daily pipeline that I can safely re-run.

---

## Table of Contents
1. [Features](#features)
2. [Tech Stack](#tech-stack)
3. [How to Run It](#how-to-run-it)
4. [Project Structure](#project-structure)
5. [How the Reconciliation Works](#how-the-reconciliation-works)
   - [Data Model](#data-model-short-version)
   - [Trade Reconciliation](#trade-reconciliation)
   - [Positions and Cash](#positions-and-cash)
   - [PnL](#pnl)
6. [Example Outputs](#example-outputs)
7. [What I Focused On](#what-i-focused-on)

---

## Features

- Generates ~50,000 synthetic trades per day across multiple symbols, accounts, and strategies.
- Creates a "broker" copy of the data with intentional corruption:
  - Missing trades (present internally, missing at broker).
  - Phantom trades (present at broker, missing internally).
  - Price, quantity, fee, and settlement date mismatches.
- Loads internal and broker trades/positions/cash into PostgreSQL using COPY for speed.
- Reconciliation steps:
  - **Trades:** detects 6 break types between internal and broker trades.
  - **Positions:** compares net positions per account/symbol between internal and broker.
  - **Cash:** compares net cash balance per account/currency.
- PnL step:
  - Computes realized PnL by account, strategy, and symbol from internal trades.
- Reporting:
  - CLI summaries for trades, positions, cash, and PnL.
  - HTML end-of-day report with tables for breaks and PnL.
  - CSV exports for detailed trade breaks and PnL rows.

---

## Tech Stack

- **Database:** PostgreSQL
- **Language:** Python (pandas, SQLAlchemy)
- **Orchestration:** Bash (end-of-day pipeline script)
- **Reporting:** HTML + CSV outputs, Streamlit dashboard

---

## How to Run It

### 1. Prerequisites

- PostgreSQL installed and running locally.
- Python 3.10+.

### 2. Setup

```bash
# Clone the repo (if you haven't already)
cd ~/Documents/GitHub
# git clone <repo-url>
cd trade-ops-recon-platform

# Create and activate virtual environment
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install Python dependencies
pip install -r requirements.txt

# Create database and load schema
createdb trade_ops_recon
psql -d trade_ops_recon -f sql/schema.sql
```

### 3. Generate Data and Run the Full EOD Pipeline

For a fresh end-to-end run on a given date:
```bash
# From project root, with venv activated

# 1) Generate synthetic data files for today
python src/generate_data.py

# 2) Run the end-of-day pipeline for that date
#    (replace 2026-02-01 with your target date if needed)
./scripts/run_eod_pipeline.sh 2026-02-01
```

This script:

- Loads internal and broker trades/positions/cash into Postgres.
- Runs trade reconciliation and prints a summary.
- Runs position and cash reconciliation and prints summaries.
- Runs PnL calculation and prints a summary.
- Generates an HTML report and CSV exports under `data/processed/eod_reports/`.

To view the HTML report for that date:
```bash
open data/processed/eod_reports/eod_summary_2026-02-01.html
```

## Project Structure
trade-ops-recon-platform/
  sql/
    schema.sql               # Tables, views, helper functions
    recon_trades.sql         # Trade-level reconciliation logic
    recon_positions.sql      # Position-level reconciliation logic
    recon_cash.sql           # Cash-level reconciliation logic
    pnl_calculation.sql      # Daily PnL calculation

  src/
    generate_data.py         # Generate synthetic internal + broker CSVs
    load_to_db.py            # Bulk load CSVs into Postgres (idempotent by date)
    reconcile_trades.py      # Run trade reconciliation and print summary
    reconcile_positions_cash.py  # Run position & cash reconciliation
    calculate_pnl.py         # Compute daily realized PnL and print summary
    generate_reports.py      # Build HTML + CSV reports from the DB
    dashboard.py             # Streamlit dashboard

  scripts/
    run_eod_pipeline.sh      # One-command end-of-day pipeline
    reset_for_demo.sh        # Helper to clear DB + regenerate data (for demos)

  data/
    raw/                     # Generated CSV inputs (not committed to git)
    processed/
      eod_reports/           # HTML and CSV outputs

  README.md
  requirements.txt
  .gitignore

## How the Reconciliation Works
### Data Model (Short Version)
The core tables are:

- `internal_trades` — trades from the internal system.
- `broker_trades` — what the broker reports back.
- `recon_trades` — one row per trade-level break.
- `internal_positions` / `broker_positions` — net position per account/symbol.
- `internal_cash` / `broker_cash` — net cash per account/currency.
- `recon_positions` / `recon_cash` — one row per position/cash-level break.
- `daily_pnl` — realized PnL by account, strategy, symbol for a date.
- `pipeline_runs` — metadata for each ETL/recon/PnL run.

### Trade Reconciliation
At the trade level, I join `internal_trades` and `broker_trades` on `trade_id` and look for:

- Trades that exist internally but not at the broker.
- Trades that exist at the broker but not internally.
- Trades that exist in both places but disagree on price, quantity, fees, or settlement date.

Example (simplified) logic for missing internal trades:
```sql
INSERT INTO recon_trades (..., break_type, ...)
SELECT
    :recon_date,
    b.trade_id,
    b.symbol,
    b.account,
    'MISSING_IN_INTERNAL',
    ...
FROM broker_trades b
LEFT JOIN internal_trades i ON b.trade_id = i.trade_id
WHERE b.trade_date = :recon_date
  AND i.trade_id IS NULL;
```

Similar inserts handle `MISSING_IN_BROKER`, `PRICE_MISMATCH`, `QTY_MISMATCH`, `FEE_MISMATCH`, and `SETTLEMENT_MISMATCH`.
I also compute a rough notional impact and assign a severity bucket in SQL (LOW / MEDIUM / HIGH / CRITICAL) so it’s easy to see which breaks are large.

### Positions and Cash
Trades roll up into positions and cash. I reconcile those aggregates too:

- Positions: full outer join on `(account, symbol, date)` between `internal_positions` and `broker_positions` and compare `net_position`.
- Cash: full outer join on `(account, currency, date)` between `internal_cash` and `broker_cash` and compare `net_cash_balance`.

If internal and broker totals don’t match, I insert a row into `recon_positions` or `recon_cash` with the difference and a rough severity based on the size of the mismatch.

### PnL
For PnL, I focus on realized PnL from that day’s internal trades. A simplified version is:
```sql
INSERT INTO daily_pnl (pnl_date, account, strategy, symbol, realized_pnl, fees_total, trade_count)
SELECT
    :pnl_date,
    account,
    strategy,
    symbol,
    SUM(CASE WHEN side = 'BUY' THEN -1 * (quantity * price)
             ELSE (quantity * price) END) AS realized_pnl,
    SUM(fees) AS fees_total,
    COUNT(*) AS trade_count
FROM internal_trades
WHERE trade_date = :pnl_date
GROUP BY account, strategy, symbol;
```

The table also has a `total_pnl` generated column (`realized_pnl` + `unrealized_pnl`), but in this project unrealized PnL is left at zero because I don’t load market data.

### Example Outputs
These are actual outputs from a run on `2026-02-01` with 50,000 trades.

1. Trade Reconciliation Summary (CLI)
```text
BREAK TYPE                | SEVERITY   | COUNT    | TOTAL $         | AVG $
---------------------------------------------------------------------------
MISSING_IN_BROKER         | CRITICAL   | 458      | $526,553,334.79 | $1,149,679.77
MISSING_IN_INTERNAL       | CRITICAL   | 231      | $268,819,780.05 | $1,163,721.99
MISSING_IN_BROKER         | HIGH       | 38       | $ 1,997,094.26 | $52,555.11
MISSING_IN_INTERNAL       | HIGH       | 18       | $ 1,022,425.78 | $56,801.43
QTY_MISMATCH              | MEDIUM     | 169      | $   511,357.45 | $3,025.78
SETTLEMENT_MISMATCH       | MEDIUM     | 485      | $         0.00 | $    0.00
PRICE_MISMATCH            | LOW        | 462      | $    33,386.58 | $   72.27
FEE_MISMATCH              | LOW        | 959      | $       479.11 | $    0.50
---------------------------------------------------------------------------
Total Breaks Found: 2893
```
2. Position and Cash Reconciliation Summary (CLI)
```text
POSITION RECONCILIATION REPORT
===========================================================================
BREAK TYPE                | SEVERITY   | COUNT    | TOTAL DIFF   | AVG DIFF
---------------------------------------------------------------------------
POSITION_MISMATCH         | CRITICAL   | 35       |      367,459 |  10,498.83
POSITION_MISMATCH         | HIGH       | 5        |        2,515 |     503.00
---------------------------------------------------------------------------
Total Breaks Found: 40

CASH RECONCILIATION REPORT
===========================================================================
BREAK TYPE                | SEVERITY   | COUNT    | TOTAL $ DIFF    | AVG $ DIFF
---------------------------------------------------------------------------
CASH_MISMATCH             | CRITICAL   | 4        | $ 56,196,292.76 | $14,049,073.19
---------------------------------------------------------------------------
Total Breaks Found: 4
```

3. PnL by Strategy (CLI)
```text
STRATEGY             | SYMBOLS  | TRADES   | REALIZED PNL    | FEES         | NET PNL
----------------------------------------------------------------------------------------------------
DeltaNeutral         |       10 |    12465 | $31,227,063.86 | $ 97,131.70 | $31,227,063.86
StatisticalArb       |       10 |    12568 | $-120,430,990.46 | $ 97,771.59 | $-120,430,990.46
LiquidityProv        |       10 |    12517 | $-149,715,692.42 | $ 97,308.52 | $-149,715,692.42
MarketMaking         |       10 |    12450 | $-162,748,424.43 | $ 96,138.44 | $-162,748,424.43
----------------------------------------------------------------------------------------------------
TOTAL                |          |    50000 |                 | $388,350.25 | $-401,668,043.45
```

4. PnL Detail Example (from `daily_pnl`)
 symbol |    strategy    |  account   |  total_pnl  | trade_count
--------+----------------+------------+-------------+-------------
 QQQ    | StatisticalArb | ACCT_HEDGE | 48698087.88 |         354
 AMZN   | LiquidityProv  | ACCT_ARB   | 41805777.02 |         323
 SPY    | DeltaNeutral   | ACCT_ARB   | 40963510.33 |         280
 MSFT   | MarketMaking   | ACCT_FLOW  | 39345265.21 |         308
 TSLA   | StatisticalArb | ACCT_MAIN  | 38091199.30 |         331

## What I Focused On
- Keeping the reconciliation logic in SQL so it is easy to inspect and reason about.
- Making the daily pipeline idempotent: I can re-run a given date without creating duplicates.
- Seeing how trade-level breaks roll up into positions, cash, and PnL.
- Producing outputs (CLI summaries, HTML, CSV) that would be usable by someone in an operations role who needs to investigate why two systems disagree.
