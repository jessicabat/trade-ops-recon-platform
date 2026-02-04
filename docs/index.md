---
title: Trade Operations Reconciliation Platform
layout: default
---

# Trade Operations Reconciliation Platform

This project simulates a day in the life of a trading operations team.  
Each day, an “internal” trading system and a “broker” both produce files describing the same trades. They don’t always agree. This pipeline loads both views into PostgreSQL, compares them, shows where they disagree, and rolls those differences up into positions, cash, and daily PnL.

I built it to get hands-on with:

- How trade, position, cash, and PnL data fit together.
- Using SQL as the main place for reconciliation logic.
- Designing a daily pipeline that is safe to re-run for a given date.

If you want to see the full implementation (schema, SQL, scripts), the GitHub README goes into detail.

[View the code and technical README →](https://github.com/jessicabat/trade-ops-recon-platform/README.md)

---

## What this project does

You can think of the system as answering three questions:

1. **Do the internal system and the broker agree on trades?**
2. **Do they agree on positions and cash once trades are aggregated?**
3. **Given the internal trades, what does daily PnL look like by account, strategy, and symbol?**

To do that, the pipeline runs once per date and:

- Generates synthetic internal and broker files, with intentional errors.
- Loads everything into PostgreSQL in a repeatable way.
- Runs a set of SQL checks to flag missing and mismatched trades.
- Aggregates trades into positions and cash and reconciles those totals.
- Calculates realized PnL.
- Produces an HTML report and CSVs that an operations analyst could actually use.

---

## Architecture at a glance

Here’s the overall flow of the system:

![Architecture diagram](assets/system_architecture.png)

From left to right:

- **Data generation (Python)**  
  Creates internal and broker CSV files for trades, positions, and cash, and injects realistic errors such as missing trades or slightly different prices.

- **Raw CSV files**  
  The files live under `data/raw/` and are named by date, for example `internal_trades_2026-02-01.csv` and `broker_trades_2026-02-01.csv`.

- **ETL loader**  
  A Python script deletes any existing rows for that date, then uses PostgreSQL’s `COPY` command to bulk-load the CSVs into tables like `internal_trades`, `broker_trades`, `internal_positions`, and `broker_positions`.

- **PostgreSQL database**  
  This is where all “truth” lives for the run: internal vs broker tables, reconciliation tables, and daily PnL.

- **Reconciliation & PnL SQL**  
  A set of SQL scripts compare internal and broker tables, populate reconciliation tables, and compute realized PnL.

- **Orchestration script**  
  A shell script `run_eod_pipeline.sh` runs the entire end-of-day workflow for a chosen date.

- **Outputs**  
  An HTML summary report and CSV files with detailed breaks and PnL rows, plus an optional Streamlit dashboard for interactive charts.

---

## A day in the life of the pipeline

### 1. Internal vs broker files

The day starts with two views of the same trading activity:

- The **internal** system’s view: what “we” think happened.
- The **broker’s** view: what the external counterparty says happened.

In this project, both views are synthetic. The internal files are generated first, and then the broker files are created by copying and intentionally corrupting them: dropping some trades, adding extras, and tweaking prices, quantities, fees, or settlement dates.

This gives the pipeline something realistic to reconcile instead of perfectly clean data.

---

### 2. Loading everything into PostgreSQL

Next, the CSVs are loaded into PostgreSQL. For a given date (like `2026-02-01`):

- Existing rows for that date are deleted from the target tables.
- The new CSVs are bulk-loaded using `COPY`.

This makes the pipeline **idempotent**: if something goes wrong, the same date can be re-run without creating duplicates.

The result is a set of core tables for the day, including:

- `internal_trades` and `broker_trades`
- `internal_positions` and `broker_positions`
- `internal_cash` and `broker_cash`

All later steps work off these tables.

---

### 3. Finding trade breaks

Once both trade tables are in the database, the next question is:  
**“Where do they disagree at the trade level?”**

Examples:

- A trade exists in `internal_trades` but not in `broker_trades` → “missing in broker”.
- A trade exists in both, but the prices differ → “price mismatch”.
- Quantities, fees, or settlement dates don’t match.

The reconciliation logic scans both tables and tags disagreements into categories like:

- `MISSING_IN_BROKER`
- `MISSING_IN_INTERNAL`
- `PRICE_MISMATCH`
- `QTY_MISMATCH`
- `FEE_MISMATCH`
- `SETTLEMENT_MISMATCH`

At the end, a summary table for the day looks like:

```text
BREAK TYPE                | SEVERITY   | COUNT    | TOTAL $         | AVG $
---------------------------------------------------------------------------
MISSING_IN_BROKER         | CRITICAL   | 458      | $526,553,334.79 | $1,149,679.77
MISSING_IN_INTERNAL       | CRITICAL   | 231      | $268,819,780.05 | $1,163,721.99
QTY_MISMATCH              | MEDIUM     | 169      | $   511,357.45 | $3,025.78
PRICE_MISMATCH            | LOW        | 462      | $    33,386.58 | $   72.27
FEE_MISMATCH              | LOW        | 959      | $       479.11 | $    0.50
...
Total Breaks Found: 2893

```

Each break also has a rough severity bucket based on its size (LOW / MEDIUM / HIGH / CRITICAL), to make it easier to see what should be investigated first.

### 4. Rolling up to positions and cash
Even if individual trades disagree, what matters upstream is:

- **Positions** — how many shares of each symbol an account ends up holding.
- **Cash** — how much cash sits in each account and currency.

The project aggregates internal and broker trades into:

`internal_positions` and `broker_positions` (per account + symbol).

`internal_cash` and `broker_cash` (per account + currency).

Then it compares the internal and broker totals for each combination. If the totals don’t match, it creates a position or cash break.

A sample summary looks like:
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

This step shows how trade-level differences propagate up into the totals that risk, treasury, and other teams care about.

### 5. PnL for the day
The last data step is daily realized PnL.
Using only the internal trades, the pipeline calculates realized PnL for each combination of:
- date,
- account,
- strategy,
- symbol.

A simplified view of the result by strategy:

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
Because the data is synthetic, the numbers are mainly there to stress-test the pipeline, but the structure matches how real desks look at PnL by strategy and symbol.

## What the outputs look like
The daily run produces:
- An HTML report for the chosen date, which includes:
    -  A summary of trade breaks by type and severity.
    - Position and cash break summaries.
    - A PnL summary by strategy.

- CSV files:

    - One with detailed trade breaks (one row per break).

    - One with detailed PnL rows (one row per account + strategy + symbol).

These outputs are meant to be the starting point for someone in an operations role:
- Trade breaks help answer “which trades are missing or mismatched?”
- Position and cash breaks help answer “do we agree at the account level?”
- PnL results help answer “what did we actually make or lose today on this data?”

## Running the whole day in one command
On a typical day, everything is driven by a single command:
```bash
./scripts/run_eod_pipeline.sh 2026-02-01
```

Behind the scenes, this runs:

1. ETL (load CSVs into Postgres).
2. Trade reconciliation.
3. Position and cash reconciliation.
4. PnL calculation.
5. Report generation.

Each step is written so that it is safe to re-run for the same date.

If you’re interested in how each script and SQL file is implemented, or want to try running the pipeline locally, the GitHub README has full setup instructions and code snippets.
https://github.com/jessicabat/trade-ops-recon-platform/README.md