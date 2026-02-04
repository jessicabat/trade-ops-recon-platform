---
layout: default
title: Trade Operations Reconciliation Platform
---

# Building a T+0 Reconciliation Engine

**Project Status:** Complete | **Stack:** Python, SQL, Bash

Hi, I'm Jessica. I am a Data Science senior at UCSD.

I kept seeing "Trade Support" and "Operations" roles asking for SQL and Python skills, but I wanted to understand what that work *actually* looks like. Instead of just reading about it, I decided to build a simulation of a bank's Middle Office on my laptop.

This project simulates a daily workflow: generating 50,000 synthetic trades, breaking them on purpose, and writing the code to catch the errors before they become financial losses.

[View the Code on GitHub →](YOUR_GITHUB_LINK_HERE)

---

## 1. The Challenge: Finding the Missing Money

In trading, an "Internal System" (what we think happened) and a "Broker" (what actually happened) often disagree. My goal was to build a pipeline that answers three questions every single day:

1.  **Execution Risk:** Did we book a trade that the broker doesn't have?
2.  **Position Risk:** Do our net holdings match the broker's records?
3.  **Performance:** What is our daily PnL based on the verified data?

---

## 2. The Architecture

I designed an End-of-Day (EOD) pipeline that runs automatically. It flows from Python (Generation) to Postgres (Storage) to SQL (Logic).

![System Architecture](assets/system_architecture.png)

> **Engineering Decision: Idempotency**
> I learned that in Ops, scripts fail. I designed this pipeline so I can re-run the same date (e.g., `2026-02-01`) ten times in a row, and the result is always correct. It cleans up old data before loading new files.

---

## 3. Creating the "Mess" (Data Generation)

Real data isn't clean. To make this simulation useful, I wrote a Python script to generate "Golden Source" internal logs, and then create a corrupt "Broker" copy.

I injected specific errors to test my logic:
* **Phantom Trades:** Records that exist at the broker but not internally.
* **Fat Finger Errors:** Random 1% deviations in price or quantity.
* **Settlement Drifts:** Changing T+1 to T+2 settlement dates.

---

## 4. The Logic (SQL Reconciliation)

This was the most important part of the project. I learned that standard joins aren't enough—you need **Anti-Joins** to find what *isn't* there.

Here is the SQL logic I wrote to catch the most dangerous break type: **Missing Internal Trades.**

```sql
/* Finds trades the broker has, but we don't */
INSERT INTO recon_trades (break_type, severity, ...)
SELECT 
    'MISSING_IN_INTERNAL',
    'CRITICAL', -- High Risk
    ...
FROM broker_trades b
LEFT JOIN internal_trades i ON b.trade_id = i.trade_id
WHERE b.trade_date = :recon_date 
  AND i.trade_id IS NULL; -- The Anti-Join
```

## 5. The Verdict (Outputs)
After processing 50,000 trades, the pipeline generates a report for the Operations team.

Trade-Level Breaks
The system flagged **2,893** discrepancies. By categorizing them, I can see that "Critical" breaks (Missing Trades) account for the majority of the risk, while "Fee Mismatches" are noise.

```text
BREAK TYPE                | SEVERITY   | COUNT    | TOTAL $ IMPACT
------------------------------------------------------------------
MISSING_IN_BROKER         | CRITICAL   | 458      | $526,553,334
MISSING_IN_INTERNAL       | CRITICAL   | 231      | $268,819,780
QTY_MISMATCH              | MEDIUM     | 169      | $    511,357
FEE_MISMATCH              | LOW        | 959      | $        479
PnL Attribution
```
Finally, the system calculates the Realized PnL for the day using the verified internal trades, grouping by strategy.

```text
STRATEGY             | TRADES   | NET PNL
------------------------------------------------------
DeltaNeutral         | 12,465   | $ 31,227,063.86
StatisticalArb       | 12,568   | $-120,430,990.46
LiquidityProv        | 12,517   | $-149,715,692.42
MarketMaking         | 12,450   | $-162,748,424.43
```

## 6. How to Run It
I wrapped the entire process in a single shell script for easy execution.

Bash
# From the project root
./scripts/run_eod_pipeline.sh 2026-02-01
This runs the ETL loader (using Postgres `COPY` for speed), executes the SQL logic, and generates the HTML reports found in `data/processed/`.

Check out the full repository on GitHub →