#!/bin/bash
# ============================================================================
# END-OF-DAY RECONCILIATION PIPELINE
# ============================================================================
# Purpose: Orchestrate the full daily reconciliation workflow
# Usage: ./scripts/run_eod_pipeline.sh [YYYY-MM-DD]
#        If no date provided, defaults to today
# ============================================================================

set -e  # Exit on any error

# --- CONFIGURATION ---
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$PROJECT_DIR"

# Activate virtual environment
source .venv/bin/activate

# Get target date (default to today if not provided)
TARGET_DATE="${1:-$(date +%Y-%m-%d)}"

echo "============================================================================"
echo "🚀 STARTING EOD PIPELINE FOR $TARGET_DATE"
echo "============================================================================"
echo ""

# --- STEP 1: DATA LOAD ---
echo "📦 STEP 1/5: Loading Data..."
python src/load_to_db.py --date "$TARGET_DATE"
if [ $? -ne 0 ]; then
    echo "❌ Data load failed. Aborting pipeline."
    exit 1
fi
echo ""

# --- STEP 2: TRADE RECONCILIATION ---
echo "🔍 STEP 2/5: Trade Reconciliation..."
python src/reconcile_trades.py --date "$TARGET_DATE"
if [ $? -ne 0 ]; then
    echo "❌ Trade reconciliation failed. Aborting pipeline."
    exit 1
fi
echo ""

# --- STEP 3: POSITION & CASH RECONCILIATION ---
echo "💼 STEP 3/5: Position & Cash Reconciliation..."
python src/reconcile_positions_cash.py --date "$TARGET_DATE"
if [ $? -ne 0 ]; then
    echo "❌ Position/Cash reconciliation failed. Aborting pipeline."
    exit 1
fi
echo ""

# --- STEP 4: PNL CALCULATION ---
echo "💰 STEP 4/5: PnL Calculation..."
python src/calculate_pnl.py --date "$TARGET_DATE"
if [ $? -ne 0 ]; then
    echo "❌ PnL calculation failed. Aborting pipeline."
    exit 1
fi
echo ""

# --- STEP 5: GENERATE REPORTS ---
echo "📊 STEP 5/5: Generating Reports..."
python src/generate_reports.py --date "$TARGET_DATE"
if [ $? -ne 0 ]; then
    echo "❌ Report generation failed. Aborting pipeline."
    exit 1
fi
echo ""

# --- SUCCESS ---
echo "============================================================================"
echo "✅ EOD PIPELINE COMPLETE FOR $TARGET_DATE"
echo "============================================================================"
echo ""
echo "📁 Reports saved to: data/processed/eod_reports/"
echo "🗄️  Database updated with reconciliation and PnL data"
echo ""
