import argparse
from sqlalchemy import create_engine, text
from datetime import datetime
import os
import sys

# --- CONFIGURATION ---
DB_CONNECTION_STR = 'postgresql://jessica@localhost:5432/trade_ops_recon'

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQL_PNL_PATH = os.path.join(BASE_DIR, 'sql', 'pnl_calculation.sql')

def run_pnl_calculation(date_str, engine):
    """
    Reads the PnL calculation SQL and executes it.
    """
    print(f"⚙️  Calculating PnL for {date_str}...")
    
    try:
        with open(SQL_PNL_PATH, 'r') as f:
            sql_script = f.read()
    except FileNotFoundError:
        print(f"❌ Error: SQL file not found at {SQL_PNL_PATH}")
        sys.exit(1)

    # Split into Logic vs Summary
    parts = sql_script.split('-- ============================================================================\n-- PNL SUMMARY')
    pnl_logic = parts[0]
    summary_query = parts[1] if len(parts) > 1 else None
    
    with engine.begin() as conn:
        statements = pnl_logic.split(';')
        
        for statement in statements:
            stmt = statement.strip()
            
            if not stmt:
                continue
            
            # Skip comment-only blocks
            lines = [line.strip() for line in stmt.split('\n') if line.strip()]
            if all(line.startswith('--') for line in lines):
                continue
            
            try:
                conn.execute(text(stmt), {"pnl_date": date_str})
            except Exception as e:
                print(f"❌ Error executing statement: {e}")
                print(f"Statement was: {stmt[:200]}...")
                raise
                
    print("✅ PnL Calculation Completed.")
    
    if summary_query:
        with engine.connect() as conn:
            result = conn.execute(text(summary_query), {"pnl_date": date_str})
            return result.fetchall()
    return []

def print_pnl_summary(summary_rows):
    """
    Pretty-prints the PnL summary.
    """
    print("\n💰 DAILY PNL REPORT")
    print("=" * 100)
    
    if not summary_rows:
        print("⚠️  No trades found for PnL calculation.")
        return
    
    print(f"{'STRATEGY':<20} | {'SYMBOLS':<8} | {'TRADES':<8} | {'REALIZED PNL':<15} | {'FEES':<12} | {'NET PNL':<15} | {'AVG/SYMBOL':<12}")
    print("-" * 100)
    
    total_pnl = 0
    total_fees = 0
    total_trades = 0
    
    for row in summary_rows:
        strategy, symbols, trades, realized, fees, net, avg = row
        print(f"{strategy:<20} | {symbols:>8} | {trades:>8} | ${realized:>13,.2f} | ${fees:>10,.2f} | ${net:>13,.2f} | ${avg:>10,.2f}")
        total_pnl += net
        total_fees += fees
        total_trades += trades
    
    print("-" * 100)
    print(f"{'TOTAL':<20} | {'':<8} | {total_trades:>8} | {'':<15} | ${total_fees:>10,.2f} | ${total_pnl:>13,.2f} | {'':<12}")
    print("=" * 100)

def print_top_performers(date_str, engine):
    """
    Show top/bottom performing symbols by PnL.
    """
    print("\n🏆 TOP 5 WINNING SYMBOLS")
    print("=" * 75)
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT symbol, strategy, account, total_pnl, trade_count
            FROM daily_pnl
            WHERE pnl_date = :date
            ORDER BY total_pnl DESC
            LIMIT 5
        """), {"date": date_str})
        
        rows = result.fetchall()
        
        if rows:
            print(f"{'SYMBOL':<10} | {'STRATEGY':<20} | {'ACCOUNT':<15} | {'NET PNL':<15} | {'TRADES':<8}")
            print("-" * 75)
            for row in rows:
                symbol, strategy, account, pnl, trades = row
                print(f"{symbol:<10} | {strategy:<20} | {account:<15} | ${pnl:>13,.2f} | {trades:>8}")
        else:
            print("No data available.")
    
    print("\n📉 TOP 5 LOSING SYMBOLS")
    print("=" * 75)
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT symbol, strategy, account, total_pnl, trade_count
            FROM daily_pnl
            WHERE pnl_date = :date
            ORDER BY total_pnl ASC
            LIMIT 5
        """), {"date": date_str})
        
        rows = result.fetchall()
        
        if rows:
            print(f"{'SYMBOL':<10} | {'STRATEGY':<20} | {'ACCOUNT':<15} | {'NET PNL':<15} | {'TRADES':<8}")
            print("-" * 75)
            for row in rows:
                symbol, strategy, account, pnl, trades = row
                print(f"{symbol:<10} | {strategy:<20} | {account:<15} | ${pnl:>13,.2f} | {trades:>8}")
        else:
            print("No data available.")

def log_pipeline_run(engine, status, start_time, date_str, error=None):
    end_time = datetime.now()
    duration = (end_time - start_time).seconds
    
    sql = text("""
        INSERT INTO pipeline_runs 
        (run_date, pipeline_name, status, start_time, end_time, duration_seconds, error_message)
        VALUES (:run_date, :pipeline_name, :status, :start_time, :end_time, :duration, :error)
    """)
    
    params = {
        "run_date": datetime.strptime(date_str, '%Y-%m-%d').date(),
        "pipeline_name": 'pnl_calculation',
        "status": status,
        "start_time": start_time,
        "end_time": end_time,
        "duration": duration,
        "error": str(error) if error else None
    }

    with engine.connect() as conn:
        conn.execute(sql, params)
        conn.commit()

def main():
    parser = argparse.ArgumentParser(description="Calculate Daily PnL for a specific date.")
    parser.add_argument('--date', type=str, required=False, help="YYYY-MM-DD")
    args = parser.parse_args()
    
    target_date = args.date if args.date else datetime.now().strftime('%Y-%m-%d')
    start_time = datetime.now()
    
    try:
        engine = create_engine(DB_CONNECTION_STR)
        
        # 1. Run PnL Calculation
        summary_rows = run_pnl_calculation(target_date, engine)
        
        # 2. Show Summary
        print_pnl_summary(summary_rows)
        
        # 3. Show Top/Bottom Performers
        print_top_performers(target_date, engine)
        
        # 4. Log Success
        log_pipeline_run(engine, 'SUCCESS', start_time, target_date)
        
        print(f"\n✨ PnL Calculation Complete for {target_date}.")

    except Exception as e:
        print(f"\n❌ PnL Calculation Failed: {e}")
        try:
            log_pipeline_run(engine, 'FAILED', start_time, target_date, error=str(e))
        except:
            pass
        sys.exit(1)

if __name__ == "__main__":
    main()
