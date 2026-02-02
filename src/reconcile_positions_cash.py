import argparse
from sqlalchemy import create_engine, text
from datetime import datetime
import os
import sys

# --- CONFIGURATION ---
DB_CONNECTION_STR = 'postgresql://jessica@localhost:5432/trade_ops_recon'

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQL_POSITIONS_PATH = os.path.join(BASE_DIR, 'sql', 'recon_positions.sql')
SQL_CASH_PATH = os.path.join(BASE_DIR, 'sql', 'recon_cash.sql')

def run_recon_sql(sql_file_path, date_str, engine, recon_type):
    """
    Generic function to run a reconciliation SQL file.
    """
    print(f"⚙️  Running {recon_type} Reconciliation for {date_str}...")
    
    try:
        with open(sql_file_path, 'r') as f:
            sql_script = f.read()
    except FileNotFoundError:
        print(f"❌ Error: SQL file not found at {sql_file_path}")
        sys.exit(1)

    # Split into Logic vs Summary
    if 'RECONCILIATION SUMMARY' in sql_script:
        parts = sql_script.split('-- ============================================================================\n-- ' + recon_type.upper() + ' RECONCILIATION SUMMARY')
        if len(parts) == 1:
            # Try alternate split pattern
            parts = sql_script.split('-- RECONCILIATION SUMMARY')
        recon_logic = parts[0]
        summary_query = parts[1] if len(parts) > 1 else None
    else:
        recon_logic = sql_script
        summary_query = None
    
    with engine.begin() as conn:
        statements = recon_logic.split(';')
        
        for statement in statements:
            stmt = statement.strip()
            
            if not stmt:
                continue
            
            # Skip comment-only blocks
            lines = [line.strip() for line in stmt.split('\n') if line.strip()]
            if all(line.startswith('--') for line in lines):
                continue
            
            try:
                conn.execute(text(stmt), {"recon_date": date_str})
            except Exception as e:
                print(f"❌ Error executing statement: {e}")
                print(f"Statement was: {stmt[:200]}...")
                raise
                
    print(f"✅ {recon_type} Reconciliation Logic Completed.")
    
    if summary_query:
        with engine.connect() as conn:
            result = conn.execute(text(summary_query), {"recon_date": date_str})
            return result.fetchall()
    return []

def print_summary(summary_rows, recon_type):
    """
    Pretty-prints the reconciliation summary.
    """
    print(f"\n📊 {recon_type.upper()} RECONCILIATION REPORT")
    print("=" * 75)
    
    if not summary_rows:
        print("✨ Perfect Match! No breaks found.")
        return 0
    
    if recon_type == "Position":
        print(f"{'BREAK TYPE':<25} | {'SEVERITY':<10} | {'COUNT':<8} | {'TOTAL DIFF':<12} | {'AVG DIFF':<10}")
    else:  # Cash
        print(f"{'BREAK TYPE':<25} | {'SEVERITY':<10} | {'COUNT':<8} | {'TOTAL $ DIFF':<15} | {'AVG $ DIFF':<12}")
    
    print("-" * 75)
    
    total_breaks = 0
    for row in summary_rows:
        break_type, severity, count, total_diff, avg_diff = row
        if recon_type == "Position":
            print(f"{break_type:<25} | {severity:<10} | {count:<8} | {total_diff:>12,.0f} | {avg_diff:>10,.2f}")
        else:
            print(f"{break_type:<25} | {severity:<10} | {count:<8} | ${total_diff:>14,.2f} | ${avg_diff:>11,.2f}")
        total_breaks += count
    
    print("-" * 75)
    print(f"Total Breaks Found: {total_breaks}")
    return total_breaks

def log_pipeline_run(engine, status, start_time, date_str, breaks_pos=0, breaks_cash=0, error=None):
    end_time = datetime.now()
    duration = (end_time - start_time).seconds
    
    sql = text("""
        INSERT INTO pipeline_runs 
        (run_date, pipeline_name, status, start_time, end_time, duration_seconds, breaks_found, error_message)
        VALUES (:run_date, :pipeline_name, :status, :start_time, :end_time, :duration, :breaks, :error)
    """)
    
    params = {
        "run_date": datetime.strptime(date_str, '%Y-%m-%d').date(),
        "pipeline_name": 'position_cash_reconciliation',
        "status": status,
        "start_time": start_time,
        "end_time": end_time,
        "duration": duration,
        "breaks": breaks_pos + breaks_cash,
        "error": str(error) if error else None
    }

    with engine.connect() as conn:
        conn.execute(sql, params)
        conn.commit()

def main():
    parser = argparse.ArgumentParser(description="Run Position & Cash Reconciliation for a specific date.")
    parser.add_argument('--date', type=str, required=False, help="YYYY-MM-DD")
    args = parser.parse_args()
    
    target_date = args.date if args.date else datetime.now().strftime('%Y-%m-%d')
    start_time = datetime.now()
    
    try:
        engine = create_engine(DB_CONNECTION_STR)
        
        # 1. Run Position Reconciliation
        position_summary = run_recon_sql(SQL_POSITIONS_PATH, target_date, engine, "Position")
        breaks_pos = print_summary(position_summary, "Position")
        
        # 2. Run Cash Reconciliation
        cash_summary = run_recon_sql(SQL_CASH_PATH, target_date, engine, "Cash")
        breaks_cash = print_summary(cash_summary, "Cash")
        
        # 3. Log Success
        log_pipeline_run(engine, 'SUCCESS', start_time, target_date, breaks_pos, breaks_cash)
        
        print(f"\n✨ Position & Cash Reconciliation Complete for {target_date}.")

    except Exception as e:
        print(f"\n❌ Reconciliation Failed: {e}")
        try:
            log_pipeline_run(engine, 'FAILED', start_time, target_date, error=str(e))
        except:
            pass
        sys.exit(1)

if __name__ == "__main__":
    main()
