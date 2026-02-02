import argparse
from sqlalchemy import create_engine, text
from datetime import datetime
import os
import sys
import pandas as pd

# --- CONFIGURATION ---
DB_CONNECTION_STR = 'postgresql://jessica@localhost:5432/trade_ops_recon'

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SQL_FILE_PATH = os.path.join(BASE_DIR, 'sql', 'recon_trades.sql')

def run_recon(date_str, engine):
    """
    Reads the SQL logic file and executes it against the database
    for the specified date.
    """
    print(f"⚙️  Running Trade Reconciliation for {date_str}...")
    
    try:
        with open(SQL_FILE_PATH, 'r') as f:
            sql_script = f.read()
    except FileNotFoundError:
        print(f"❌ Error: SQL file not found at {SQL_FILE_PATH}")
        sys.exit(1)

    # Split into Logic vs Summary
    parts = sql_script.split('-- ============================================================================\n-- RECONCILIATION SUMMARY')
    recon_logic = parts[0]
    summary_query = parts[1] if len(parts) > 1 else None
    
    with engine.begin() as conn:
        statements = recon_logic.split(';')
        
        for statement in statements:
            # Clean up whitespace
            stmt = statement.strip()
            
            # Skip if:
            # - Empty
            # - Only comments (all lines start with --)
            # - Only whitespace/newlines
            if not stmt:
                continue
            
            # Check if it's only comments (every non-empty line starts with --)
            lines = [line.strip() for line in stmt.split('\n') if line.strip()]
            if all(line.startswith('--') for line in lines):
                continue
            
            # Execute valid SQL
            try:
                conn.execute(text(stmt), {"recon_date": date_str})
            except Exception as e:
                print(f"❌ Error executing statement: {e}")
                print(f"Statement was: {stmt[:200]}...")  # Show first 200 chars for debugging
                raise
                
    print("✅ Reconciliation Logic Completed.")
    
    if summary_query:
        with engine.connect() as conn:
            result = conn.execute(text(summary_query), {"recon_date": date_str})
            return result.fetchall()
    return []


def print_summary(summary_rows):
    """
    Pretty-prints the reconciliation summary returned by the SQL query.
    """
    print("\n📊 RECONCILIATION REPORT")
    print("=" * 75)
    
    if not summary_rows:
        print("✨ Perfect Match! No breaks found.")
        return 0
    
    print(f"{'BREAK TYPE':<25} | {'SEVERITY':<10} | {'COUNT':<8} | {'TOTAL $':<15} | {'AVG $':<10}")
    print("-" * 75)
    
    total_breaks = 0
    for row in summary_rows:
        break_type, severity, count, total_notional, avg_notional = row
        print(f"{break_type:<25} | {severity:<10} | {count:<8} | ${total_notional:>13,.2f} | ${avg_notional:>8,.2f}")
        total_breaks += count
    
    print("-" * 75)
    print(f"Total Breaks Found: {total_breaks}")
    return total_breaks

def export_breaks_csv(date_str, engine):
    """Export detailed breaks to CSV for investigation."""    
    with engine.connect() as conn:
        query = text("""
            SELECT recon_date, trade_id, symbol, account, break_type, severity, 
                   internal_value, broker_value, notional_impact, resolved
            FROM recon_trades
            WHERE recon_date = :date
            ORDER BY severity, notional_impact DESC
        """)
        df = pd.read_sql(query, conn, params={"date": date_str})
    
    output_path = f"data/processed/recon_reports/breaks_{date_str}.csv"
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    df.to_csv(output_path, index=False)
    print(f"📄 Detailed breaks exported to {output_path}")


def log_pipeline_run(engine, status, start_time, date_str, breaks=0, error=None):
    end_time = datetime.now()
    duration = (end_time - start_time).seconds
    
    sql = text("""
        INSERT INTO pipeline_runs 
        (run_date, pipeline_name, status, start_time, end_time, duration_seconds, breaks_found, error_message)
        VALUES (:run_date, :pipeline_name, :status, :start_time, :end_time, :duration, :breaks, :error)
    """)
    
    params = {
        "run_date": datetime.strptime(date_str, '%Y-%m-%d').date(),
        "pipeline_name": 'trade_reconciliation',
        "status": status,
        "start_time": start_time,
        "end_time": end_time,
        "duration": duration,
        "breaks": breaks,
        "error": str(error) if error else None
    }

    with engine.connect() as conn:
        conn.execute(sql, params)
        conn.commit()

def main():
    parser = argparse.ArgumentParser(description="Run Trade Reconciliation for a specific date.")
    parser.add_argument('--date', type=str, required=False, help="YYYY-MM-DD")
    args = parser.parse_args()
    
    target_date = args.date if args.date else datetime.now().strftime('%Y-%m-%d')
    start_time = datetime.now()
    
    try:
        engine = create_engine(DB_CONNECTION_STR)
        
        # Run the Logic and get summary
        summary_rows = run_recon(target_date, engine)
        
        # Show Results
        total_breaks = print_summary(summary_rows)

        # Export detailed breaks to CSV
        if total_breaks > 0:
            export_breaks_csv(target_date, engine)
        
        # Log Success
        log_pipeline_run(engine, 'SUCCESS', start_time, target_date, breaks=total_breaks)
        
        print(f"\n✨ Reconciliation Complete for {target_date}. Check recon_trades table for details.")

    except Exception as e:
        print(f"\n❌ Reconciliation Failed: {e}")
        try:
            log_pipeline_run(engine, 'FAILED', start_time, target_date, error=str(e))
        except:
            pass
        sys.exit(1)

if __name__ == "__main__":
    main()

def print_top_breaks(date_str, engine):
    """Show the top 10 most critical breaks for immediate attention."""
    print("\n🚨 TOP 10 CRITICAL BREAKS")
    print("=" * 75)
    
    with engine.connect() as conn:
        result = conn.execute(text("""
            SELECT trade_id, break_type, symbol, notional_impact
            FROM recon_trades
            WHERE recon_date = :date AND severity IN ('CRITICAL', 'HIGH')
            ORDER BY notional_impact DESC
            LIMIT 10
        """), {"date": date_str})
        
        rows = result.fetchall()
        for row in rows:
            trade_id, break_type, symbol, notional = row
            print(f"{trade_id:<15} | {break_type:<25} | {symbol:<8} | ${notional:>12,.2f}")
