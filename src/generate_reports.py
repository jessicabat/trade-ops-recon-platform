import argparse
from sqlalchemy import create_engine, text
from datetime import datetime
import os
import pandas as pd

# --- CONFIGURATION ---
DB_CONNECTION_STR = 'postgresql://jessica@localhost:5432/trade_ops_recon'

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
REPORT_DIR = os.path.join(BASE_DIR, 'data', 'processed', 'eod_reports')

def generate_summary_report(date_str, engine):
    """
    Generate an HTML executive summary of the day's reconciliation.
    """
    print(f"📄 Generating Executive Summary for {date_str}...")
    
    # Create report directory
    os.makedirs(REPORT_DIR, exist_ok=True)
    
    with engine.connect() as conn:
        # Get break counts
        trade_breaks = conn.execute(text("""
            SELECT break_type, severity, COUNT(*), SUM(notional_impact)
            FROM recon_trades WHERE recon_date = :d
            GROUP BY break_type, severity
        """), {"d": date_str}).fetchall()
        
        pos_breaks = conn.execute(text("""
            SELECT break_type, COUNT(*), SUM(ABS(position_difference))
            FROM recon_positions WHERE recon_date = :d
            GROUP BY break_type
        """), {"d": date_str}).fetchall()
        
        cash_breaks = conn.execute(text("""
            SELECT break_type, COUNT(*), SUM(ABS(cash_difference))
            FROM recon_cash WHERE recon_date = :d
            GROUP BY break_type
        """), {"d": date_str}).fetchall()
        
        # Get PnL summary
        pnl_summary = conn.execute(text("""
            SELECT strategy, SUM(total_pnl), SUM(fees_total), SUM(trade_count)
            FROM daily_pnl WHERE pnl_date = :d
            GROUP BY strategy ORDER BY SUM(total_pnl) DESC
        """), {"d": date_str}).fetchall()
    
    # Build HTML
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
        <title>EOD Report - {date_str}</title>
        <style>
            body {{ font-family: Arial, sans-serif; margin: 40px; background: #f5f5f5; }}
            h1 {{ color: #2c3e50; border-bottom: 3px solid #3498db; padding-bottom: 10px; }}
            h2 {{ color: #34495e; margin-top: 30px; }}
            table {{ border-collapse: collapse; width: 100%; margin: 20px 0; background: white; }}
            th {{ background: #3498db; color: white; padding: 12px; text-align: left; }}
            td {{ padding: 10px; border-bottom: 1px solid #ddd; }}
            tr:hover {{ background: #f1f1f1; }}
            .critical {{ color: #e74c3c; font-weight: bold; }}
            .high {{ color: #e67e22; }}
            .medium {{ color: #f39c12; }}
            .summary {{ background: #ecf0f1; padding: 20px; border-radius: 5px; margin: 20px 0; }}
            .positive {{ color: #27ae60; }}
            .negative {{ color: #e74c3c; }}
        </style>
    </head>
    <body>
        <h1>📊 End-of-Day Reconciliation Report</h1>
        <div class="summary">
            <strong>Date:</strong> {date_str}<br>
            <strong>Generated:</strong> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}<br>
        </div>
        
        <h2>🔍 Trade Reconciliation</h2>
        <table>
            <tr><th>Break Type</th><th>Severity</th><th>Count</th><th>Notional Impact</th></tr>
    """
    
    for break_type, severity, count, notional in trade_breaks:
        severity_class = severity.lower() if severity else 'low'
        html += f"<tr><td>{break_type}</td><td class='{severity_class}'>{severity}</td><td>{count}</td><td>${notional:,.2f}</td></tr>"
    
    html += "</table><h2>💼 Position Reconciliation</h2><table><tr><th>Break Type</th><th>Count</th><th>Total Position Diff</th></tr>"
    
    for break_type, count, diff in pos_breaks:
        html += f"<tr><td>{break_type}</td><td>{count}</td><td>{diff:,.0f}</td></tr>"
    
    html += "</table><h2>💰 Cash Reconciliation</h2><table><tr><th>Break Type</th><th>Count</th><th>Total Cash Diff</th></tr>"
    
    for break_type, count, diff in cash_breaks:
        html += f"<tr><td>{break_type}</td><td>{count}</td><td>${diff:,.2f}</td></tr>"
    
    html += "</table><h2>📈 PnL Summary</h2><table><tr><th>Strategy</th><th>Net PnL</th><th>Fees</th><th>Trades</th></tr>"
    
    for strategy, pnl, fees, trades in pnl_summary:
        pnl_class = 'positive' if pnl > 0 else 'negative'
        html += f"<tr><td>{strategy}</td><td class='{pnl_class}'>${pnl:,.2f}</td><td>${fees:,.2f}</td><td>{trades}</td></tr>"
    
    html += """
        </table>
    </body>
    </html>
    """
    
    # Save report
    report_path = os.path.join(REPORT_DIR, f'eod_summary_{date_str}.html')
    with open(report_path, 'w') as f:
        f.write(html)
    
    print(f"✅ HTML Report: {report_path}")
    return report_path

def export_detailed_csvs(date_str, engine):
    """
    Export detailed CSVs for investigation.
    """
    print(f"📁 Exporting Detailed CSVs for {date_str}...")
    
    with engine.connect() as conn:
        # Export unresolved trade breaks
        trades_df = pd.read_sql(text("""
            SELECT * FROM recon_trades 
            WHERE recon_date = :d AND resolved = FALSE
            ORDER BY severity, notional_impact DESC
        """), conn, params={"d": date_str})
        
        if not trades_df.empty:
            csv_path = os.path.join(REPORT_DIR, f'trade_breaks_{date_str}.csv')
            trades_df.to_csv(csv_path, index=False)
            print(f"  ✓ Trade breaks: {csv_path}")
        
        # Export PnL detail
        pnl_df = pd.read_sql(text("""
            SELECT * FROM daily_pnl 
            WHERE pnl_date = :d
            ORDER BY total_pnl DESC
        """), conn, params={"d": date_str})
        
        if not pnl_df.empty:
            csv_path = os.path.join(REPORT_DIR, f'pnl_detail_{date_str}.csv')
            pnl_df.to_csv(csv_path, index=False)
            print(f"  ✓ PnL detail: {csv_path}")

def main():
    parser = argparse.ArgumentParser(description="Generate EOD Reports for a specific date.")
    parser.add_argument('--date', type=str, required=False, help="YYYY-MM-DD")
    args = parser.parse_args()
    
    target_date = args.date if args.date else datetime.now().strftime('%Y-%m-%d')
    
    try:
        engine = create_engine(DB_CONNECTION_STR)
        
        # Generate reports
        report_path = generate_summary_report(target_date, engine)
        export_detailed_csvs(target_date, engine)
        
        print(f"\n✨ Reports Generated Successfully for {target_date}")
        print(f"\n📂 View HTML report: open {report_path}")

    except Exception as e:
        print(f"\n❌ Report Generation Failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()
