import pandas as pd
from sqlalchemy import create_engine, text
import os
import argparse
import sys
from datetime import datetime
import csv
from io import StringIO
print("DEBUG: Script started")

# --- CONFIGURATION ---
DB_CONNECTION_STR = 'postgresql://jessica@localhost:5432/trade_ops_recon'
print(f"DEBUG: Connection string = {DB_CONNECTION_STR}")

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')

FILE_MAPPING = {
    'internal_trades':    ('internal_trades', 'internal_trades'),
    'broker_trades':      ('broker_trades', 'broker_trades'),
    'internal_positions': ('positions', 'internal_positions'),
    'broker_positions':   ('positions', 'broker_positions'),
    'internal_cash':      ('cash', 'internal_cash'),
    'broker_cash':        ('cash', 'broker_cash')
}

def get_file_path(date_str, key):
    subfolder, prefix = FILE_MAPPING[key]
    filename = f"{prefix}_{date_str}.csv"
    return os.path.join(DATA_DIR, subfolder, filename)

# def fast_load_csv(file_path, table_name, engine):
#     """
#     Optimized loader using PostgreSQL 'COPY' for high performance.
#     Explicitly specifies columns so DB can use DEFAULTs for created_at.
#     """
#     if not os.path.exists(file_path):
#         print(f"⚠️  File not found: {file_path}. Skipping {table_name}.")
#         return

#     print(f"Loading {os.path.basename(file_path)} into '{table_name}'...")
    
#     try:
#         df = pd.read_csv(file_path)
        
#         # Create a raw connection for the COPY command
#         conn = engine.raw_connection()
#         cursor = conn.cursor()
        
#         # Use StringIO to simulate a file object in memory
#         output = StringIO()
#         df.to_csv(output, sep='\t', header=False, index=False)
#         output.seek(0)
        
#         # Tell COPY: "Here are the columns I'm providing; use defaults for the rest"
#         columns_csv = ', '.join(df.columns)  # trade_id, trade_date, ..., principal
#         copy_sql = f"COPY {table_name} ({columns_csv}) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t', NULL '')"
        
#         # Use copy_expert instead of copy_from to specify columns
#         cursor.copy_expert(copy_sql, output)
        
#         conn.commit()
#         cursor.close()
        
#         print(f"✅ Fast Loaded {len(df)} rows.")
        
#     except Exception as e:
#         print(f"❌ Error loading {table_name}: {e}")
#         raise e
def fast_load_csv(file_path, table_name, engine, date_str):
    """
    Optimized loader using PostgreSQL 'COPY' for high performance.
    IDEMPOTENT: Deletes existing records for the date before loading.
    """
    if not os.path.exists(file_path):
        print(f"⚠️  File not found: {file_path}. Skipping {table_name}.")
        return

    print(f"Loading {os.path.basename(file_path)} into '{table_name}'...")
    
    try:
        # STEP 1: Delete existing records for this date (IDEMPOTENCY)
        conn = engine.raw_connection()
        cursor = conn.cursor()
        
        # Determine the date column name based on table
        if 'trade' in table_name:
            date_col = 'trade_date'
        elif 'position' in table_name:
            date_col = 'position_date'
        elif 'cash' in table_name:
            date_col = 'cash_date'
        else:
            date_col = None
        
        if date_col:
            delete_sql = f"DELETE FROM {table_name} WHERE {date_col} = '{date_str}'"
            cursor.execute(delete_sql)
            deleted_count = cursor.rowcount
            if deleted_count > 0:
                print(f"  ♻️  Deleted {deleted_count} existing rows for {date_str}")
        
        # STEP 2: Load new data
        df = pd.read_csv(file_path)
        
        # Use StringIO to simulate a file object in memory
        output = StringIO()
        df.to_csv(output, sep='\t', header=False, index=False)
        output.seek(0)
        
        # Tell COPY: "Here are the columns I'm providing; use defaults for the rest"
        columns_csv = ', '.join(df.columns)
        copy_sql = f"COPY {table_name} ({columns_csv}) FROM STDIN WITH (FORMAT TEXT, DELIMITER E'\\t', NULL '')"
        
        cursor.copy_expert(copy_sql, output)
        conn.commit()
        cursor.close()
        
        print(f"✅ Loaded {len(df)} rows.")
        
    except Exception as e:
        print(f"❌ Error loading {table_name}: {e}")
        raise e



def log_pipeline_run(engine, status, start_time, date_str, rows=0, error=None):
    end_time = datetime.now()
    duration = (end_time - start_time).seconds
    
    sql = text("""
        INSERT INTO pipeline_runs 
        (run_date, pipeline_name, status, start_time, end_time, duration_seconds, rows_processed, error_message)
        VALUES (:run_date, :pipeline_name, :status, :start_time, :end_time, :duration, :rows, :error)
    """)
    
    params = {
        "run_date": datetime.strptime(date_str, '%Y-%m-%d').date(),
        "pipeline_name": 'daily_etl_load',
        "status": status,
        "start_time": start_time,
        "end_time": end_time,
        "duration": duration,
        "rows": rows,
        "error": str(error) if error else None
    }

    with engine.connect() as conn:
        conn.execute(sql, params)
        conn.commit()

# def main():
#     parser = argparse.ArgumentParser(description="Load Trade Ops Data for a specific date.")
#     parser.add_argument('--date', type=str, required=False, help="YYYY-MM-DD")
#     args = parser.parse_args()
    
#     target_date = args.date if args.date else datetime.now().strftime('%Y-%m-%d')

#     start_time = datetime.now()
#     print(f"🚀 Starting FAST ETL Pipeline for date: {target_date}")
    
#     try:
#         engine = create_engine(DB_CONNECTION_STR)

#         # Use the new fast_load_csv function
#         fast_load_csv(get_file_path(target_date, 'internal_trades'), 'internal_trades', engine)
#         fast_load_csv(get_file_path(target_date, 'internal_positions'), 'internal_positions', engine)
#         fast_load_csv(get_file_path(target_date, 'internal_cash'), 'internal_cash', engine)
        
#         fast_load_csv(get_file_path(target_date, 'broker_trades'), 'broker_trades', engine)
#         fast_load_csv(get_file_path(target_date, 'broker_positions'), 'broker_positions', engine)
#         fast_load_csv(get_file_path(target_date, 'broker_cash'), 'broker_cash', engine)
        
#         log_pipeline_run(engine, 'SUCCESS', start_time, target_date, rows=50000)
#         print(f"\n✨ ETL Pipeline Finished Successfully for {target_date}.")

#     except Exception as e:
#         print(f"\n❌ Pipeline Failed: {e}")
#         try:
#             log_pipeline_run(engine, 'FAILED', start_time, target_date, error=str(e))
#         except:
#             pass
#         sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Load Trade Ops Data for a specific date.")
    parser.add_argument('--date', type=str, required=False, help="YYYY-MM-DD")
    args = parser.parse_args()
    
    target_date = args.date if args.date else datetime.now().strftime('%Y-%m-%d')

    start_time = datetime.now()
    print(f"🚀 Starting FAST ETL Pipeline for date: {target_date}")
    
    try:
        engine = create_engine(DB_CONNECTION_STR)

        # Use the new fast_load_csv function with date parameter
        fast_load_csv(get_file_path(target_date, 'internal_trades'), 'internal_trades', engine, target_date)
        fast_load_csv(get_file_path(target_date, 'internal_positions'), 'internal_positions', engine, target_date)
        fast_load_csv(get_file_path(target_date, 'internal_cash'), 'internal_cash', engine, target_date)
        
        fast_load_csv(get_file_path(target_date, 'broker_trades'), 'broker_trades', engine, target_date)
        fast_load_csv(get_file_path(target_date, 'broker_positions'), 'broker_positions', engine, target_date)
        fast_load_csv(get_file_path(target_date, 'broker_cash'), 'broker_cash', engine, target_date)
        
        log_pipeline_run(engine, 'SUCCESS', start_time, target_date, rows=50000)
        print(f"\n✨ ETL Pipeline Finished Successfully for {target_date}.")

    except Exception as e:
        print(f"\n❌ Pipeline Failed: {e}")
        try:
            log_pipeline_run(engine, 'FAILED', start_time, target_date, error=str(e))
        except:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()