import pandas as pd
import numpy as np
import os
import argparse
from datetime import datetime, timedelta

# --- Configuration ---
NUM_TRADES = 50000
SYMBOLS = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'SPY', 'QQQ', 'NVDA', 'AMD', 'INTC']
ACCOUNTS = ['ACCT_MAIN', 'ACCT_HEDGE', 'ACCT_ARB', 'ACCT_FLOW']
STRATEGIES = ['MarketMaking', 'StatisticalArb', 'LiquidityProv', 'DeltaNeutral']
VENUES = ['NASDAQ', 'NYSE', 'ARCA', 'BATS', 'IEX']

# Get the absolute path of the project root
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, 'data', 'raw')

PATHS = {
    'internal_trades': os.path.join(DATA_DIR, 'internal_trades'),
    'broker_trades': os.path.join(DATA_DIR, 'broker_trades'),
    'internal_positions': os.path.join(DATA_DIR, 'positions'),
    'broker_positions': os.path.join(DATA_DIR, 'positions'),
    'internal_cash': os.path.join(DATA_DIR, 'cash'),
    'broker_cash': os.path.join(DATA_DIR, 'cash')
}

def setup_directories():
    print(f"📂 Creating directories in {DATA_DIR}...")
    for path in PATHS.values():
        os.makedirs(path, exist_ok=True)
    print("✓ Directories verified.")

def generate_internal_trades(n=1000, target_date=None):
    if target_date is None:
        target_date = datetime.now().date()
        
    print(f"Generating {n} internal trades for {target_date}...")
    
    trade_ids = [f"TRD_{i:07d}" for i in range(1, n + 1)]
    
    # UPDATED: Use target_date instead of datetime.now()
    dates = [target_date] * n
    
    # Settlement logic: random 1 or 2 days from the target date
    settle_deltas = np.random.choice([1, 2], n)
    settle_dates = [d + timedelta(days=int(delta)) for d, delta in zip(dates, settle_deltas)]
    
    data = {
        'trade_id': trade_ids,
        'trade_date': dates,
        'settlement_date': settle_dates,
        'symbol': np.random.choice(SYMBOLS, n),
        'account': np.random.choice(ACCOUNTS, n),
        'strategy': np.random.choice(STRATEGIES, n),
        'venue': np.random.choice(VENUES, n),
        'side': np.random.choice(['BUY', 'SELL'], n),
        'quantity': np.random.randint(10, 5000, n),
        'price': np.round(np.random.uniform(50, 800, n), 2),
        'fees': np.round(np.random.uniform(0.50, 15.00, n), 2),
        'currency': ['USD'] * n
    }
    
    df = pd.DataFrame(data)
    
    # Net Amount Calculation
    df['principal'] = df.apply(lambda x: 
                                -1 * ((x['quantity'] * x['price']) + x['fees']) if x['side'] == 'BUY' 
                                else ((x['quantity'] * x['price']) - x['fees']), axis=1)
    return df

def corrupt_broker_data(internal_df):
    print("Corrupting data to create Broker records...")
    broker_df = internal_df.copy()
    
    # 1. Price Mismatches (1%)
    mask_price = np.random.choice([True, False], size=len(broker_df), p=[0.01, 0.99])
    broker_df.loc[mask_price, 'price'] += np.random.uniform(-0.05, 0.05, sum(mask_price))
    broker_df['price'] = broker_df['price'].round(2)

    # 2. Quantity Mismatches (0.5%)
    mask_qty = np.random.choice([True, False], size=len(broker_df), p=[0.005, 0.995])
    broker_df.loc[mask_qty, 'quantity'] += np.random.randint(-10, 10, sum(mask_qty))
    
    # 3. Fee Mismatches (2%)
    mask_fee = np.random.choice([True, False], size=len(broker_df), p=[0.02, 0.98])
    broker_df.loc[mask_fee, 'fees'] += np.random.uniform(-1.00, 1.00, sum(mask_fee))
    broker_df['fees'] = broker_df['fees'].round(2)

    # 4. Settlement Date Mismatches (1%)
    mask_settle = np.random.choice([True, False], size=len(broker_df), p=[0.01, 0.99])
    broker_df['settlement_date'] = pd.to_datetime(broker_df['settlement_date'])
    random_days = np.random.choice([-1, 1], sum(mask_settle))
    broker_df.loc[mask_settle, 'settlement_date'] += pd.to_timedelta(random_days, unit='D')
    broker_df['settlement_date'] = broker_df['settlement_date'].dt.date

    # 5. Missing Trades (1%)
    drop_indices = np.random.choice(broker_df.index, size=int(len(broker_df) * 0.01), replace=False)
    broker_df = broker_df.drop(drop_indices)
    
    # 6. Phantom Trades (0.5%)
    n_phantom = int(len(internal_df) * 0.005)
    # Important: Pass the same trade date to phantom trades
    target_date = internal_df['trade_date'].iloc[0]
    phantom_df = generate_internal_trades(n_phantom, target_date)
    phantom_df['trade_id'] = [f"BRK_ONLY_{i}" for i in range(n_phantom)]
    broker_df = pd.concat([broker_df, phantom_df])
    
    # Recalculate Net Amount
    broker_df['principal'] = broker_df.apply(lambda x: 
                                -1 * ((x['quantity'] * x['price']) + x['fees']) if x['side'] == 'BUY' 
                                else ((x['quantity'] * x['price']) - x['fees']), axis=1)
    return broker_df

def aggregate_positions(df, target_date):
    df_copy = df.copy()
    df_copy['signed_qty'] = df_copy.apply(lambda x: x['quantity'] if x['side'] == 'BUY' else -x['quantity'], axis=1)
    pos_df = df_copy.groupby(['account', 'symbol'])['signed_qty'].sum().reset_index()
    pos_df.rename(columns={'signed_qty': 'net_position'}, inplace=True)
    # UPDATED: Use target_date
    pos_df['position_date'] = target_date
    return pos_df

def aggregate_cash(df, target_date):
    cash_df = df.groupby(['account', 'currency'])['principal'].sum().reset_index()
    cash_df.rename(columns={'principal': 'net_cash_balance'}, inplace=True)
    # UPDATED: Use target_date
    cash_df['cash_date'] = target_date
    return cash_df

def main():
    # UPDATED: Add Argument Parser
    parser = argparse.ArgumentParser(description="Generate Trade Ops Data.")
    parser.add_argument('--date', type=str, required=False, help="YYYY-MM-DD format")
    args = parser.parse_args()

    # Determine date (Argument -> Today)
    if args.date:
        target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
    else:
        target_date = datetime.now().date()

    setup_directories()
    
    # Pass target_date to generator
    internal_df = generate_internal_trades(NUM_TRADES, target_date)
    broker_df = corrupt_broker_data(internal_df)
    
    # Pass target_date to aggregators
    internal_pos = aggregate_positions(internal_df, target_date)
    broker_pos = aggregate_positions(broker_df, target_date)
    
    internal_cash = aggregate_cash(internal_df, target_date)
    broker_cash = aggregate_cash(broker_df, target_date)
    
    # Use target_date for filename
    date_str = target_date.strftime('%Y-%m-%d')
    
    print(f"💾 Saving to CSV for date {date_str}...")
    internal_df.to_csv(f"{PATHS['internal_trades']}/internal_trades_{date_str}.csv", index=False)
    broker_df.to_csv(f"{PATHS['broker_trades']}/broker_trades_{date_str}.csv", index=False)
    
    internal_pos.to_csv(f"{PATHS['internal_positions']}/internal_positions_{date_str}.csv", index=False)
    broker_pos.to_csv(f"{PATHS['broker_positions']}/broker_positions_{date_str}.csv", index=False)
    
    internal_cash.to_csv(f"{PATHS['internal_cash']}/internal_cash_{date_str}.csv", index=False)
    broker_cash.to_csv(f"{PATHS['broker_cash']}/broker_cash_{date_str}.csv", index=False)
    
    print("\n✓ Data Generation Complete.")
    print(f"  Internal Trades: {len(internal_df)}")
    print(f"  Broker Trades:   {len(broker_df)}")
    print(f"  Location: {DATA_DIR}")

if __name__ == "__main__":
    main()