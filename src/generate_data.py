import pandas as pd
import numpy as np
import os
from datetime import datetime, timedelta

# --- Configuration ---
NUM_TRADES = 50000
SYMBOLS = ['AAPL', 'GOOGL', 'MSFT', 'AMZN', 'TSLA', 'SPY', 'QQQ', 'NVDA', 'AMD', 'INTC']
ACCOUNTS = ['ACCT_MAIN', 'ACCT_HEDGE', 'ACCT_ARB', 'ACCT_FLOW']
STRATEGIES = ['MarketMaking', 'StatisticalArb', 'LiquidityProv', 'DeltaNeutral']
VENUES = ['NASDAQ', 'NYSE', 'ARCA', 'BATS', 'IEX']

# Get the absolute path of the project root to avoid "No such file" errors
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

def generate_internal_trades(n=1000):
    print(f"Generating {n} internal trades...")
    
    # Generate data using native Python types where possible to avoid Pandas warnings
    trade_ids = [f"TRD_{i:07d}" for i in range(1, n + 1)]
    dates = [datetime.now().date()] * n
    
    # Settlement logic: random 1 or 2 days
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

    # 4. Settlement Date Mismatches (1%) - Fixed the Warning here
    mask_settle = np.random.choice([True, False], size=len(broker_df), p=[0.01, 0.99])
    # Convert column to datetime first to be safe
    broker_df['settlement_date'] = pd.to_datetime(broker_df['settlement_date'])
    # Add random timedelta
    random_days = np.random.choice([-1, 1], sum(mask_settle))
    broker_df.loc[mask_settle, 'settlement_date'] += pd.to_timedelta(random_days, unit='D')
    # Convert back to date object for clean CSV
    broker_df['settlement_date'] = broker_df['settlement_date'].dt.date

    # 5. Missing Trades (1%)
    drop_indices = np.random.choice(broker_df.index, size=int(len(broker_df) * 0.01), replace=False)
    broker_df = broker_df.drop(drop_indices)
    
    # 6. Phantom Trades (0.5%)
    n_phantom = int(len(internal_df) * 0.005)
    phantom_df = generate_internal_trades(n_phantom)
    phantom_df['trade_id'] = [f"BRK_ONLY_{i}" for i in range(n_phantom)]
    broker_df = pd.concat([broker_df, phantom_df])
    
    # Recalculate Net Amount
    broker_df['principal'] = broker_df.apply(lambda x: 
                                -1 * ((x['quantity'] * x['price']) + x['fees']) if x['side'] == 'BUY' 
                                else ((x['quantity'] * x['price']) - x['fees']), axis=1)
    return broker_df

def aggregate_positions(df):
    # Work on a copy to avoid modifying the original trades dataframe
    df_copy = df.copy()
    df_copy['signed_qty'] = df_copy.apply(lambda x: x['quantity'] if x['side'] == 'BUY' else -x['quantity'], axis=1)
    pos_df = df_copy.groupby(['account', 'symbol'])['signed_qty'].sum().reset_index()
    pos_df.rename(columns={'signed_qty': 'net_position'}, inplace=True)
    pos_df['position_date'] = datetime.now().date()
    return pos_df



def aggregate_cash(df):
    cash_df = df.groupby(['account', 'currency'])['principal'].sum().reset_index()
    cash_df.rename(columns={'principal': 'net_cash_balance'}, inplace=True)
    cash_df['cash_date'] = datetime.now().date()
    return cash_df


def main():
    setup_directories()
    
    internal_df = generate_internal_trades(NUM_TRADES)
    broker_df = corrupt_broker_data(internal_df)
    
    internal_pos = aggregate_positions(internal_df)
    broker_pos = aggregate_positions(broker_df)
    
    internal_cash = aggregate_cash(internal_df)
    broker_cash = aggregate_cash(broker_df)
    
    date_str = datetime.now().strftime('%Y-%m-%d')
    
    print("💾 Saving to CSV...")
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