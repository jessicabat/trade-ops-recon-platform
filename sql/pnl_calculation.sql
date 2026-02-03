-- ============================================================================
-- DAILY PNL CALCULATION
-- ============================================================================
-- Purpose: Calculate realized P&L by Strategy, Symbol, and Account
-- Note: This is a simplified version. Real systems also track:
--       - Unrealized P&L (requires EOD mark prices)
--       - Multi-day position tracking (FIFO/LIFO lot accounting)
--       - FX P&L for multi-currency books
-- ============================================================================

-- 1. CLEAN UP: Remove existing PnL for this date to allow re-runs
DELETE FROM daily_pnl WHERE pnl_date = :pnl_date;

-- 2. CALCULATE REALIZED PNL
-- Strategy: For each symbol/strategy/account, calculate net realized PnL
-- Formula: (Sell proceeds - Buy cost) - Fees

INSERT INTO daily_pnl (
    pnl_date, account, strategy, symbol,
    realized_pnl, fees_total, trade_count
)
SELECT 
    :pnl_date AS pnl_date,
    account,
    strategy,
    symbol,
    -- Realized PnL = Sum of all principal (already signed: BUY = negative, SELL = positive)
    -- The 'principal' column already includes fees in the cash flow calculation,
    -- but we want to show fees separately for reporting
    SUM(CASE WHEN side = 'BUY' THEN -1 * (quantity * price) ELSE (quantity * price) END) AS realized_pnl,
    SUM(fees) AS fees_total,
    COUNT(*) AS trade_count
FROM internal_trades
WHERE trade_date = :pnl_date
GROUP BY account, strategy, symbol;

-- ============================================================================
-- PNL SUMMARY
-- ============================================================================
-- Return aggregate PnL metrics for the day
SELECT 
    strategy,
    COUNT(DISTINCT symbol) AS symbols_traded,
    SUM(trade_count) AS total_trades,
    SUM(realized_pnl) AS total_realized_pnl,
    SUM(fees_total) AS total_fees,
    SUM(total_pnl) AS net_pnl,
    ROUND(AVG(realized_pnl), 2) AS avg_pnl_per_symbol
FROM daily_pnl
WHERE pnl_date = :pnl_date
GROUP BY strategy
ORDER BY net_pnl DESC;
