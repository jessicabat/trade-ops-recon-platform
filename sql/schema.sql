-- ============================================================================
-- TRADE OPERATIONS RECONCILIATION PLATFORM - DATABASE SCHEMA
-- ============================================================================
-- Purpose: Schema for trade, position, and cash reconciliation system
-- Database: PostgreSQL 14+
-- Author: Jessica
-- ============================================================================

-- Dropping existing tables if re-running
DROP TABLE IF EXISTS recon_trades CASCADE;
DROP TABLE IF EXISTS recon_positions CASCADE;
DROP TABLE IF EXISTS recon_cash CASCADE;
DROP TABLE IF EXISTS daily_pnl CASCADE;
DROP TABLE IF EXISTS internal_trades CASCADE;
DROP TABLE IF EXISTS broker_trades CASCADE;
DROP TABLE IF EXISTS internal_positions CASCADE;
DROP TABLE IF EXISTS broker_positions CASCADE;
DROP TABLE IF EXISTS internal_cash CASCADE;
DROP TABLE IF EXISTS broker_cash CASCADE;
DROP TABLE IF EXISTS pipeline_runs CASCADE;

-- ============================================================================
-- CORE TRADE DATA TABLES
-- ============================================================================

-- Internal Trades (Source of Truth from firm's trading system)
CREATE TABLE internal_trades (
    trade_id VARCHAR(50) PRIMARY KEY,
    trade_date DATE NOT NULL,
    settlement_date DATE NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    account VARCHAR(50) NOT NULL,
    strategy VARCHAR(50) NOT NULL,
    venue VARCHAR(20) NOT NULL,
    side VARCHAR(4) NOT NULL CHECK (side IN ('BUY', 'SELL')),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    price NUMERIC(12, 4) NOT NULL CHECK (price > 0),
    fees NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    net_amount NUMERIC(15, 2) NOT NULL,  -- Signed cash flow (negative for buys)
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Broker Trades (External broker/custodian confirms)
CREATE TABLE broker_trades (
    trade_id VARCHAR(50) PRIMARY KEY,
    trade_date DATE NOT NULL,
    settlement_date DATE NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    account VARCHAR(50) NOT NULL,
    strategy VARCHAR(50) NOT NULL,
    venue VARCHAR(20) NOT NULL,
    side VARCHAR(4) NOT NULL CHECK (side IN ('BUY', 'SELL')),
    quantity INTEGER NOT NULL CHECK (quantity > 0),
    price NUMERIC(12, 4) NOT NULL CHECK (price > 0),
    fees NUMERIC(10, 2) NOT NULL DEFAULT 0.00,
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    net_amount NUMERIC(15, 2) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- POSITION DATA TABLES
-- ============================================================================

-- Internal Positions (Aggregated from internal trades)
CREATE TABLE internal_positions (
    account VARCHAR(50) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    net_position INTEGER NOT NULL,  -- Signed: positive = long, negative = short
    position_date DATE NOT NULL DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account, symbol, position_date)
);

-- Broker Positions (From broker/custodian position reports)
CREATE TABLE broker_positions (
    account VARCHAR(50) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    net_position INTEGER NOT NULL,
    position_date DATE NOT NULL DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account, symbol, position_date)
);

-- ============================================================================
-- CASH DATA TABLES
-- ============================================================================

-- Internal Cash (Aggregated from internal trades)
CREATE TABLE internal_cash (
    account VARCHAR(50) NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    net_cash_balance NUMERIC(15, 2) NOT NULL,
    cash_date DATE NOT NULL DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account, currency, cash_date)
);

-- Broker Cash (From broker/custodian cash reports)
CREATE TABLE broker_cash (
    account VARCHAR(50) NOT NULL,
    currency VARCHAR(3) NOT NULL DEFAULT 'USD',
    net_cash_balance NUMERIC(15, 2) NOT NULL,
    cash_date DATE NOT NULL DEFAULT CURRENT_DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (account, currency, cash_date)
);

-- ============================================================================
-- RECONCILIATION OUTPUT TABLES
-- ============================================================================

-- Trade Reconciliation Results
CREATE TABLE recon_trades (
    recon_id SERIAL PRIMARY KEY,
    recon_date DATE NOT NULL DEFAULT CURRENT_DATE,
    trade_id VARCHAR(50) NOT NULL,
    symbol VARCHAR(20),
    account VARCHAR(50),
    break_type VARCHAR(50) NOT NULL,  -- e.g., MISSING_IN_BROKER, PRICE_MISMATCH
    severity VARCHAR(20) DEFAULT 'MEDIUM',  -- LOW, MEDIUM, HIGH, CRITICAL
    internal_value TEXT,  -- JSON or text description of internal record
    broker_value TEXT,    -- JSON or text description of broker record
    notional_impact NUMERIC(15, 2),  -- Dollar impact of the break
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Position Reconciliation Results
CREATE TABLE recon_positions (
    recon_id SERIAL PRIMARY KEY,
    recon_date DATE NOT NULL DEFAULT CURRENT_DATE,
    account VARCHAR(50) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    internal_position INTEGER,
    broker_position INTEGER,
    position_difference INTEGER,  -- internal - broker
    break_type VARCHAR(50) NOT NULL,  -- e.g., POSITION_MISMATCH, MISSING_POSITION
    severity VARCHAR(20) DEFAULT 'MEDIUM',
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Cash Reconciliation Results
CREATE TABLE recon_cash (
    recon_id SERIAL PRIMARY KEY,
    recon_date DATE NOT NULL DEFAULT CURRENT_DATE,
    account VARCHAR(50) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    internal_balance NUMERIC(15, 2),
    broker_balance NUMERIC(15, 2),
    cash_difference NUMERIC(15, 2),  -- internal - broker
    break_type VARCHAR(50) NOT NULL,  -- e.g., CASH_MISMATCH
    severity VARCHAR(20) DEFAULT 'MEDIUM',
    resolved BOOLEAN DEFAULT FALSE,
    resolved_at TIMESTAMP,
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- PNL TABLE
-- ============================================================================

-- Daily PnL by Strategy/Symbol/Account
CREATE TABLE daily_pnl (
    pnl_id SERIAL PRIMARY KEY,
    pnl_date DATE NOT NULL DEFAULT CURRENT_DATE,
    account VARCHAR(50) NOT NULL,
    strategy VARCHAR(50) NOT NULL,
    symbol VARCHAR(20) NOT NULL,
    realized_pnl NUMERIC(15, 2) DEFAULT 0.00,
    unrealized_pnl NUMERIC(15, 2) DEFAULT 0.00,
    total_pnl NUMERIC(15, 2) GENERATED ALWAYS AS (realized_pnl + unrealized_pnl) STORED,
    fees_total NUMERIC(10, 2) DEFAULT 0.00,
    trade_count INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (pnl_date, account, strategy, symbol)
);

-- ============================================================================
-- PIPELINE METADATA TABLE
-- ============================================================================

-- Track pipeline runs for monitoring/SLA
CREATE TABLE pipeline_runs (
    run_id SERIAL PRIMARY KEY,
    run_date DATE NOT NULL,
    pipeline_name VARCHAR(100) NOT NULL,  -- e.g., 'eod_reconciliation', 'pnl_calculation'
    status VARCHAR(20) NOT NULL,  -- SUCCESS, FAILED, RUNNING
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    duration_seconds INTEGER,
    rows_processed INTEGER,
    breaks_found INTEGER,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- ============================================================================
-- INDEXES FOR PERFORMANCE
-- ============================================================================

-- Internal Trades Indexes
CREATE INDEX idx_internal_trades_date ON internal_trades(trade_date);
CREATE INDEX idx_internal_trades_symbol ON internal_trades(symbol);
CREATE INDEX idx_internal_trades_account ON internal_trades(account);
CREATE INDEX idx_internal_trades_strategy ON internal_trades(strategy);
CREATE INDEX idx_internal_trades_settlement ON internal_trades(settlement_date);

-- Broker Trades Indexes
CREATE INDEX idx_broker_trades_date ON broker_trades(trade_date);
CREATE INDEX idx_broker_trades_symbol ON broker_trades(symbol);
CREATE INDEX idx_broker_trades_account ON broker_trades(account);
CREATE INDEX idx_broker_trades_strategy ON broker_trades(strategy);
CREATE INDEX idx_broker_trades_settlement ON broker_trades(settlement_date);

-- Reconciliation Indexes
CREATE INDEX idx_recon_trades_date ON recon_trades(recon_date);
CREATE INDEX idx_recon_trades_type ON recon_trades(break_type);
CREATE INDEX idx_recon_trades_resolved ON recon_trades(resolved);
CREATE INDEX idx_recon_trades_severity ON recon_trades(severity);

CREATE INDEX idx_recon_positions_date ON recon_positions(recon_date);
CREATE INDEX idx_recon_positions_resolved ON recon_positions(resolved);

CREATE INDEX idx_recon_cash_date ON recon_cash(recon_date);
CREATE INDEX idx_recon_cash_resolved ON recon_cash(resolved);

-- PnL Indexes
CREATE INDEX idx_daily_pnl_date ON daily_pnl(pnl_date);
CREATE INDEX idx_daily_pnl_strategy ON daily_pnl(strategy);
CREATE INDEX idx_daily_pnl_account ON daily_pnl(account);

-- Pipeline Runs Indexes
CREATE INDEX idx_pipeline_runs_date ON pipeline_runs(run_date);
CREATE INDEX idx_pipeline_runs_status ON pipeline_runs(status);

-- ============================================================================
-- VIEWS FOR COMMON QUERIES
-- ============================================================================

-- View: Unresolved Breaks Summary
CREATE OR REPLACE VIEW v_unresolved_breaks AS
SELECT 
    'TRADE' AS break_category,
    recon_date,
    break_type,
    severity,
    COUNT(*) AS count,
    SUM(COALESCE(notional_impact, 0)) AS total_notional_impact
FROM recon_trades
WHERE resolved = FALSE
GROUP BY recon_date, break_type, severity

UNION ALL

SELECT 
    'POSITION' AS break_category,
    recon_date,
    break_type,
    severity,
    COUNT(*) AS count,
    NULL AS total_notional_impact
FROM recon_positions
WHERE resolved = FALSE
GROUP BY recon_date, break_type, severity

UNION ALL

SELECT 
    'CASH' AS break_category,
    recon_date,
    break_type,
    severity,
    COUNT(*) AS count,
    SUM(ABS(COALESCE(cash_difference, 0))) AS total_notional_impact
FROM recon_cash
WHERE resolved = FALSE
GROUP BY recon_date, break_type, severity;

-- View: Daily PnL Summary by Strategy
CREATE OR REPLACE VIEW v_pnl_by_strategy AS
SELECT 
    pnl_date,
    strategy,
    SUM(realized_pnl) AS total_realized_pnl,
    SUM(unrealized_pnl) AS total_unrealized_pnl,
    SUM(total_pnl) AS total_pnl,
    SUM(fees_total) AS total_fees,
    SUM(trade_count) AS total_trades
FROM daily_pnl
GROUP BY pnl_date, strategy
ORDER BY pnl_date DESC, total_pnl DESC;

-- ============================================================================
-- HELPER FUNCTIONS
-- ============================================================================

-- Function: Calculate trade notional
CREATE OR REPLACE FUNCTION calculate_notional(
    p_quantity INTEGER,
    p_price NUMERIC
) RETURNS NUMERIC AS $$
BEGIN
    RETURN ABS(p_quantity * p_price);
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- Function: Classify break severity based on notional
CREATE OR REPLACE FUNCTION classify_severity(
    p_notional NUMERIC
) RETURNS VARCHAR AS $$
BEGIN
    CASE
        WHEN p_notional >= 100000 THEN RETURN 'CRITICAL';
        WHEN p_notional >= 10000 THEN RETURN 'HIGH';
        WHEN p_notional >= 1000 THEN RETURN 'MEDIUM';
        ELSE RETURN 'LOW';
    END CASE;
END;
$$ LANGUAGE plpgsql IMMUTABLE;

-- ============================================================================
-- COMMENTS FOR DOCUMENTATION
-- ============================================================================

COMMENT ON TABLE internal_trades IS 'Golden source trade records from internal trading systems';
COMMENT ON TABLE broker_trades IS 'Trade confirms received from external brokers and custodians';
COMMENT ON TABLE recon_trades IS 'Trade-level reconciliation breaks and exceptions';
COMMENT ON TABLE recon_positions IS 'Position-level reconciliation breaks';
COMMENT ON TABLE recon_cash IS 'Cash balance reconciliation breaks';
COMMENT ON TABLE daily_pnl IS 'Daily profit and loss by strategy, symbol, and account';
COMMENT ON TABLE pipeline_runs IS 'Metadata and SLA tracking for ETL and reconciliation pipelines';

COMMENT ON COLUMN recon_trades.break_type IS 'Type of break: MISSING_IN_BROKER, MISSING_IN_INTERNAL, PRICE_MISMATCH, QTY_MISMATCH, FEE_MISMATCH, SETTLEMENT_MISMATCH';
COMMENT ON COLUMN recon_trades.severity IS 'Break severity based on notional impact: LOW, MEDIUM, HIGH, CRITICAL';
COMMENT ON COLUMN daily_pnl.total_pnl IS 'Auto-calculated: realized_pnl + unrealized_pnl';

-- ============================================================================
-- GRANT PERMISSIONS (adjust based on your setup)
-- ============================================================================

-- Example: Grant access to a 'recon_user' role
-- GRANT SELECT, INSERT, UPDATE ON ALL TABLES IN SCHEMA public TO recon_user;
-- GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO recon_user;

-- ============================================================================
-- END OF SCHEMA
-- ============================================================================
