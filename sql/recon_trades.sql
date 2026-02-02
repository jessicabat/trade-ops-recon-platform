-- ============================================================================
-- TRADE RECONCILIATION LOGIC
-- ============================================================================
-- Purpose: Identify breaks between Internal and Broker trade records
-- Note: A single trade can generate MULTIPLE breaks (e.g., both price + fee mismatch).
--       Each break is tracked separately for investigation.
-- ============================================================================

-- 1. CLEAN UP: Remove existing breaks for this date to allow re-runs (Idempotency)
DELETE FROM recon_trades WHERE recon_date = :recon_date;

-- 2. INSERT NEW BREAKS

-- A. MISSING IN BROKER (We have it, they don't)
INSERT INTO recon_trades (recon_date, trade_id, symbol, account, break_type, severity, internal_value, broker_value, notional_impact)
SELECT 
    :recon_date,
    i.trade_id,
    i.symbol,
    i.account,
    'MISSING_IN_BROKER',
    classify_severity(ABS(i.quantity * i.price)),
    'Side: ' || i.side || ' Qty: ' || i.quantity || ' @ ' || i.price,
    'NOT FOUND',
    ABS(i.quantity * i.price)
FROM internal_trades i
LEFT JOIN broker_trades b ON i.trade_id = b.trade_id
WHERE i.trade_date = :recon_date 
  AND b.trade_id IS NULL;

-- B. MISSING IN INTERNAL (Phantom Trade - They have it, we don't)
INSERT INTO recon_trades (recon_date, trade_id, symbol, account, break_type, severity, internal_value, broker_value, notional_impact)
SELECT 
    :recon_date,
    b.trade_id,
    b.symbol,
    b.account,
    'MISSING_IN_INTERNAL',
    classify_severity(ABS(b.quantity * b.price)),
    'NOT FOUND',
    'Side: ' || b.side || ' Qty: ' || b.quantity || ' @ ' || b.price,
    ABS(b.quantity * b.price)
FROM broker_trades b
LEFT JOIN internal_trades i ON b.trade_id = i.trade_id
WHERE b.trade_date = :recon_date 
  AND i.trade_id IS NULL;

-- C. PRICE MISMATCHES (IDs match, Price differs)
INSERT INTO recon_trades (recon_date, trade_id, symbol, account, break_type, severity, internal_value, broker_value, notional_impact)
SELECT 
    :recon_date,
    i.trade_id,
    i.symbol,
    i.account,
    'PRICE_MISMATCH',
    classify_severity(ABS( (i.price - b.price) * i.quantity )),
    CAST(i.price AS TEXT),
    CAST(b.price AS TEXT),
    ABS( (i.price - b.price) * i.quantity )
FROM internal_trades i
JOIN broker_trades b ON i.trade_id = b.trade_id
WHERE i.trade_date = :recon_date 
  AND i.price != b.price;

-- D. QUANTITY MISMATCHES
INSERT INTO recon_trades (recon_date, trade_id, symbol, account, break_type, severity, internal_value, broker_value, notional_impact)
SELECT 
    :recon_date,
    i.trade_id,
    i.symbol,
    i.account,
    'QTY_MISMATCH',
    classify_severity(ABS( (i.quantity - b.quantity) * i.price )),
    CAST(i.quantity AS TEXT),
    CAST(b.quantity AS TEXT),
    ABS( (i.quantity - b.quantity) * i.price )
FROM internal_trades i
JOIN broker_trades b ON i.trade_id = b.trade_id
WHERE i.trade_date = :recon_date 
  AND i.quantity != b.quantity
  AND i.quantity > 0 AND b.quantity > 0;

-- E. FEE MISMATCHES
INSERT INTO recon_trades (recon_date, trade_id, symbol, account, break_type, severity, internal_value, broker_value, notional_impact)
SELECT 
    :recon_date,
    i.trade_id,
    i.symbol,
    i.account,
    'FEE_MISMATCH',
    'LOW',
    CAST(i.fees AS TEXT),
    CAST(b.fees AS TEXT),
    ABS(i.fees - b.fees)
FROM internal_trades i
JOIN broker_trades b ON i.trade_id = b.trade_id
WHERE i.trade_date = :recon_date 
  AND i.fees != b.fees;

-- F. SETTLEMENT DATE MISMATCHES
INSERT INTO recon_trades (recon_date, trade_id, symbol, account, break_type, severity, internal_value, broker_value, notional_impact)
SELECT 
    :recon_date,
    i.trade_id,
    i.symbol,
    i.account,
    'SETTLEMENT_MISMATCH',
    'MEDIUM',
    CAST(i.settlement_date AS TEXT),
    CAST(b.settlement_date AS TEXT),
    0
FROM internal_trades i
JOIN broker_trades b ON i.trade_id = b.trade_id
WHERE i.trade_date = :recon_date 
  AND i.settlement_date != b.settlement_date;

-- ============================================================================
-- RECONCILIATION SUMMARY
-- ============================================================================
SELECT 
    break_type,
    severity,
    COUNT(*) AS break_count,
    SUM(notional_impact) AS total_notional,
    ROUND(AVG(notional_impact), 2) AS avg_notional
FROM recon_trades
WHERE recon_date = :recon_date
GROUP BY break_type, severity
ORDER BY 
    CASE severity 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
        WHEN 'MEDIUM' THEN 3 
        ELSE 4 
    END,
    total_notional DESC;
