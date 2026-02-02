-- ============================================================================
-- CASH RECONCILIATION LOGIC
-- ============================================================================
-- Purpose: Identify breaks between Internal and Broker cash balances
-- Note: Cash breaks indicate either missing trades, incorrect fees, or FX issues.
-- ============================================================================

-- 1. CLEAN UP: Remove existing breaks for this date
DELETE FROM recon_cash WHERE recon_date = :recon_date;

-- 2. INSERT NEW BREAKS

-- A. CASH MISMATCHES (Both sides have cash, but balances differ)
INSERT INTO recon_cash (
    recon_date, account, currency,
    internal_balance, broker_balance, cash_difference,
    break_type, severity
)
SELECT 
    :recon_date,
    COALESCE(i.account, b.account) AS account,
    COALESCE(i.currency, b.currency) AS currency,
    COALESCE(i.net_cash_balance, 0) AS internal_balance,
    COALESCE(b.net_cash_balance, 0) AS broker_balance,
    COALESCE(i.net_cash_balance, 0) - COALESCE(b.net_cash_balance, 0) AS cash_difference,
    CASE 
        WHEN i.account IS NULL THEN 'MISSING_IN_INTERNAL'
        WHEN b.account IS NULL THEN 'MISSING_IN_BROKER'
        ELSE 'CASH_MISMATCH'
    END AS break_type,
    CASE 
        WHEN ABS(COALESCE(i.net_cash_balance, 0) - COALESCE(b.net_cash_balance, 0)) >= 100000 THEN 'CRITICAL'
        WHEN ABS(COALESCE(i.net_cash_balance, 0) - COALESCE(b.net_cash_balance, 0)) >= 10000 THEN 'HIGH'
        WHEN ABS(COALESCE(i.net_cash_balance, 0) - COALESCE(b.net_cash_balance, 0)) >= 1000 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity
FROM internal_cash i
FULL OUTER JOIN broker_cash b 
    ON i.account = b.account 
    AND i.currency = b.currency
    AND i.cash_date = :recon_date
    AND b.cash_date = :recon_date
WHERE i.cash_date = :recon_date 
   OR b.cash_date = :recon_date
   -- Only flag if there's an actual difference (allow $0.01 rounding tolerance)
   AND ABS(COALESCE(i.net_cash_balance, 0) - COALESCE(b.net_cash_balance, 0)) > 0.01;

-- ============================================================================
-- CASH RECONCILIATION SUMMARY
-- ============================================================================
SELECT 
    break_type,
    severity,
    COUNT(*) AS break_count,
    SUM(ABS(cash_difference)) AS total_cash_diff,
    ROUND(AVG(ABS(cash_difference)), 2) AS avg_cash_diff
FROM recon_cash
WHERE recon_date = :recon_date
GROUP BY break_type, severity
ORDER BY 
    CASE severity 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
        WHEN 'MEDIUM' THEN 3 
        ELSE 4 
    END,
    total_cash_diff DESC;
