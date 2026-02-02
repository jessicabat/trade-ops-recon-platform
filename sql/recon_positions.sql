-- ============================================================================
-- POSITION RECONCILIATION LOGIC
-- ============================================================================
-- Purpose: Identify breaks between Internal and Broker position records
-- Note: Positions are aggregated from trades, so position breaks often indicate
--       either missing trades or incorrect aggregation logic.
-- ============================================================================

-- 1. CLEAN UP: Remove existing breaks for this date
DELETE FROM recon_positions WHERE recon_date = :recon_date;

-- 2. INSERT NEW BREAKS

-- A. POSITION MISMATCHES (Both sides have the position, but quantities differ)
INSERT INTO recon_positions (
    recon_date, account, symbol, 
    internal_position, broker_position, position_difference,
    break_type, severity
)
SELECT 
    :recon_date,
    COALESCE(i.account, b.account) AS account,
    COALESCE(i.symbol, b.symbol) AS symbol,
    COALESCE(i.net_position, 0) AS internal_position,
    COALESCE(b.net_position, 0) AS broker_position,
    COALESCE(i.net_position, 0) - COALESCE(b.net_position, 0) AS position_difference,
    CASE 
        WHEN i.symbol IS NULL THEN 'MISSING_IN_INTERNAL'
        WHEN b.symbol IS NULL THEN 'MISSING_IN_BROKER'
        ELSE 'POSITION_MISMATCH'
    END AS break_type,
    CASE 
        WHEN ABS(COALESCE(i.net_position, 0) - COALESCE(b.net_position, 0)) >= 1000 THEN 'CRITICAL'
        WHEN ABS(COALESCE(i.net_position, 0) - COALESCE(b.net_position, 0)) >= 100 THEN 'HIGH'
        WHEN ABS(COALESCE(i.net_position, 0) - COALESCE(b.net_position, 0)) >= 10 THEN 'MEDIUM'
        ELSE 'LOW'
    END AS severity
FROM internal_positions i
FULL OUTER JOIN broker_positions b 
    ON i.account = b.account 
    AND i.symbol = b.symbol
    AND i.position_date = :recon_date
    AND b.position_date = :recon_date
WHERE i.position_date = :recon_date 
   OR b.position_date = :recon_date
   -- Only flag if there's an actual difference
   AND COALESCE(i.net_position, 0) != COALESCE(b.net_position, 0);

-- ============================================================================
-- POSITION RECONCILIATION SUMMARY
-- ============================================================================
SELECT 
    break_type,
    severity,
    COUNT(*) AS break_count,
    SUM(ABS(position_difference)) AS total_position_diff,
    ROUND(AVG(ABS(position_difference)), 2) AS avg_position_diff
FROM recon_positions
WHERE recon_date = :recon_date
GROUP BY break_type, severity
ORDER BY 
    CASE severity 
        WHEN 'CRITICAL' THEN 1 
        WHEN 'HIGH' THEN 2 
        WHEN 'MEDIUM' THEN 3 
        ELSE 4 
    END,
    total_position_diff DESC;
