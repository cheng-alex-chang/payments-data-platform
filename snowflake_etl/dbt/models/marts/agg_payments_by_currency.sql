-- Grain: one row per (month, currency, country_code).
-- The finance/BI deliverable, deliberately a DIFFERENT shape from the streaming lakehouse's
-- hourly operational gold: MONTHLY grain, normalized to USD. usd_volume is the headline
-- (cross-currency revenue you can actually sum); avg_rate_to_usd exposes the FX drift that
-- motivated the normalization.
SELECT
    DATE_TRUNC('month', created_at)::DATE AS month,
    currency,
    country_code,
    COUNT(*)            AS payment_count,
    SUM(amount)         AS native_volume,
    SUM(usd_amount)     AS usd_volume,
    AVG(rate_to_usd)    AS avg_rate_to_usd
FROM {{ ref('fct_payments_usd') }}
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
