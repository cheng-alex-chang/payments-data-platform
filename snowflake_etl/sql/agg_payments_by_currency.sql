-- Aggregate: monthly payment volume by currency and country, in both native and USD terms.
--
-- This is the finance/BI deliverable, and it is deliberately a DIFFERENT shape from the
-- streaming lakehouse's hourly operational gold: MONTHLY grain, currency-normalized to USD.
-- usd_volume is the headline (cross-currency revenue you can actually sum); avg_rate_to_usd
-- exposes the FX drift over the 12-month window that motivated the whole normalization.
CREATE OR REPLACE TABLE ANALYTICS.AGG_PAYMENTS_BY_CURRENCY AS
SELECT
    DATE_TRUNC('month', created_at)::DATE AS month,
    currency,
    country_code,
    COUNT(*)            AS payment_count,
    SUM(amount)         AS native_volume,
    SUM(usd_amount)     AS usd_volume,
    AVG(rate_to_usd)    AS avg_rate_to_usd
FROM ANALYTICS.FCT_PAYMENTS_USD
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3;
