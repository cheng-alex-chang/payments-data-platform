-- Dimension: one clean rate_to_usd per (currency, calendar day), gaps forward-filled.
--
-- The problem: ECB/Frankfurter publishes rates on business days only. A payment created on a
-- Saturday, Sunday, or holiday has no rate for that exact date, so a naive join would drop it.
-- The fix: build a continuous calendar spine over the data window, cross-join every currency,
-- LEFT JOIN the actual rates, then carry the most recent known rate forward
-- (LAST_VALUE ... IGNORE NULLS). A backward FIRST_VALUE covers the leading edge (a payment that
-- precedes the first published rate). is_filled flags any day whose rate was carried rather than
-- published -- useful for transparency and auditing.
CREATE OR REPLACE TABLE ANALYTICS.DIM_FX_RATES AS
WITH bounds AS (
    SELECT
        (SELECT MIN(created_at::DATE) FROM ANALYTICS.STG_PAYMENTS) AS pay_min,
        (SELECT MAX(created_at::DATE) FROM ANALYTICS.STG_PAYMENTS) AS pay_max,
        (SELECT MIN(rate_date) FROM ANALYTICS.STG_FX_RATES)        AS fx_min,
        (SELECT MAX(rate_date) FROM ANALYTICS.STG_FX_RATES)        AS fx_max
),
span AS (
    SELECT LEAST(pay_min, fx_min) AS start_date,
           GREATEST(pay_max, fx_max) AS end_date
    FROM bounds
),
date_spine AS (
    SELECT d FROM (
        SELECT DATEADD('day', SEQ4(), (SELECT start_date FROM span)) AS d
        FROM TABLE(GENERATOR(ROWCOUNT => 800))  -- ~2.2yr buffer; trimmed to the span below
    )
    WHERE d <= (SELECT end_date FROM span)
),
currencies AS (
    SELECT DISTINCT currency FROM ANALYTICS.STG_FX_RATES
),
grid AS (
    SELECT c.currency, s.d AS rate_date
    FROM currencies c
    CROSS JOIN date_spine s
),
joined AS (
    SELECT g.currency, g.rate_date, f.rate_to_usd
    FROM grid g
    LEFT JOIN ANALYTICS.STG_FX_RATES f
        ON f.currency = g.currency
       AND f.rate_date = g.rate_date
)
SELECT
    currency,
    rate_date,
    COALESCE(
        LAST_VALUE(rate_to_usd) IGNORE NULLS OVER (
            PARTITION BY currency ORDER BY rate_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ),
        FIRST_VALUE(rate_to_usd) IGNORE NULLS OVER (
            PARTITION BY currency ORDER BY rate_date
            ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING
        )
    ) AS rate_to_usd,
    rate_to_usd IS NULL AS is_filled  -- TRUE = carried from a prior/next business day
FROM joined;
