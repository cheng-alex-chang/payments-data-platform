-- Grain: one row per (currency, calendar day), gap-free.
-- ECB/Frankfurter publishes rates on business days only; a weekend/holiday payment has no
-- rate for its exact date and a naive join would drop it. Take the calendar spine from the
-- conformed date dimension, cross-join every currency, LEFT JOIN the published rates, then
-- carry the last known rate forward (LAST_VALUE IGNORE NULLS). A backward FIRST_VALUE covers
-- the leading edge. is_filled flags carried days -- auditable, not hidden.
WITH date_spine AS (
    SELECT date_day AS d FROM {{ ref('dim_date') }}  -- dim_date owns the calendar
),
currencies AS (
    SELECT DISTINCT currency FROM {{ ref('stg_fx_rates') }}
),
grid AS (
    SELECT c.currency, s.d AS rate_date
    FROM currencies c
    CROSS JOIN date_spine s
),
joined AS (
    SELECT g.currency, g.rate_date, f.rate_to_usd
    FROM grid g
    LEFT JOIN {{ ref('stg_fx_rates') }} f
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
FROM joined
