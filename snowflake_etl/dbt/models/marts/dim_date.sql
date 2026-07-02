-- Grain: one row per calendar day, spanning the full payments + FX window.
-- The conformed date dimension: it owns the calendar spine (dim_fx_rates builds its
-- gap-free grid off this, and fct_payments_usd's created_date joins to it), plus the
-- standard reporting attributes (year/quarter/month/day-of-week/weekend flag).
WITH bounds AS (
    SELECT
        LEAST(
            (SELECT MIN(created_at::DATE) FROM {{ ref('stg_payments') }}),
            (SELECT MIN(rate_date) FROM {{ ref('stg_fx_rates') }})
        ) AS start_date,
        GREATEST(
            (SELECT MAX(created_at::DATE) FROM {{ ref('stg_payments') }}),
            (SELECT MAX(rate_date) FROM {{ ref('stg_fx_rates') }})
        ) AS end_date
),
spine AS (
    SELECT d FROM (
        SELECT DATEADD('day', SEQ4(), (SELECT start_date FROM bounds)) AS d
        FROM TABLE(GENERATOR(ROWCOUNT => 3660))  -- ~10yr cap; trimmed to the span below
    )
    WHERE d <= (SELECT end_date FROM bounds)
)
SELECT
    d                                  AS date_day,
    YEAR(d)                            AS year,
    QUARTER(d)                         AS quarter,
    MONTH(d)                           AS month,
    DATE_TRUNC('month', d)::DATE       AS month_start,
    DAYOFWEEKISO(d)                    AS day_of_week,  -- 1 = Monday .. 7 = Sunday
    DAYNAME(d)                         AS day_name,
    DAYOFWEEKISO(d) IN (6, 7)          AS is_weekend    -- the days FX never publishes
FROM spine
