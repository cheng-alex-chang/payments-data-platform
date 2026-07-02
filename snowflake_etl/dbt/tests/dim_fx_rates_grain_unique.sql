-- Grain enforcement for the composite key: one row per (currency, rate_date).
-- The built-in `unique` test is single-column, so the dimension's grain gets a singular test.
SELECT currency, rate_date, COUNT(*) AS n
FROM {{ ref('dim_fx_rates') }}
GROUP BY currency, rate_date
HAVING COUNT(*) > 1
