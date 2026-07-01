-- No null or non-positive rate slipped through the forward-fill.
SELECT currency, rate_date, rate_to_usd
FROM {{ ref('dim_fx_rates') }}
WHERE rate_to_usd IS NULL OR rate_to_usd <= 0
