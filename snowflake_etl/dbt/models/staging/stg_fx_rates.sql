-- Grain: one row per (rate_date, currency) as published (business days only; gaps are the
-- dimension's job). rate_to_usd is already "USD per 1 unit" -- the extractor inverted the
-- ECB quote -- so the fact layer multiplies directly. Re-loads dedup by load time.
-- rate_to_usd is money: fixed-scale NUMBER(18,8), never FLOAT -- a binary float drifts at
-- the cent level once multiplied through amount * rate in the fact.
SELECT
    raw:rate_date::DATE            AS rate_date,
    raw:currency::STRING           AS currency,
    raw:rate_to_usd::NUMBER(18, 8) AS rate_to_usd
FROM {{ source('raw', 'raw_fx_rates') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY raw:rate_date::DATE, raw:currency::STRING
    ORDER BY loaded_at DESC
) = 1
