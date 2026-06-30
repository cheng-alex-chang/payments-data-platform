-- Staging: type the RAW_FX_RATES VARIANT and dedup to one rate per (date, currency).
--
-- rate_to_usd is already "USD per 1 unit of currency" (the extractor inverted the ECB quote),
-- so the fact layer can multiply amount * rate_to_usd directly. Re-loads are deduped by load
-- time so the latest copy of a given (rate_date, currency) wins.
CREATE OR REPLACE VIEW ANALYTICS.STG_FX_RATES AS
SELECT
    raw:rate_date::DATE     AS rate_date,
    raw:currency::STRING    AS currency,
    raw:rate_to_usd::FLOAT  AS rate_to_usd
FROM RAW.RAW_FX_RATES
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY raw:rate_date::DATE, raw:currency::STRING
    ORDER BY loaded_at DESC
) = 1;
