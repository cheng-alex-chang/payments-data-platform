-- Staging: type the RAW_PAYMENTS VARIANT into columns and dedup to one row per payment.
--
-- Each daily extract is a FULL snapshot of the payments table, so RAW_PAYMENTS accumulates
-- multiple versions of the same payment_id across dt partitions. QUALIFY keeps only the latest
-- version (newest updated_at, breaking ties by load time) -- a replay-safe, idempotent recompute
-- that mirrors the silver-layer dedup in the streaming half of the project.
--
-- amount was serialized as a JSON *string* in Phase 2 to preserve exact money precision; we cast
-- it back to NUMBER(12,2) here, never going through a float.
CREATE OR REPLACE VIEW ANALYTICS.STG_PAYMENTS AS
SELECT
    raw:payment_id::INTEGER        AS payment_id,
    raw:merchant_id::INTEGER       AS merchant_id,
    raw:shopper_id::INTEGER        AS shopper_id,
    raw:amount::NUMBER(12, 2)      AS amount,
    raw:currency::STRING           AS currency,
    raw:payment_method::STRING     AS payment_method,
    raw:payment_status::STRING     AS payment_status,
    raw:country_code::STRING       AS country_code,
    raw:created_at::TIMESTAMP_NTZ  AS created_at,
    raw:updated_at::TIMESTAMP_NTZ  AS updated_at
FROM RAW.RAW_PAYMENTS
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY raw:payment_id::INTEGER
    ORDER BY raw:updated_at::TIMESTAMP_NTZ DESC, loaded_at DESC
) = 1;
