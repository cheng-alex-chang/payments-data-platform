-- Grain: one row per payment (latest version). Types the RAW VARIANT into columns and
-- dedups: each daily extract is a full snapshot, so RAW accumulates multiple versions per
-- payment_id; QUALIFY keeps the newest (updated_at, load-time tiebreak) -- replay-safe,
-- mirroring the silver-layer dedup in the streaming half.
-- amount was serialized as a JSON *string* to preserve exact money precision; cast straight
-- back to NUMBER(12,2), never through a float.
-- shopper_id is PII: SHA-256 hashed here so the raw customer id never propagates past
-- staging, matching the streaming/DLT bronze masking (sha256 of the id string) -- both
-- pipelines then expose the same tokenized identifier.
-- MIGRATION: this changes shopper_id's type (numeric -> 64-char text). fct_payments_usd is
-- incremental, so deploying this over an existing build needs a one-time
-- `dbt run --full-refresh` -- Snowflake can't alter the populated numeric column in place.
SELECT
    raw:payment_id::INTEGER        AS payment_id,
    raw:merchant_id::INTEGER       AS merchant_id,
    SHA2(raw:shopper_id::INTEGER::STRING, 256) AS shopper_id,
    raw:amount::NUMBER(12, 2)      AS amount,
    raw:currency::STRING           AS currency,
    raw:payment_method::STRING     AS payment_method,
    raw:payment_status::STRING     AS payment_status,
    raw:country_code::STRING       AS country_code,
    raw:created_at::TIMESTAMP_NTZ  AS created_at,
    raw:updated_at::TIMESTAMP_NTZ  AS updated_at
FROM {{ source('raw', 'raw_payments') }}
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY raw:payment_id::INTEGER
    ORDER BY raw:updated_at::TIMESTAMP_NTZ DESC, loaded_at DESC
) = 1
