-- Grain: one row per payment, normalized to USD.
-- usd_amount = amount * rate_to_usd is THE business deliverable -- it makes the 6 currencies
-- summable. LEFT JOIN (not INNER) on purpose: an unmatched payment must survive with a NULL
-- usd_amount so the not_null test catches it loudly, rather than being silently dropped.
--
-- Incremental: on re-runs only payments newer than the fact's own high watermark
-- (MAX(updated_at)) are recomputed and MERGEd by payment_id, so a daily run costs
-- O(changed rows), not O(all history). `dbt run --full-refresh` rebuilds from scratch.
{{ config(
    materialized='incremental',
    unique_key='payment_id',
) }}

SELECT
    p.payment_id,
    p.merchant_id,
    p.shopper_id,
    p.currency,
    p.country_code,
    p.payment_method,
    p.payment_status,
    p.amount,
    d.rate_to_usd,
    ROUND(p.amount * d.rate_to_usd, 2) AS usd_amount,
    d.is_filled                        AS fx_rate_filled,
    p.created_at,
    p.created_at::DATE                 AS created_date,
    p.updated_at                       AS updated_at
FROM {{ ref('stg_payments') }} p
LEFT JOIN {{ ref('dim_fx_rates') }} d
    ON d.currency = p.currency
   AND d.rate_date = p.created_at::DATE
{% if is_incremental() %}
-- Rows changed since the last build. >= (not >) so a late arrival sharing the exact
-- watermark timestamp is never dropped; the MERGE on payment_id dedups the overlap.
WHERE p.updated_at >= (SELECT COALESCE(MAX(updated_at), '1900-01-01'::TIMESTAMP_NTZ) FROM {{ this }})
{% endif %}
