-- Fact: every payment normalized to USD by joining the gap-free FX dimension on
-- (currency, created date). usd_amount = amount * rate_to_usd is THE business deliverable --
-- it makes the 6 currencies summable.
--
-- LEFT JOIN (not INNER) on purpose: an unmatched payment must survive into the fact with a NULL
-- usd_amount so validate.sql can catch it loudly, rather than being silently dropped. With the
-- forward-filled dimension covering every calendar day, there should be zero unmatched rows.
CREATE OR REPLACE TABLE ANALYTICS.FCT_PAYMENTS_USD AS
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
    p.created_at::DATE                 AS created_date
FROM ANALYTICS.STG_PAYMENTS p
LEFT JOIN ANALYTICS.DIM_FX_RATES d
    ON d.currency = p.currency
   AND d.rate_date = p.created_at::DATE;
