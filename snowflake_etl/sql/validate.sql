-- Validation: data-quality gates the orchestrator asserts after the transforms run.
-- Each row is one named check; the runner fails the pipeline if any result = 'FAIL'.
WITH checks AS (
    -- 1. Reconcile: every staged payment produced exactly one fact row (no drops, no dupes).
    SELECT
        'fact_reconciles_to_payments' AS check_name,
        (SELECT COUNT(*) FROM ANALYTICS.STG_PAYMENTS)      AS expected,
        (SELECT COUNT(*) FROM ANALYTICS.FCT_PAYMENTS_USD)  AS actual

    UNION ALL
    -- 2. No payment failed to get a USD amount (would mean an unmatched currency/date).
    SELECT
        'no_unmatched_usd_amount',
        0,
        (SELECT COUNT(*) FROM ANALYTICS.FCT_PAYMENTS_USD WHERE usd_amount IS NULL)

    UNION ALL
    -- 3. No null or non-positive FX rate slipped through the forward-fill.
    SELECT
        'no_null_or_zero_fx_rate',
        0,
        (SELECT COUNT(*) FROM ANALYTICS.DIM_FX_RATES WHERE rate_to_usd IS NULL OR rate_to_usd <= 0)

    UNION ALL
    -- 4. USD identity: USD payments must convert 1:1 (amount == usd_amount).
    SELECT
        'usd_payments_unchanged',
        0,
        (SELECT COUNT(*) FROM ANALYTICS.FCT_PAYMENTS_USD
         WHERE currency = 'USD' AND usd_amount <> amount)
)
SELECT
    check_name,
    expected,
    actual,
    IFF(expected = actual, 'PASS', 'FAIL') AS result
FROM checks
ORDER BY result DESC, check_name;
