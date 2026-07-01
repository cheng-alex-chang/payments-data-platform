-- Reconcile: every staged payment produced exactly one fact row (no drops, no dupes).
-- Returns a row (= test failure) when the counts diverge.
WITH counts AS (
    SELECT
        (SELECT COUNT(*) FROM {{ ref('stg_payments') }})     AS expected,
        (SELECT COUNT(*) FROM {{ ref('fct_payments_usd') }}) AS actual
)
SELECT * FROM counts WHERE expected <> actual
