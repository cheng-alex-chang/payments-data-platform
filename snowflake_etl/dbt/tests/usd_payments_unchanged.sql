-- USD identity: USD payments must convert 1:1 (amount == usd_amount).
SELECT payment_id, amount, usd_amount
FROM {{ ref('fct_payments_usd') }}
WHERE currency = 'USD' AND usd_amount <> amount
