WITH

payments AS (
    SELECT
        id                     AS payment_id,
        orderid                AS order_id,
        paymentmethod          AS payment_method,
        status                 AS payment_status,
        ROUND(amount/100.0, 2) AS payment_amount,
        created                AS payment_created_at
    FROM {{ source('stripe', 'payment') }}
)

SELECT * FROM payments