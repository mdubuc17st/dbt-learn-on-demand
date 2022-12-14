WITH

payments AS (
    SELECT * FROM {{ ref('stg_payments') }}
),

completed_payments AS (
    SELECT
        order_id,
        MAX(payment_created_at) AS payment_finalized_date,
        SUM(payment_amount)     AS total_amount_paid
    FROM payments
    WHERE payment_status != 'fail'
    GROUP BY order_id 
)

SELECT * FROM completed_payments