WITH

customers AS (
    SELECT * FROM {{ ref('stg_customers') }}
),

orders AS (
    SELECT * FROM {{ ref('stg_orders') }}
),

completed_payments AS (
    SELECT * FROM {{ ref('int_completed_payments') }}
),

paid_orders AS (
    SELECT
        o.order_id,
        o.customer_id,
        o.order_placed_at,
        o.order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        c.customer_first_name,
        c.customer_last_name
    FROM orders o
        LEFT JOIN completed_payments p
            ON o.order_id = p.order_id
        LEFT JOIN customers c
            ON o.customer_id = c.customer_id
)

SELECT * FROM paid_orders