WITH

orders AS (
    SELECT
        id          AS order_id,
        user_id     AS customer_id,
        status      AS order_status,
        order_date  AS order_placed_at,
        CASE
            WHEN status NOT IN ('returned', 'return_pending')
                THEN order_date
        END         AS valid_order_date    
    FROM {{ source('jaffle_shop', 'orders') }}
)

SELECT * FROM orders