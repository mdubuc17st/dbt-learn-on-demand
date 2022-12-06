WITH paid_orders AS (
    SELECT
        o.id                      AS order_id,
        o.user_id                 AS customer_id,
        o.order_date              AS order_placed_at,
        o.status                  AS order_status,
        p.total_amount_paid,
        p.payment_finalized_date,
        c.first_name              AS customer_first_name,
        c.last_name               AS customer_last_name
    FROM mdubuc_raw.jaffle_shop.orders o
        LEFT JOIN (
            SELECT
                orderid           AS order_id,
                MAX(created)      AS payment_finalized_date,
                SUM(amount)/100.0 AS total_amount_paid
            FROM mdubuc_raw.stripe.payment
            WHERE status != 'fail'
            GROUP BY orderid
        ) p
            ON o.id = p.order_id
        LEFT JOIN mdubuc_raw.jaffle_shop.customers c
            ON o.user_id = c.id
),

customer_orders AS (
    SELECT
        c.id            AS customer_id,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS most_recent_order_date,
        COUNT(o.id)     AS number_of_orders
    FROM mdubuc_raw.jaffle_shop.customers c
        LEFT JOIN mdubuc_raw.jaffle_shop.orders o
            ON c.id = o.user_id 
    GROUP BY c.id
)

SELECT
    p.*,
    ROW_NUMBER() OVER (ORDER BY p.order_id)                            AS transaction_seq,
    ROW_NUMBER() OVER (PARTITION BY p.customer_id ORDER BY p.order_id) AS customer_sales_seq,
    CASE
        WHEN c.first_order_date = p.order_placed_at
            THEN 'new'
        ELSE
            'return'
    END                                                                AS nvsr,
    x.clv_bad                                                          AS customer_lifetime_value,
    c.first_order_date                                                 AS fdos
FROM paid_orders p
    LEFT JOIN customer_orders c
        ON p.customer_id = c.customer_id
    LEFT JOIN (
        SELECT
            p.order_id,
            SUM(t2.total_amount_paid) AS clv_bad
        FROM paid_orders p
            LEFT JOIN paid_orders t2
                ON p.customer_id = t2.customer_id
                    AND p.order_id >= t2.order_id
        GROUP BY p.order_id
        ORDER BY p.order_id
    ) x
        ON x.order_id = p.order_id
ORDER BY p.order_id