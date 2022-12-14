WITH

paid_orders AS (
    SELECT * FROM {{ ref('int_paid_orders') }}
),

final AS (
    SELECT
        po.*,
        ROW_NUMBER() OVER (ORDER BY po.order_id)                             AS transaction_seq,
        ROW_NUMBER() OVER (PARTITION BY po.customer_id ORDER BY po.order_id) AS customer_sales_seq,
        CASE
            WHEN RANK() OVER (
                PARTITION BY po.customer_id
                ORDER BY po.order_placed_at, po.order_id
            ) = 1
                THEN 'new'
            ELSE
                'return'
        END                                                                  AS nvsr,
        SUM(total_amount_paid) OVER (
            PARTITION BY po.customer_id
            ORDER BY po.order_placed_at
        )                                                                    AS customer_lifetime_value,
        FIRST_VALUE(po.order_placed_at) OVER (
            PARTITION BY po.customer_id
            ORDER BY po.order_placed_at
        )                                                                    AS fdos
    FROM paid_orders po
)

SELECT * FROM final