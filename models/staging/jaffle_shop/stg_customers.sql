WITH

customers AS (
    SELECT
        id                         AS customer_id,
        first_name                 AS customer_first_name,
        last_name                  AS customer_last_name,
        first_name||' '||last_name AS customer_full_name
    FROM {{ source('jaffle_shop', 'customers') }}
)

SELECT * FROM customers