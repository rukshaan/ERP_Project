{{ config(materialized='table') }}

WITH customers AS (
    SELECT
        customer_key,
        customer
    FROM {{ ref('stg_customer') }}
)

SELECT
    customer_key,
    customer
FROM customers
ORDER BY customer
