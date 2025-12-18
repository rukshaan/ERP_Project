{{ config(materialized='table') }}

WITH cte AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer']) }} AS customer_key,  -- surrogate key
        customer,
        ROW_NUMBER() OVER(PARTITION BY customer ORDER BY customer) AS rank       -- deduplicate
    FROM {{ ref('stg_customer') }}
)

SELECT
    customer_key,
    customer
FROM cte
WHERE rank = 1
ORDER BY customer
