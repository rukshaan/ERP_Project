{{ config(materialized='table') }}

WITH cte AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['customer_name']) }} AS customer_key,  -- surrogate key
        customer_name,
        ROW_NUMBER() OVER(PARTITION BY customer_name ORDER BY customer_name) AS rank       -- deduplicate
    FROM {{ ref('stg_customer') }}
)

SELECT
    customer_key,
    customer_name
FROM cte
WHERE rank = 1
ORDER BY customer_name
