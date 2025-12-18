-- Dimension table for Company
{{ config(materialized='table') }}

WITH cte AS (
    SELECT
        {{ dbt_utils.generate_surrogate_key(['company']) }} AS company_key,  -- surrogate key
        company, 
        ROW_NUMBER() OVER(PARTITION BY company ORDER BY company) AS rank  -- deduplicate
    FROM {{ ref('stg_sales_order') }}
)

SELECT
    company_key,
    company
FROM cte
where rank=1
ORDER BY company
