{{ config(
    materialized='table'
) }}
 
 
WITH cte AS (SELECT
      {{ dbt_utils.generate_surrogate_key(['item_code']) }} AS item_key,
      item_code,
      item_name,
      ROW_NUMBER() OVER(PARTITION BY item_code ORDER BY item_code) AS rank
FROM {{ ref('stg_sales_order') }})
SELECT * FROM cte WHERE rank=1
 
 