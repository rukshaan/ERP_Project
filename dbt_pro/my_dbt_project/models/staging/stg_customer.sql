{{ config(materialized='table') }}

WITH source_data AS (
    SELECT *
    FROM delta_scan('/opt/airflow/data/Silver/delta/SalesOrderItems')
),

cleaned AS (
    SELECT
       customer AS customer,
        md5(split_part(sales_order_id, '-', 1)) AS customer_key
    FROM source_data
    GROUP BY 1, 2
)

SELECT *
FROM cleaned
