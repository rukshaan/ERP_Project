{{ config(materialized='table') }}

WITH cte AS (
    SELECT DISTINCT
        CAST(order_date AS DATE) AS date_value,  -- convert order_date to DATE
        md5(CAST(order_date AS text)) AS date_key  -- generate surrogate key based on order_date
    FROM {{ ref('stg_sales_order') }}  -- get data from the staging table `stg_sales_order`
    WHERE order_date IS NOT NULL  -- exclude NULL order dates
),

dates AS (
    SELECT
        date_key,
        date_value,
        EXTRACT(YEAR FROM date_value) AS year,
        EXTRACT(MONTH FROM date_value) AS month,
        EXTRACT(DAY FROM date_value) AS day,
        EXTRACT(QUARTER FROM date_value) AS quarter,
        strftime('%B', date_value) AS month_name,  -- DuckDB formatting for full month name
        EXTRACT(WEEK FROM date_value) AS week,
        EXTRACT(DOW FROM date_value) AS day_of_week,  -- extract the day of the week (0=Sunday, 6=Saturday)
        strftime('%A', date_value) AS day_name   -- DuckDB formatting for full day name
    FROM cte
)

SELECT 
    date_key,
    date_value,
    year,
    month,
    day,
    quarter,
    month_name,
    week,
    day_of_week,
    day_name
FROM dates

ORDER BY date_value
