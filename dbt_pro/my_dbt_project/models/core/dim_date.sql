{{ config(materialized='incremental', unique_key='date_key') }}

WITH cte AS (
    SELECT DISTINCT
        CAST(order_date AS DATE) AS date_value,  -- Convert order_date to DATE
        md5(CAST(order_date AS text)) AS date_key  -- Generate surrogate key based on order_date
    FROM {{ ref('stg_sales_order') }}  -- Get data from the staging table `stg_sales_order`
    WHERE order_date IS NOT NULL  -- Exclude NULL order dates
),

dates AS (
    SELECT
        date_key,
        date_value,
        EXTRACT(YEAR FROM date_value) AS year,
        EXTRACT(MONTH FROM date_value) AS month,
        EXTRACT(DAY FROM date_value) AS day,
        EXTRACT(QUARTER FROM date_value) AS quarter,
        strftime('%B', date_value) AS month_name,  -- Full month name
        EXTRACT(WEEK FROM date_value) AS week,
        EXTRACT(DOW FROM date_value) AS day_of_week,  -- Day of the week (0=Sunday, 6=Saturday)
        strftime('%A', date_value) AS day_name,  -- Full day name
        -- Mark all records as current by default
        'Y' AS current_flag,  
        CURRENT_DATE AS start_date,
        NULL AS end_date  -- Set end_date to NULL initially
    FROM cte
),

latest_dates AS (
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
        day_name,
        current_flag,
        start_date,
        end_date
    FROM dates
    -- Add logic for incremental updates
    {% if is_incremental() %}
        WHERE date_key NOT IN (SELECT date_key FROM {{ this }} WHERE current_flag = 'Y')
    {% endif %}
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
    day_name,
    current_flag,
    start_date,
    end_date
FROM latest_dates
ORDER BY date_value

